// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * `IOperationBackend` implementation for the SEA path.
 *
 * Combines:
 * - **Fetch pipeline (from sea-results):**
 *   `napi.Statement.fetchNextBatch()` → `SeaResultsProvider` →
 *   `ArrowResultConverter` (Phase 1 + Phase 2; reused unchanged) →
 *   `ResultSlicer` (chunk-size normalisation; reused unchanged). The M0
 *   row shape is byte-identical to the thrift path for every M0
 *   datatype (parity gate exercised by `tests/integration/sea/results-e2e.test.ts`).
 *
 * - **Lifecycle (from sea-operation):** `cancel()` / `close()` /
 *   `finished()` (alias of `waitUntilReady`) delegate to the helpers
 *   in `SeaOperationLifecycle.ts`. The helpers handle idempotency,
 *   flag-set-before-await ordering (so cancel-mid-fetch propagates),
 *   logging via `IClientContext`, and kernel-error mapping.
 *
 * The lifecycle helpers route fetch-after-cancel / fetch-after-close
 * through `failIfNotActive`, which throws an `OperationStateError`
 * matching the Thrift `failIfClosed` semantics. We call it from
 * `fetchChunk`/`hasMore`/`getResultMetadata` so the cancel-mid-fetch
 * e2e (cancel < 200ms) drives against this backend cleanly.
 */

import { v4 as uuidv4 } from 'uuid';
import { TTableSchema } from '../../thrift/TCLIService_types';
import IOperationBackend, { IOperationBackendWaitOptions } from '../contracts/IOperationBackend';
import { OperationStatus, OperationState } from '../contracts/OperationStatus';
import { ResultMetadata, ResultFormat } from '../contracts/ResultMetadata';
import IClientContext from '../contracts/IClientContext';
import Status from '../dto/Status';
import ArrowResultConverter from '../result/ArrowResultConverter';
import ResultSlicer from '../result/ResultSlicer';
import SeaResultsProvider from './SeaResultsProvider';
import { arrowSchemaToThriftSchema, decodeIpcSchema } from './SeaArrowIpc';
import { SeaStatement } from './SeaNativeLoader';
import {
  SeaStatementHandle,
  SeaOperationLifecycleState,
  createLifecycleState,
  seaCancel,
  seaClose,
  seaFinished,
  failIfNotActive,
} from './SeaOperationLifecycle';

/**
 * Structural union of the lifecycle surface (cancel/close) and the
 * fetch surface (fetchNextBatch/schema). The real napi `Statement`
 * implements both; lifecycle-only test stubs implement only the
 * cancel/close half — fetch methods are accessed lazily and the
 * lifecycle tests never reach that path.
 */
export type SeaOperationStatement = SeaStatementHandle & Partial<SeaStatement>;

/**
 * Constructor options for `SeaOperationBackend`.
 */
export interface SeaOperationBackendOptions {
  /** The opaque napi `Statement` handle returned by `Connection.executeStatement(...)`. */
  statement: SeaOperationStatement;
  context: IClientContext;
  /**
   * Optional override for `id`. When not provided a fresh UUIDv4 is
   * generated upstream (in `SeaSessionBackend.executeStatement`); the
   * kernel does not yet surface its internal statement-id at the napi
   * boundary. Once it does, the JS layer can thread it through here.
   */
  id?: string;
}

export default class SeaOperationBackend implements IOperationBackend {
  private readonly statement: SeaOperationStatement;

  private readonly context: IClientContext;

  private readonly _id: string;

  private readonly lifecycle: SeaOperationLifecycleState = createLifecycleState();

  private resultSlicer?: ResultSlicer<any>;

  private resultsProvider?: SeaResultsProvider;

  private metadata?: ResultMetadata;

  private metadataPromise?: Promise<ResultMetadata>;

  constructor({ statement, context, id }: SeaOperationBackendOptions) {
    this.statement = statement;
    this.context = context;
    this._id = id ?? uuidv4();
  }

  public get id(): string {
    return this._id;
  }

  public hasResultSet(): boolean {
    // M0 only routes through SeaOperationBackend for executeStatement
    // calls. DDL/DML without a result set is not exercised through SEA
    // for M0; the napi Statement still produces a schema (empty) in
    // that case, which the converter renders as zero rows. Reporting
    // `true` keeps the facade's fetch path enabled for M0 parity.
    return true;
  }

  // ---------------------------------------------------------------------------
  // Fetch / metadata (owned by the sea-results pipeline).
  // ---------------------------------------------------------------------------

  public async fetchChunk({
    limit,
    disableBuffering,
  }: {
    limit: number;
    disableBuffering?: boolean;
  }): Promise<Array<object>> {
    // Cancel-mid-fetch propagation: if cancel() has flipped the
    // lifecycle flag, fail locally without a wire round-trip.
    failIfNotActive(this.lifecycle);
    const slicer = await this.getResultSlicer();
    return slicer.fetchNext({ limit, disableBuffering });
  }

  public async hasMore(): Promise<boolean> {
    failIfNotActive(this.lifecycle);
    const slicer = await this.getResultSlicer();
    return slicer.hasMore();
  }

  public async getResultMetadata(): Promise<ResultMetadata> {
    failIfNotActive(this.lifecycle);
    if (this.metadata) {
      return this.metadata;
    }
    if (this.metadataPromise) {
      return this.metadataPromise;
    }
    this.metadataPromise = (async () => {
      if (!this.statement.schema) {
        throw new Error('SeaOperationBackend: statement.schema() is not available on this handle');
      }
      const arrowSchemaIpc = await this.statement.schema();
      const arrowSchema = decodeIpcSchema(arrowSchemaIpc.ipcBytes);
      // `ResultMetadata.schema` keeps the Thrift `TTableSchema` shape for
      // back-compat with the public `IOperation.getSchema()` surface.
      const thriftSchema: TTableSchema = arrowSchemaToThriftSchema(arrowSchema);
      const meta: ResultMetadata = {
        schema: thriftSchema,
        // SEA inline + CloudFetch both surface to JS as Arrow batches;
        // both flow through the same Arrow result converter.
        resultFormat: ResultFormat.ArrowBased,
        lz4Compressed: false,
        // Carry the raw Arrow IPC schema bytes for ARROW_BASED consumers.
        arrowSchema: arrowSchemaIpc.ipcBytes,
        isStagingOperation: false,
      };
      this.metadata = meta;
      return meta;
    })();
    try {
      return await this.metadataPromise;
    } finally {
      this.metadataPromise = undefined;
    }
  }

  // ---------------------------------------------------------------------------
  // Status / lifecycle (owned by the sea-operation lifecycle helpers).
  // ---------------------------------------------------------------------------

  public async status(_progress: boolean): Promise<OperationStatus> {
    // Synthesised — the kernel resolves `Statement::execute().await` before
    // it hands back a Statement handle, so by the time a SeaOperationBackend
    // exists the statement is terminal. Report Cancelled/Closed if the
    // lifecycle flag is set, else Succeeded. Returns the backend-neutral
    // OperationStatus the IOperationBackend contract expects, so the
    // DBSQLOperation facade switches on `state` identically across backends.
    if (this.lifecycle.isCancelled) {
      return { state: OperationState.Cancelled, hasResultSet: true };
    }
    if (this.lifecycle.isClosed) {
      return { state: OperationState.Closed, hasResultSet: true };
    }
    return { state: OperationState.Succeeded, hasResultSet: true };
  }

  public async waitUntilReady(options?: IOperationBackendWaitOptions): Promise<void> {
    // Kernel's `Statement::execute().await` has already resolved by the
    // time we hold a Statement handle — there is no pending/running
    // state to poll for M0. seaFinished fires the progress callback
    // once with a synthesised FINISHED response so progress-UI callers
    // see the same one-shot completion tick the Thrift path emits at
    // the end of its polling loop.
    return seaFinished(this.lifecycle, options);
  }

  public async cancel(): Promise<Status> {
    return seaCancel(this.lifecycle, this.statement, this.context, this._id);
  }

  public async close(): Promise<Status> {
    return seaClose(this.lifecycle, this.statement, this.context, this._id);
  }

  // ---------------------------------------------------------------------------
  // Internals.
  // ---------------------------------------------------------------------------

  private async getResultSlicer(): Promise<ResultSlicer<any>> {
    if (this.resultSlicer) {
      return this.resultSlicer;
    }
    if (!this.statement.fetchNextBatch) {
      throw new Error('SeaOperationBackend: statement.fetchNextBatch() is not available on this handle');
    }
    const metadata = await this.getResultMetadata();
    // The lifecycle subset has cancel/close only; fetch methods exist on
    // the full napi Statement. Cast is safe here because we've just
    // verified `fetchNextBatch` is callable.
    this.resultsProvider = new SeaResultsProvider(this.statement as SeaStatement);
    const converter = new ArrowResultConverter(this.context, this.resultsProvider, metadata);
    this.resultSlicer = new ResultSlicer(this.context, converter);
    return this.resultSlicer;
  }
}
