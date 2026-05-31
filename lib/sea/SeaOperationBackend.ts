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
 * Two construction shapes, sharing one fetch/lifecycle surface:
 *
 * - **Async query path (`asyncStatement`)** — `executeStatement` submits
 *   with the kernel's `wait_timeout=0s` and hands back a pending
 *   `AsyncStatement`. `waitUntilReady()` polls `status()` to a terminal
 *   state (firing the progress callback each tick, exactly like the
 *   Thrift backend's `getOperationStatus` loop), then materialises the
 *   result stream via `awaitResult()`. This is true Thrift-parity
 *   async execution: `status()` reports real Pending/Running/Succeeded
 *   states and a long-running query can be cancelled mid-flight.
 *
 *   The JS-side poll loop (rather than a single blocking `awaitResult()`)
 *   is what keeps `cancel()` responsive: the kernel `AsyncStatement`
 *   serialises its methods behind one mutex, so a single in-flight
 *   `awaitResult()` would hold that mutex for the whole query and queue
 *   `cancel()` behind it. Polling `status()` releases the mutex between
 *   ticks, leaving gaps for `cancel()` to land.
 *
 * - **Blocking metadata path (`statement`)** — the metadata methods
 *   (`listCatalogs`, `listTypeInfo`, …) return a kernel `Statement`
 *   that has already run to a terminal state, so there is nothing to
 *   poll: `waitUntilReady()` resolves immediately (one synthesized
 *   FINISHED tick) and the handle itself is the result source.
 *
 * Fetch pipeline (shared): `fetchNextBatch()` → `SeaResultsProvider` →
 * `ArrowResultConverter` → `ResultSlicer`, byte-identical to the Thrift
 * path for every datatype.
 *
 * Lifecycle (shared): `cancel()` / `close()` delegate to the helpers in
 * `SeaOperationLifecycle.ts` (idempotency, flag-set-before-await
 * ordering, kernel-error mapping). `failIfNotActive` routes
 * fetch-after-cancel / fetch-after-close through an `OperationStateError`
 * matching the Thrift `failIfClosed` semantics.
 */

import { v4 as uuidv4 } from 'uuid';
import {
  TGetOperationStatusResp,
  TGetResultSetMetadataResp,
  TOperationState,
  TSparkRowSetType,
  TStatusCode,
  TTableSchema,
} from '../../thrift/TCLIService_types';
import IOperationBackend from '../contracts/IOperationBackend';
import IClientContext from '../contracts/IClientContext';
import Status from '../dto/Status';
import OperationStateError, { OperationStateErrorCode } from '../errors/OperationStateError';
import ArrowResultConverter from '../result/ArrowResultConverter';
import ResultSlicer from '../result/ResultSlicer';
import SeaResultsProvider from './SeaResultsProvider';
import { arrowSchemaToThriftSchema, decodeIpcSchema } from './SeaArrowIpc';
import {
  SeaNativeStatement,
  SeaNativeAsyncStatement,
  SeaNativeStatementStatus,
  SeaArrowSchema,
  SeaArrowBatch,
} from './SeaNativeLoader';
import { decodeNapiKernelError } from './SeaErrorMapping';
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
 * Server-status poll cadence for the async path, in milliseconds.
 * Matches the Thrift backend's `waitUntilReady` `delay(100)` so the
 * two backends place the same GetStatementStatus / getOperationStatus
 * load on the server for the same query.
 */
const STATUS_POLL_INTERVAL_MS = 100;

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

/**
 * Structural union of the lifecycle surface (cancel/close) and the
 * fetch surface (fetchNextBatch/schema). The real napi `Statement`
 * implements both; lifecycle-only test stubs implement only the
 * cancel/close half — fetch methods are accessed lazily and the
 * lifecycle tests never reach that path.
 */
export type SeaOperationStatement = SeaStatementHandle & Partial<SeaNativeStatement>;

/**
 * Minimal result-fetch surface shared by the async `AsyncResultHandle`
 * (from `awaitResult()`) and the blocking metadata `Statement`. Both
 * expose `schema()` + `fetchNextBatch()`; only this slice is consumed
 * by the fetch pipeline.
 */
interface SeaFetchHandle {
  schema(): Promise<SeaArrowSchema>;
  fetchNextBatch(): Promise<SeaArrowBatch | null>;
}

/**
 * Constructor options for `SeaOperationBackend`. Exactly one of
 * `asyncStatement` (query path) or `statement` (metadata path) must be
 * provided.
 */
export interface SeaOperationBackendOptions {
  /**
   * The pending napi `AsyncStatement` returned by
   * `Connection.submitStatement(...)`. Async query path.
   */
  asyncStatement?: SeaNativeAsyncStatement;
  /**
   * The terminal napi `Statement` returned by a metadata method
   * (`listCatalogs`, `listTypeInfo`, …). Blocking metadata path.
   */
  statement?: SeaOperationStatement;
  context: IClientContext;
  /**
   * Optional override for `id`. When not provided a fresh UUIDv4 is
   * generated. For the async path the kernel surfaces a real
   * server-issued `statementId`, which is used as the id when no
   * explicit override is given.
   */
  id?: string;
}

/** Map the kernel's `StatementStatus` variant name to a Thrift `TOperationState`. */
function statusToOperationState(status: SeaNativeStatementStatus): TOperationState {
  switch (status) {
    case 'Pending':
      return TOperationState.PENDING_STATE;
    case 'Running':
      return TOperationState.RUNNING_STATE;
    case 'Succeeded':
      return TOperationState.FINISHED_STATE;
    case 'Failed':
      return TOperationState.ERROR_STATE;
    case 'Cancelled':
      return TOperationState.CANCELED_STATE;
    case 'Closed':
      return TOperationState.CLOSED_STATE;
    case 'Unknown':
    default:
      return TOperationState.UKNOWN_STATE;
  }
}

/** Synthesize the Thrift status response shape from an operation state. */
function synthesizeStatus(operationState: TOperationState): TGetOperationStatusResp {
  return {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationState,
    hasResultSet: true,
  };
}

export default class SeaOperationBackend implements IOperationBackend {
  private readonly asyncStatement?: SeaNativeAsyncStatement;

  private readonly blockingStatement?: SeaOperationStatement;

  /** cancel/close target — the async handle or the blocking statement. */
  private readonly lifecycleHandle: SeaStatementHandle;

  private readonly context: IClientContext;

  private readonly _id: string;

  private readonly lifecycle: SeaOperationLifecycleState = createLifecycleState();

  private resultSlicer?: ResultSlicer<any>;

  private resultsProvider?: SeaResultsProvider;

  private metadata?: TGetResultSetMetadataResp;

  private metadataPromise?: Promise<TGetResultSetMetadataResp>;

  /**
   * Memoised result-fetch handle. For the async path this is the
   * `awaitResult()` promise (resolved once the statement is terminal);
   * for the metadata path it resolves to the blocking statement itself.
   */
  private fetchHandlePromise?: Promise<SeaFetchHandle>;

  constructor({ asyncStatement, statement, context, id }: SeaOperationBackendOptions) {
    if ((asyncStatement === undefined) === (statement === undefined)) {
      throw new Error(
        'SeaOperationBackend: exactly one of `asyncStatement` or `statement` must be provided',
      );
    }
    this.asyncStatement = asyncStatement;
    this.blockingStatement = statement;
    this.lifecycleHandle = (asyncStatement ?? statement) as SeaStatementHandle;
    this.context = context;
    this._id = id ?? asyncStatement?.statementId ?? statement?.statementId ?? uuidv4();
  }

  public get id(): string {
    return this._id;
  }

  public get hasResultSet(): boolean {
    // The kernel statement always produces a schema (empty for DDL/DML),
    // which the converter renders as zero rows. Reporting `true` keeps
    // the facade's fetch path enabled for parity with the Thrift backend.
    return true;
  }

  // ---------------------------------------------------------------------------
  // Fetch / metadata.
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

  public async getResultMetadata(): Promise<TGetResultSetMetadataResp> {
    failIfNotActive(this.lifecycle);
    if (this.metadata) {
      return this.metadata;
    }
    if (this.metadataPromise) {
      return this.metadataPromise;
    }
    this.metadataPromise = (async () => {
      const handle = await this.getFetchHandle();
      const arrowSchemaIpc = await handle.schema();
      const arrowSchema = decodeIpcSchema(arrowSchemaIpc.ipcBytes);
      const thriftSchema: TTableSchema = arrowSchemaToThriftSchema(arrowSchema);
      const meta: TGetResultSetMetadataResp = {
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
        schema: thriftSchema,
        // SEA inline + CloudFetch both surface to JS as Arrow batches;
        // both flow through the same converter that handles the
        // ARROW_BASED_SET path on the thrift side.
        resultFormat: TSparkRowSetType.ARROW_BASED_SET,
        lz4Compressed: false,
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
  // Status / lifecycle.
  // ---------------------------------------------------------------------------

  public async status(_progress: boolean): Promise<TGetOperationStatusResp> {
    // JS-initiated lifecycle wins — it may be ahead of the server.
    if (this.lifecycle.isCancelled) {
      return synthesizeStatus(TOperationState.CANCELED_STATE);
    }
    if (this.lifecycle.isClosed) {
      return synthesizeStatus(TOperationState.CLOSED_STATE);
    }
    if (this.asyncStatement) {
      // Real server status — single GetStatementStatus RPC, no polling.
      const state = await this.asyncStatement.status();
      return synthesizeStatus(statusToOperationState(state));
    }
    // Blocking metadata path: the statement is already terminal.
    return synthesizeStatus(TOperationState.FINISHED_STATE);
  }

  public async waitUntilReady(options?: {
    progress?: boolean;
    callback?: (progress: TGetOperationStatusResp) => unknown;
  }): Promise<void> {
    if (this.asyncStatement) {
      return this.waitUntilReadyAsync(options);
    }
    // Blocking metadata path: the kernel statement has already resolved,
    // so there is nothing to poll. Fire the progress callback once with
    // a synthesized FINISHED tick, matching the Thrift path's final tick.
    return seaFinished(this.lifecycle, options);
  }

  public async cancel(): Promise<Status> {
    return seaCancel(this.lifecycle, this.lifecycleHandle, this.context, this._id);
  }

  public async close(): Promise<Status> {
    return seaClose(this.lifecycle, this.lifecycleHandle, this.context, this._id);
  }

  // ---------------------------------------------------------------------------
  // Internals.
  // ---------------------------------------------------------------------------

  /**
   * Poll the kernel `AsyncStatement` to a terminal state, mirroring the
   * Thrift backend's `getOperationStatus` loop. Fires the progress
   * callback each tick; on FINISHED, materialises the result stream
   * (so the first fetch is free); on a bad terminal state, throws the
   * same `OperationStateError` the Thrift path raises.
   */
  private async waitUntilReadyAsync(options?: {
    progress?: boolean;
    callback?: (progress: TGetOperationStatusResp) => unknown;
  }): Promise<void> {
    // Already materialised → terminal-and-ready, nothing to wait for.
    if (this.fetchHandlePromise) {
      return;
    }

    for (;;) {
      // JS-initiated cancel/close short-circuits before the next poll.
      failIfNotActive(this.lifecycle);

      // eslint-disable-next-line no-await-in-loop
      const state = await this.asyncStatement!.status();
      const operationState = statusToOperationState(state);

      if (options?.callback) {
        // eslint-disable-next-line no-await-in-loop
        await Promise.resolve(options.callback(synthesizeStatus(operationState)));
      }

      switch (operationState) {
        case TOperationState.INITIALIZED_STATE:
        case TOperationState.PENDING_STATE:
        case TOperationState.RUNNING_STATE:
          break;

        case TOperationState.FINISHED_STATE:
          // Materialise the result stream now so the first fetch/metadata
          // call doesn't pay an extra await_result round-trip.
          // eslint-disable-next-line no-await-in-loop
          await this.getFetchHandle();
          return;

        case TOperationState.CANCELED_STATE:
          throw new OperationStateError(
            OperationStateErrorCode.Canceled,
            synthesizeStatus(operationState),
          );

        case TOperationState.CLOSED_STATE:
          throw new OperationStateError(
            OperationStateErrorCode.Closed,
            synthesizeStatus(operationState),
          );

        case TOperationState.ERROR_STATE:
          // `status()` collapses Failed to the variant name only; the
          // real SQL-error envelope (sql_state / error_code / query_id)
          // rides on `awaitResult()`'s rejection. Surface that.
          // eslint-disable-next-line no-await-in-loop
          await this.throwAsyncError(synthesizeStatus(operationState));
          break;

        case TOperationState.TIMEDOUT_STATE:
          throw new OperationStateError(
            OperationStateErrorCode.Timeout,
            synthesizeStatus(operationState),
          );

        case TOperationState.UKNOWN_STATE:
        default:
          throw new OperationStateError(
            OperationStateErrorCode.Unknown,
            synthesizeStatus(operationState),
          );
      }

      // eslint-disable-next-line no-await-in-loop
      await delay(STATUS_POLL_INTERVAL_MS);
    }
  }

  /**
   * Drive `awaitResult()` to extract the kernel's typed error envelope
   * for a Failed statement and re-throw it decoded. Falls back to a
   * generic `OperationStateError` if `awaitResult()` unexpectedly
   * resolves.
   */
  private async throwAsyncError(response: TGetOperationStatusResp): Promise<never> {
    try {
      await this.asyncStatement!.awaitResult();
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    throw new OperationStateError(OperationStateErrorCode.Error, response);
  }

  /**
   * Resolve (and memoise) the result-fetch handle. Async path: the
   * `awaitResult()` stream; metadata path: the blocking statement itself.
   */
  private getFetchHandle(): Promise<SeaFetchHandle> {
    if (!this.fetchHandlePromise) {
      if (this.asyncStatement) {
        this.fetchHandlePromise = this.asyncStatement.awaitResult();
      } else {
        const stmt = this.blockingStatement!;
        if (!stmt.schema || !stmt.fetchNextBatch) {
          return Promise.reject(
            new Error('SeaOperationBackend: fetch surface is not available on this handle'),
          );
        }
        this.fetchHandlePromise = Promise.resolve(stmt as SeaFetchHandle);
      }
    }
    return this.fetchHandlePromise;
  }

  private async getResultSlicer(): Promise<ResultSlicer<any>> {
    if (this.resultSlicer) {
      return this.resultSlicer;
    }
    const metadata = await this.getResultMetadata();
    const handle = await this.getFetchHandle();
    // SeaResultsProvider consumes only `fetchNextBatch`; the fetch handle
    // (async result handle or blocking statement) satisfies that slice.
    this.resultsProvider = new SeaResultsProvider(handle as unknown as SeaNativeStatement);
    const converter = new ArrowResultConverter(this.context, this.resultsProvider, metadata);
    this.resultSlicer = new ResultSlicer(this.context, converter);
    return this.resultSlicer;
  }
}
