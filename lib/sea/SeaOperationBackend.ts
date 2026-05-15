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

import { NIL } from 'uuid';
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
import ArrowResultConverter from '../result/ArrowResultConverter';
import ResultSlicer from '../result/ResultSlicer';
import SeaResultsProvider from './SeaResultsProvider';
import { arrowSchemaToThriftSchema, decodeIpcSchema } from './SeaArrowIpc';

/**
 * The minimal slice of the napi-binding `Statement` class that
 * `SeaOperationBackend` calls. Defined locally so unit tests can pass
 * a stub without loading the native binding.
 */
export interface SeaStatementNative {
  fetchNextBatch(): Promise<{ ipcBytes: Buffer } | null>;
  schema(): Promise<{ ipcBytes: Buffer }>;
  cancel(): Promise<void>;
  close(): Promise<void>;
}

interface SeaOperationBackendOptions {
  statement: SeaStatementNative;
  context: IClientContext;
  operationId?: string;
}

/**
 * `IOperationBackend` over the napi-bound kernel `Statement`. Adapts
 * the kernel's Arrow IPC stream onto the existing thrift-shaped result
 * pipeline (`ArrowResultConverter` + `ResultSlicer`) so the M0 row
 * shape is byte-identical to the thrift path for every M0 datatype.
 *
 * Pipeline:
 *   napi.Statement.fetchNextBatch()  (IPC bytes per batch)
 *     -> SeaResultsProvider          (adapts to IResultsProvider<ArrowBatch>)
 *     -> ArrowResultConverter        (Phase 1 + Phase 2; reused unchanged)
 *     -> ResultSlicer                (chunk-size normalisation; reused unchanged)
 *
 * The kernel exposes only the `Arrow` `ResultBatch` variant for M0 —
 * both CloudFetch (external links) and inline batches flow through
 * `ResultStream::next_batch` and surface as a single Arrow IPC stream
 * per call, per
 * `sea-workflow/findings/arch/napi-binding/round2-methods-2026-05-15.md:69`.
 * One backend therefore covers both fetch modes without dispatching on
 * `TSparkRowSetType`.
 */
export default class SeaOperationBackend implements IOperationBackend {
  private readonly statement: SeaStatementNative;

  private readonly context: IClientContext;

  private readonly operationId: string;

  private resultSlicer?: ResultSlicer<any>;

  private resultsProvider?: SeaResultsProvider;

  private metadata?: TGetResultSetMetadataResp;

  private metadataPromise?: Promise<TGetResultSetMetadataResp>;

  // Tracks the operation's terminal state. The kernel does not expose
  // pending/running observability at the napi surface today; `execute`
  // resolves only after the statement has reached a result-fetching
  // state, so we treat the backend as FINISHED until `close()`/`cancel()`.
  private state: TOperationState = TOperationState.FINISHED_STATE;

  constructor({ statement, context, operationId }: SeaOperationBackendOptions) {
    this.statement = statement;
    this.context = context;
    this.operationId = operationId ?? NIL;
  }

  public get id(): string {
    return this.operationId;
  }

  public get hasResultSet(): boolean {
    // M0 only routes through SeaOperationBackend for executeStatement
    // calls. DDL/DML without a result set is not exercised through SEA
    // for M0; the napi Statement still produces a schema (empty) in
    // that case, which the converter renders as zero rows. Reporting
    // `true` keeps the facade's fetch path enabled for M0 parity.
    return true;
  }

  public async fetchChunk({
    limit,
    disableBuffering,
  }: {
    limit: number;
    disableBuffering?: boolean;
  }): Promise<Array<object>> {
    const slicer = await this.getResultSlicer();
    return slicer.fetchNext({ limit, disableBuffering });
  }

  public async hasMore(): Promise<boolean> {
    const slicer = await this.getResultSlicer();
    return slicer.hasMore();
  }

  public async waitUntilReady(options?: {
    progress?: boolean;
    callback?: (progress: TGetOperationStatusResp) => unknown;
  }): Promise<void> {
    // The kernel's `executeStatement` resolves once results are
    // available; there's no pending/running state to observe here. We
    // synthesise an immediate FINISHED status for the optional callback.
    if (options?.callback) {
      await Promise.resolve(options.callback(await this.status(Boolean(options.progress))));
    }
  }

  public async status(_progress: boolean): Promise<TGetOperationStatusResp> {
    return {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
      operationState: this.state,
      hasResultSet: true,
    };
  }

  public async getResultMetadata(): Promise<TGetResultSetMetadataResp> {
    if (this.metadata) {
      return this.metadata;
    }
    if (this.metadataPromise) {
      return this.metadataPromise;
    }
    this.metadataPromise = (async () => {
      const arrowSchemaIpc = await this.statement.schema();
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

  public async cancel(): Promise<Status> {
    await this.statement.cancel();
    this.state = TOperationState.CANCELED_STATE;
    return Status.success();
  }

  public async close(): Promise<Status> {
    await this.statement.close();
    this.state = TOperationState.CLOSED_STATE;
    return Status.success();
  }

  private async getResultSlicer(): Promise<ResultSlicer<any>> {
    if (this.resultSlicer) {
      return this.resultSlicer;
    }
    const metadata = await this.getResultMetadata();
    this.resultsProvider = new SeaResultsProvider(this.statement);
    const converter = new ArrowResultConverter(this.context, this.resultsProvider, metadata);
    this.resultSlicer = new ResultSlicer(this.context, converter);
    return this.resultSlicer;
  }
}
