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
 * Round 1 (impl-operation) lands the **lifecycle** methods:
 * - `cancel()` — forwards to napi `Statement.cancel()`,
 * - `close()`  — forwards to napi `Statement.close()`,
 * - `finished({progress, callback})` — M0 no-op (kernel
 *   `Statement::execute().await` already blocks until the statement
 *   is in a terminal state, so by the time we have a `Statement`
 *   handle the operation has already finished).
 *
 * Fetch / metadata / status methods are intentionally stubbed; they
 * are owned by the parallel impl-results feature on
 * `~/databricks-sql-nodejs-sea-WT/results` (`sea-results`). At
 * integration time impl-results either:
 *  (a) replaces this file with its own SeaOperationBackend that
 *      imports `SeaOperationLifecycle` for cancel/close/finished, or
 *  (b) extends the fetch methods here while keeping the lifecycle
 *      methods unchanged.
 * Both directions land cleanly with this file shape — the lifecycle
 * helpers are factored out into `SeaOperationLifecycle.ts` so neither
 * branch's diff touches the other's call sites.
 */

import { v4 as uuid } from 'uuid';
import {
  TGetOperationStatusResp,
  TGetResultSetMetadataResp,
  TOperationState,
  TStatusCode,
} from '../../thrift/TCLIService_types';
import IOperationBackend from '../contracts/IOperationBackend';
import IClientContext from '../contracts/IClientContext';
import Status from '../dto/Status';
import HiveDriverError from '../errors/HiveDriverError';
import {
  SeaStatementHandle,
  SeaOperationLifecycleState,
  createLifecycleState,
  seaCancel,
  seaClose,
  seaFinished,
  failIfNotActive,
} from './SeaOperationLifecycle';

interface SeaOperationBackendOptions {
  /**
   * The napi `Statement` handle returned by
   * `Connection.executeStatement(...)`. Typed structurally so unit
   * tests can hand in a mock without loading the native binding.
   */
  statement: SeaStatementHandle;
  /**
   * Driver-level context — used by lifecycle helpers for logging.
   */
  context: IClientContext;
  /**
   * Optional operation id. The kernel doesn't expose a stable
   * statement-id string yet (it's an internal `Uuid` on the
   * `ExecutedStatementHandle` trait but not surfaced through the
   * napi binding's public d.ts). For M0 we synthesize a client-side
   * UUID so the public `id` getter on `DBSQLOperation` returns
   * something stable; impl-results will swap this out for the
   * kernel-provided id once the binding exposes it.
   */
  id?: string;
}

const NOT_IMPLEMENTED_FETCH =
  'SEA result fetching is owned by the parallel impl-results feature ' +
  '(branch sea-results) and not wired in this commit.';

export default class SeaOperationBackend implements IOperationBackend {
  private readonly statement: SeaStatementHandle;

  private readonly context: IClientContext;

  private readonly _id: string;

  private readonly lifecycle: SeaOperationLifecycleState = createLifecycleState();

  constructor({ statement, context, id }: SeaOperationBackendOptions) {
    this.statement = statement;
    this.context = context;
    this._id = id ?? uuid();
  }

  public get id(): string {
    return this._id;
  }

  /**
   * SEA always returns row results from `executeStatement` (the
   * kernel doesn't surface a separate "no result set" path the way
   * Thrift does via `TOperationHandle.hasResultSet`). For M0 we
   * report `true` so the public-facade `DBSQLOperation.fetchChunk`
   * proceeds to call through. DDL statements (which produce no
   * rows) will be handled by impl-results returning an empty Arrow
   * batch from `fetchNextBatch`.
   */
  public get hasResultSet(): boolean {
    return true;
  }

  /**
   * Pre-flight gate used by every fetch* method. Exposed as
   * protected so impl-results can call it without re-importing
   * from `SeaOperationLifecycle`.
   */
  protected ensureActive(): void {
    failIfNotActive(this.lifecycle);
  }

  // ------------------------------------------------------------------
  // Fetch / metadata / status — stubs owned by impl-results.
  // ------------------------------------------------------------------

  public async fetchChunk(_options: {
    limit: number;
    disableBuffering?: boolean;
  }): Promise<Array<object>> {
    // Active-state gate is still meaningful here so cancel-mid-fetch
    // tests can drive against this stub: a fetch issued after cancel
    // throws the cancelled error rather than the not-implemented one.
    this.ensureActive();
    throw new HiveDriverError(NOT_IMPLEMENTED_FETCH);
  }

  public async hasMore(): Promise<boolean> {
    this.ensureActive();
    throw new HiveDriverError(NOT_IMPLEMENTED_FETCH);
  }

  public async status(_progress: boolean): Promise<TGetOperationStatusResp> {
    // For M0 the operation is always finished by the time we have a
    // Statement handle. Synthesize a FINISHED_STATE response so any
    // facade-level callers (`DBSQLOperation.status`, public surface
    // via `IOperation.status`) get a sensible answer without
    // throwing. Richer status fields (numModifiedRows, displayMessage,
    // progressUpdateResponse) defer to M1.
    if (this.lifecycle.isCancelled) {
      return {
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
        operationState: TOperationState.CANCELED_STATE,
      } as TGetOperationStatusResp;
    }
    if (this.lifecycle.isClosed) {
      return {
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
        operationState: TOperationState.CLOSED_STATE,
      } as TGetOperationStatusResp;
    }
    return {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
      operationState: TOperationState.FINISHED_STATE,
    } as TGetOperationStatusResp;
  }

  public async getResultMetadata(): Promise<TGetResultSetMetadataResp> {
    this.ensureActive();
    throw new HiveDriverError(NOT_IMPLEMENTED_FETCH);
  }

  // ------------------------------------------------------------------
  // Lifecycle — owned by impl-operation.
  // ------------------------------------------------------------------

  public async waitUntilReady(options?: {
    progress?: boolean;
    callback?: (progress: TGetOperationStatusResp) => unknown;
  }): Promise<void> {
    // `IOperationBackend.waitUntilReady` is the polling-loop entry on
    // Thrift (`ThriftOperationBackend.waitUntilReady`); for SEA M0 it
    // shares the implementation with `finished()` because both have
    // the same semantics here (the operation is already in a terminal
    // state by the time we have a Statement handle).
    return seaFinished(this.lifecycle, options);
  }

  public async cancel(): Promise<Status> {
    return seaCancel(this.lifecycle, this.statement, this.context, this._id);
  }

  public async close(): Promise<Status> {
    return seaClose(this.lifecycle, this.statement, this.context, this._id);
  }
}
