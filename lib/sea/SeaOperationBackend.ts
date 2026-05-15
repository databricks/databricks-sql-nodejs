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

import { v4 as uuidv4 } from 'uuid';
import { TGetOperationStatusResp, TGetResultSetMetadataResp, TOperationState } from '../../thrift/TCLIService_types';
import IOperationBackend from '../contracts/IOperationBackend';
import IClientContext from '../contracts/IClientContext';
import Status from '../dto/Status';
import { SeaNativeStatement } from './SeaNativeLoader';
import { mapKernelErrorToJsError, KernelErrorShape } from './SeaErrorMapping';
import HiveDriverError from '../errors/HiveDriverError';

/**
 * Constructor options for `SeaOperationBackend`.
 *
 * `statement` is the opaque napi `Statement` handle returned by
 * `Connection.executeStatement(...)`. The kernel has already internalized
 * async polling — by the time we hold a `Statement`, the SQL is at least
 * accepted by the server.
 *
 * `id` is captured at construction so `IOperationBackend.id` can return a
 * stable string without async work. The napi binding does not currently
 * expose the server-side `statement_id`, so the M0 shim generates a
 * synthetic UUIDv4. Once the binding surfaces the kernel statement id,
 * this is the only line that needs to change.
 */
export interface SeaOperationBackendOptions {
  statement: SeaNativeStatement;
  context: IClientContext;
  /**
   * Optional override for `id`. When not provided a fresh UUIDv4 is used.
   * Reserved for the sea-results / sea-integration features which may
   * thread the kernel-side statement id through once the napi binding
   * surfaces it.
   */
  id?: string;
}

/**
 * Sentinel string the napi binding uses on `Error.reason` JSON envelopes.
 * Keep in sync with `native/sea/src/error.rs` (`SENTINEL`).
 */
const KERNEL_ERROR_SENTINEL = '__databricks_error__:';

/**
 * Inspect a thrown error from the napi binding. If it carries the
 * sentinel-prefixed JSON envelope, parse and re-throw as the mapped JS
 * driver error class; otherwise re-throw verbatim.
 *
 * Used by every method body that crosses the napi boundary so that
 * kernel `ErrorCode` + SQLSTATE are preserved on the JS error surface.
 */
function rethrowKernelError(err: unknown): never {
  if (err && typeof err === 'object' && 'message' in err) {
    const reason = (err as { reason?: unknown }).reason;
    if (typeof reason === 'string' && reason.startsWith(KERNEL_ERROR_SENTINEL)) {
      try {
        const payload = JSON.parse(reason.slice(KERNEL_ERROR_SENTINEL.length)) as KernelErrorShape;
        throw mapKernelErrorToJsError(payload);
      } catch (parseErr) {
        // If JSON.parse failed, fall through to the raw error. The
        // `parseErr` itself is the mapped error if we successfully threw above.
        if (parseErr !== err) {
          throw parseErr;
        }
      }
    }
  }
  throw err;
}

/**
 * SEA-backed implementation of `IOperationBackend`.
 *
 * **M0 scope:** carries the napi `Statement` handle and supports
 * `cancel()` + `close()` (both pass-through to the kernel). The
 * row-fetch / status / result-metadata methods are owned by the
 * `sea-results` feature — until that lands, calling them throws an
 * explicit `M1`-deferred error so consumers fail loudly rather than
 * silently. The `sea-integration` round will reconcile this shim with
 * the real implementation from `sea-results`.
 *
 * **Why a thin shim now:** `sea-execution` (this feature) needs to
 * return an `IOperationBackend` from `SeaSessionBackend.executeStatement`
 * to keep the abstraction's type contract. Splitting the row-fetch
 * implementation into `sea-results` lets the two features land
 * independently in a stacked-PR workflow without one blocking the other.
 */
export default class SeaOperationBackend implements IOperationBackend {
  private readonly statement: SeaNativeStatement;

  // Retained for symmetry with ThriftOperationBackend — logger access happens
  // via `context.getLogger()`. The integration round will lean on this to
  // emit per-operation lifecycle events.
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  private readonly context: IClientContext;

  private readonly _id: string;

  private closed = false;

  private cancelled = false;

  constructor({ statement, context, id }: SeaOperationBackendOptions) {
    this.statement = statement;
    this.context = context;
    this._id = id ?? uuidv4();
  }

  public get id(): string {
    return this._id;
  }

  public get hasResultSet(): boolean {
    // SEA's `Statement::execute` only returns a handle for successfully
    // started statements; rows may be empty but the result-set channel is
    // always available (the kernel's `ResultStream::next_batch` resolves
    // to `None` when exhausted). M0 mirrors the JDBC SEA driver which
    // treats every executed statement as result-set-bearing.
    return true;
  }

  /**
   * Pull the next batch of rows. **Owned by sea-results.** Returning a
   * deferred error here keeps the build green while the row-decoding
   * pipeline (Arrow IPC → JS objects) lands separately.
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async fetchChunk(_options: { limit: number; disableBuffering?: boolean }): Promise<Array<object>> {
    throw new HiveDriverError(
      'SeaOperationBackend.fetchChunk: not implemented yet (lands in sea-results feature)',
    );
  }

  public async hasMore(): Promise<boolean> {
    throw new HiveDriverError(
      'SeaOperationBackend.hasMore: not implemented yet (lands in sea-results feature)',
    );
  }

  /**
   * Wait until the operation reaches a terminal state. The kernel
   * already internalises async polling inside `Statement::execute`, so
   * by the time we hold a `Statement` handle the operation is at least
   * RUNNING or FINISHED. M0 treats this as a no-op; the JDBC SEA driver
   * does the same when the kernel has already absorbed the polling
   * loop. The sea-results feature may override if status callbacks need
   * to fire.
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async waitUntilReady(_options?: {
    progress?: boolean;
    callback?: (progress: TGetOperationStatusResp) => unknown;
  }): Promise<void> {
    // No-op — kernel has already polled to readiness internally.
  }

  /**
   * Single-shot status. M0 synthesises a "finished" response because the
   * kernel surfaces only terminal-or-running statements through its
   * public API. The sea-results feature will tighten this up with the
   * real kernel `StatementStatus` mapping.
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async status(_progress: boolean): Promise<TGetOperationStatusResp> {
    return {
      status: { statusCode: 0 },
      operationState: TOperationState.FINISHED_STATE,
    } as TGetOperationStatusResp;
  }

  public async getResultMetadata(): Promise<TGetResultSetMetadataResp> {
    throw new HiveDriverError(
      'SeaOperationBackend.getResultMetadata: not implemented yet (lands in sea-results feature)',
    );
  }

  public async cancel(): Promise<Status> {
    if (this.cancelled || this.closed) {
      return Status.success();
    }
    try {
      await this.statement.cancel();
    } catch (err) {
      rethrowKernelError(err);
    }
    this.cancelled = true;
    return Status.success();
  }

  public async close(): Promise<Status> {
    if (this.closed) {
      return Status.success();
    }
    try {
      await this.statement.close();
    } catch (err) {
      rethrowKernelError(err);
    }
    this.closed = true;
    return Status.success();
  }
}
