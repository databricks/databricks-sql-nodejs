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
 * SEA operation lifecycle helpers (M0).
 *
 * The three methods exposed here (`cancel`, `close`, `finished`) are
 * standalone functions that the `SeaOperationBackend` implementation
 * delegates to. Keeping them in this dedicated file lets the parallel
 * impl-results work (which owns the fetch-* methods on
 * `SeaOperationBackend`) land independently — at merge time it can
 * either import these helpers from here or inline them, with no
 * conflicts on the call sites.
 *
 * Mapping to the existing `DBSQLOperation` semantics:
 * - `cancel()` → ` driver.cancelOperation(...)` on Thrift today
 *   (`lib/DBSQLOperation.ts:241-259`). For SEA this is a one-shot
 *   forward to the napi `Statement.cancel()` which in turn calls
 *   `ExecutedStatementHandle::cancel(&self).await` in the kernel.
 * - `close()` → `driver.closeOperation(...)` on Thrift today
 *   (`lib/DBSQLOperation.ts:265-284`). For SEA this is the napi
 *   `Statement.close()` which awaits the server-side delete.
 * - `finished({progress, callback})` → the 100ms polling loop in
 *   `DBSQLOperation.waitUntilReady` today (`lib/DBSQLOperation.ts:337-391`).
 *   For M0 the kernel's `Statement::execute().await` already blocks
 *   until the statement is in a terminal state, so by the time the JS
 *   side has an `ExecutedStatement` (and therefore a binding-level
 *   `Statement`) the underlying operation is already finished. The
 *   M0 implementation here therefore resolves immediately, optionally
 *   firing the progress callback once with a synthesized "finished"
 *   response so callers that wire a progress UI still see a single
 *   completion tick.
 */

import Status from '../dto/Status';
import { OperationStatus, OperationState } from '../contracts/OperationStatus';
import { LogLevel } from '../contracts/IDBSQLLogger';
import IClientContext from '../contracts/IClientContext';
import { decodeNapiKernelError } from './SeaErrorMapping';
import OperationStateError, { OperationStateErrorCode } from '../errors/OperationStateError';

/**
 * Minimal shape of the napi `Statement` that the lifecycle helpers
 * depend on. Declared structurally so unit tests can hand in a mock
 * without pulling the real native binding into the test process.
 *
 * The real binding's `Statement` (see `native/sea/index.d.ts`) has
 * additional methods (`fetchNextBatch`, `schema`) which the lifecycle
 * helpers deliberately don't touch — those belong to the results
 * feature's surface.
 */
export interface SeaStatementHandle {
  cancel(): Promise<void>;
  close(): Promise<void>;
}

/**
 * Internal lifecycle state shared between the operation backend and
 * these helpers. `SeaOperationBackend` keeps an instance of this and
 * passes it to each helper call. Centralising the flags here means
 * the helpers stay pure (no `this`) and the backend stays
 * straightforward.
 */
export interface SeaOperationLifecycleState {
  /** True once `cancel()` has succeeded — subsequent fetch* must throw. */
  isCancelled: boolean;
  /** True once `close()` has been called (idempotent). */
  isClosed: boolean;
}

/**
 * Factory for a fresh lifecycle-state record. Helps keep test setup
 * tidy.
 */
export function createLifecycleState(): SeaOperationLifecycleState {
  return { isCancelled: false, isClosed: false };
}

/**
 * Normalise an error thrown by the napi `Statement` into one of the
 * driver's typed error classes, then throw it.
 *
 * Delegates to the canonical {@link decodeNapiKernelError} so cancel /
 * close errors get exactly the same fidelity as fetch errors: the
 * `sqlState` remap (the envelope field is `sqlState`, not `sqlstate`),
 * the `kernelMetadata` namespace (vendorCode / httpStatus / retryable /
 * queryId), and the strict `startsWith` sentinel match. The previous
 * hand-rolled reimplementation here dropped SQLSTATE and metadata and
 * used a looser substring match.
 */
function rethrowKernelError(err: unknown): never {
  throw decodeNapiKernelError(err);
}

/**
 * Cancel an in-flight SEA operation.
 *
 * Mirrors `DBSQLOperation.cancel` semantics
 * (`lib/DBSQLOperation.ts:241-259`):
 * - idempotent: returns success if already cancelled or closed
 *   (no-ops are not bubbled to the kernel because the binding's
 *   `Statement::cancel` already treats already-finished statements as
 *   a no-op, but we still want to avoid a network round-trip here),
 * - sets the cancelled flag _before_ awaiting the napi call so that a
 *   concurrent `fetchChunk()` observing the flag short-circuits as
 *   soon as the await yields (matches the Thrift flag-set ordering
 *   at `lib/DBSQLOperation.ts:254`),
 * - returns a `Status.success()` on success (no rich Thrift status
 *   payload is available from the kernel side).
 */
export async function seaCancel(
  state: SeaOperationLifecycleState,
  statement: SeaStatementHandle,
  context: IClientContext,
  operationId: string,
): Promise<Status> {
  if (state.isCancelled || state.isClosed) {
    return Status.success();
  }

  context.getLogger().log(LogLevel.debug, `Cancelling SEA operation with id: ${operationId}`);

  state.isCancelled = true;

  try {
    await statement.cancel();
  } catch (err) {
    state.isCancelled = false;
    rethrowKernelError(err);
  }

  return Status.success();
}

/**
 * Close a SEA operation.
 *
 * Mirrors `DBSQLOperation.close` semantics
 * (`lib/DBSQLOperation.ts:265-284`) without the Thrift-only
 * direct-results-prefetch optimisation:
 * - idempotent: a second call is a no-op,
 * - awaits the binding's `Statement::close` (which goes through to
 *   the kernel's `delete_statement` RPC),
 * - sets the closed flag _before_ awaiting so a concurrent fetch
 *   sees the closed state as soon as the await yields.
 */
export async function seaClose(
  state: SeaOperationLifecycleState,
  statement: SeaStatementHandle,
  context: IClientContext,
  operationId: string,
): Promise<Status> {
  if (state.isClosed) {
    return Status.success();
  }

  context.getLogger().log(LogLevel.debug, `Closing SEA operation with id: ${operationId}`);

  state.isClosed = true;

  try {
    await statement.close();
  } catch (err) {
    state.isClosed = false;
    rethrowKernelError(err);
  }

  return Status.success();
}

/**
 * Synthesize a neutral {@link OperationStatus} reporting the "finished"
 * state. `IOperationBackend.waitUntilReady` is backend-neutral surface — its
 * `callback` receives an {@link OperationStatus}, not a Thrift wire struct
 * (the public Thrift-shaped `OperationStatusCallback` is adapted at the
 * `DBSQLOperation` facade boundary). For M0 we report `Succeeded`. Richer
 * fields (`numModifiedRows`, `progressUpdateResponse`, `errorMessage`) defer
 * to M1 per the operation feature plan.
 */
function synthesizeFinishedStatus(): OperationStatus {
  return {
    state: OperationState.Succeeded,
    hasResultSet: true,
  };
}

/**
 * `IOperation.finished({progress, callback})` M0 implementation.
 *
 * The Thrift implementation is a 100ms polling loop over
 * `getOperationStatus` (`lib/DBSQLOperation.ts:337-391`). For SEA M0,
 * the kernel's `Statement::execute().await` already blocks until the
 * statement reaches a terminal state — by the time the JS layer has
 * a `Statement` handle, the operation has already finished.
 *
 * Therefore the M0 implementation resolves immediately. If the
 * caller supplied a progress callback we still invoke it once (a
 * single completion tick) so progress-UI consumers see the same
 * "operation is now finished" signal they'd get from the polling
 * Thrift path — just without the intermediate `RUNNING_STATE`
 * notifications.
 *
 * If the operation is already cancelled or closed, this is a no-op
 * (matches the Thrift `failIfClosed` / cancelled-state semantics
 * without throwing; throwing is the responsibility of subsequent
 * fetch calls).
 */
export async function seaFinished(
  state: SeaOperationLifecycleState,
  options?: {
    progress?: boolean;
    callback?: (status: OperationStatus) => unknown;
  },
): Promise<void> {
  if (state.isCancelled || state.isClosed) {
    return;
  }

  if (options?.callback) {
    const response = synthesizeFinishedStatus();
    // Await the callback in case it returns a promise — matches the
    // Thrift code path at `lib/DBSQLOperation.ts:348-351`.
    await Promise.resolve(options.callback(response));
  }
}

/**
 * Pre-flight check used by fetch* methods on `SeaOperationBackend`.
 * If the operation has been cancelled or closed, throw the same
 * `OperationStateError` classes the facade uses. Keeping these typed lets
 * callers branch on `OperationStateErrorCode` consistently for Thrift and SEA.
 *
 * Exported so impl-results can call it at the top of every fetch
 * call without duplicating the if/throw logic.
 */
export function failIfNotActive(state: SeaOperationLifecycleState): void {
  if (state.isCancelled) {
    // Use the canonical `OperationStateError(Canceled)` (message "The operation
    // was canceled by a client") rather than a custom string, so the message
    // matches the Thrift path verbatim and the code branch stays consistent
    // with the Closed case below.
    throw new OperationStateError(OperationStateErrorCode.Canceled);
  }
  if (state.isClosed) {
    throw new OperationStateError(OperationStateErrorCode.Closed);
  }
}
