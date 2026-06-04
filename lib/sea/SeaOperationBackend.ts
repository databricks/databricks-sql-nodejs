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
 *   datatype (parity gate exercised by `tests/e2e/sea/results-e2e.test.ts`).
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
import { LogLevel } from '../contracts/IDBSQLLogger';
import Status from '../dto/Status';
import HiveDriverError from '../errors/HiveDriverError';
import OperationStateError, { OperationStateErrorCode } from '../errors/OperationStateError';
import ArrowResultConverter from '../result/ArrowResultConverter';
import ResultSlicer from '../result/ResultSlicer';
import SeaResultsProvider from './SeaResultsProvider';
import { arrowSchemaToThriftSchema, decodeIpcSchema, patchIpcBytes } from './SeaArrowIpc';
import { decodeNapiKernelError } from './SeaErrorMapping';
import {
  SeaStatement,
  SeaNativeAsyncStatement,
  SeaNativeAsyncResultHandle,
  SeaNativeCancellableExecution,
} from './SeaNativeLoader';
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
 * The fetch surface shared by the blocking metadata `Statement` and the async
 * query path's `AsyncResultHandle` (from `awaitResult()`): both expose
 * `fetchNextBatch()` + a synchronous `schema()`, so the results pipeline
 * (`SeaResultsProvider` → `ArrowResultConverter` → `ResultSlicer`) consumes
 * either interchangeably.
 */
type SeaFetchHandle = Pick<SeaStatement, 'fetchNextBatch' | 'schema'>;

/** Poll cadence for the async `status()` loop — matches the Thrift backend's 100ms. */
const STATUS_POLL_INTERVAL_MS = 100;

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

/**
 * Map a kernel `AsyncStatement.status()` string to the backend-neutral
 * `OperationState`. The kernel variant names (`Pending` / `Running` /
 * `Succeeded` / `Failed` / `Cancelled` / `Closed` / `Unknown`) line up 1:1
 * with the enum; `Canceled` (one-L spelling) is mapped defensively, and any
 * unrecognised value collapses to `Unknown`.
 */
function statusStringToOperationState(state: string): OperationState {
  if (state === 'Canceled') {
    return OperationState.Cancelled;
  }
  if ((Object.values(OperationState) as string[]).includes(state)) {
    return state as OperationState;
  }
  return OperationState.Unknown;
}

/**
 * Constructor options for `SeaOperationBackend`. Exactly one of
 * `asyncStatement` (query path — `Connection.submitStatement`) or `statement`
 * (metadata path — `Connection.list*` / `get*`, already terminal) must be set.
 */
export interface SeaOperationBackendOptions {
  /** The pending napi `AsyncStatement` from `Connection.submitStatement(...)`. */
  asyncStatement?: SeaNativeAsyncStatement;
  /** The terminal napi `Statement` from a metadata call. */
  statement?: SeaOperationStatement;
  /**
   * The pending napi `CancellableExecution` from
   * `Connection.executeStatementCancellable(...)` — the sync (`runAsync: false`)
   * query path. `result()` drives the blocking `execute()` to a terminal
   * `Statement` (the fetch handle); `cancel()` fires a detached canceller that
   * interrupts a still-running `result()` mid-COMPUTE. Exactly one of
   * `asyncStatement`, `statement`, or `cancellableExecution` must be set.
   */
  cancellableExecution?: SeaNativeCancellableExecution;
  context: IClientContext;
  /**
   * Optional override for `id`. Defaults to the napi statement-id when the
   * handle exposes one, else a fresh UUIDv4.
   */
  id?: string;
  /**
   * Client-side query timeout in whole seconds (the public `queryTimeout`).
   * The kernel ignores `queryTimeoutSecs` on the async submit path
   * (`submitStatement` always sends `wait_timeout=0s`), so the JS poll loop
   * enforces it as a deadline — on expiry it best-effort cancels the statement
   * and throws `OperationStateError(Timeout)`, matching the Thrift path's
   * server-side TIMEDOUT outcome. Omitted ⇒ no client-side deadline.
   */
  queryTimeoutSecs?: number;
}

export default class SeaOperationBackend implements IOperationBackend {
  // Async query path: pending async statement we poll to terminal. Undefined on
  // the metadata / sync-execute paths.
  private readonly asyncStatement?: SeaNativeAsyncStatement;

  // Sync query path (`runAsync: false`): pending cancellable execution whose
  // `result()` drives the blocking `execute()` to a terminal `Statement`.
  // Undefined on the async / metadata paths.
  private readonly cancellableExecution?: SeaNativeCancellableExecution;

  // Sync path: the server statement id captured from the resolved `Statement`
  // once `result()` settles. Undefined until then; surfaced via `id`.
  private resolvedStatementId?: string;

  // Metadata path: terminal statement. Also the resolved fetch handle on the
  // sync-execute path once `cancellableExecution.result()` settles.
  private blockingStatement?: SeaOperationStatement;

  // The cancel/close surface — whichever handle backs this operation. Both
  // `AsyncStatement` and `Statement` expose `cancel()` / `close()`; the
  // sync-execute path uses a composite that routes `cancel()` to the
  // cancellable execution (mid-compute) and `close()` to the resolved statement.
  private readonly lifecycleHandle: SeaStatementHandle;

  private readonly context: IClientContext;

  private readonly _id: string;

  private readonly lifecycle: SeaOperationLifecycleState = createLifecycleState();

  private resultSlicer?: ResultSlicer<any>;

  private resultsProvider?: SeaResultsProvider;

  private metadata?: ResultMetadata;

  private metadataPromise?: Promise<ResultMetadata>;

  // Memoised fetch handle: on the async path it is `awaitResult()`'s result
  // (resolved once the statement is terminal); on the metadata path it is the
  // already-terminal statement. Drives both fetch and result-metadata.
  private fetchHandlePromise?: Promise<SeaFetchHandle>;

  // Client-side query-timeout deadline in ms (the public `queryTimeout`),
  // undefined when unset. Enforced in the async poll loop.
  private readonly queryTimeoutMs?: number;

  constructor({
    asyncStatement,
    statement,
    cancellableExecution,
    context,
    id,
    queryTimeoutSecs,
  }: SeaOperationBackendOptions) {
    // Exactly one of the three handle kinds must be supplied.
    const providedCount =
      (asyncStatement !== undefined ? 1 : 0) +
      (statement !== undefined ? 1 : 0) +
      (cancellableExecution !== undefined ? 1 : 0);
    if (providedCount !== 1) {
      throw new HiveDriverError(
        'SeaOperationBackend: exactly one of `asyncStatement`, `statement`, or `cancellableExecution` must be provided',
      );
    }
    this.asyncStatement = asyncStatement;
    this.cancellableExecution = cancellableExecution;
    this.blockingStatement = statement;
    // Lifecycle surface. The async/metadata handles expose both cancel/close.
    // The sync-execute path uses a composite: `cancel()` always routes to the
    // cancellable execution (lock-free, interrupts a running `result()`
    // mid-compute and is a no-op once terminal); `close()` routes to the
    // resolved terminal statement once `result()` has produced it (before that
    // there is nothing server-side to close, and the kernel's per-execute drop
    // guard handles an abandoned in-flight execution).
    this.lifecycleHandle = cancellableExecution
      ? {
          cancel: () => cancellableExecution.cancel(),
          close: () => (this.blockingStatement ? this.blockingStatement.close() : Promise.resolve()),
        }
      : ((asyncStatement ?? statement) as SeaStatementHandle);
    this.context = context;
    this._id =
      id ?? asyncStatement?.statementId ?? statement?.statementId ?? cancellableExecution?.statementId ?? uuidv4();
    this.queryTimeoutMs = queryTimeoutSecs !== undefined && queryTimeoutSecs > 0 ? queryTimeoutSecs * 1000 : undefined;
  }

  public get id(): string {
    // On the sync (cancellable) path the server statement id isn't known at
    // construction — it's published mid-`result()` once the initial execute
    // round-trip returns. Surface it once available (preferring the id captured
    // from the resolved `Statement`, then the live canceller slot) so a
    // cancelled/closed sync operation is traceable by the same id the server and
    // kernel logs key on, matching the async path (which has it from `submit`).
    // Falls back to the construction-time id (a client UUID) until then.
    return this.resolvedStatementId ?? this.cancellableExecution?.statementId ?? this._id;
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
    isClosed,
  }: {
    limit: number;
    disableBuffering?: boolean;
    isClosed?: () => boolean;
  }): Promise<Array<object>> {
    // Cancel-mid-fetch propagation: if cancel() has flipped the
    // lifecycle flag, fail locally without a wire round-trip.
    failIfNotActive(this.lifecycle);
    // Cooperative cancel (parity with ThriftOperationBackend): the facade
    // supplies `isClosed` and re-checks `failIfClosed()` after we return, so
    // bailing with `[]` at a yield point is the contract-correct way for a
    // concurrent cancel()/close() to interrupt the fetch.
    if (isClosed?.()) {
      return [];
    }
    try {
      const slicer = await this.getResultSlicer();
      if (isClosed?.()) {
        return [];
      }
      return await slicer.fetchNext({ limit, disableBuffering });
    } catch (err) {
      // The napi fetch contract leaves the stream in an unspecified state on
      // error ("call close() and discard"). Close the statement so the server
      // reclaims it promptly — best-effort, so a close failure never masks the
      // original fetch error — then surface a typed kernel error.
      //
      // If close() ALSO fails, seaClose has reset isClosed back to false and
      // the kernel-side statement handle is now leaked (the stream is already
      // wedged, so nothing downstream forces another close). We still don't
      // mask the original fetch error, but log the close failure at warn so
      // the leak is diagnosable rather than completely invisible.
      await seaClose(this.lifecycle, this.lifecycleHandle, this.context, this._id).catch((closeErr) => {
        const cause = closeErr instanceof Error ? closeErr.message : String(closeErr);
        this.context
          .getLogger()
          .log(
            LogLevel.warn,
            `SEA fetch-error cleanup: close() failed for operation ${this._id}; the server-side ` +
              `statement may leak until the session is closed. Cause: ${cause}`,
          );
      });
      throw decodeNapiKernelError(err);
    }
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
      // The schema lives on the fetch handle: the metadata `Statement`
      // directly, or the async path's `AsyncResultHandle` (materialised by
      // `getFetchHandle()` once the statement is terminal).
      const handle = await this.getFetchHandle();
      if (!handle.schema) {
        throw new HiveDriverError('SeaOperationBackend: schema() is not available on this handle');
      }
      // `schema()` is a synchronous napi getter (returns `ArrowSchema`, not a
      // Promise) — no `await` needed.
      const arrowSchemaIpc = handle.schema();
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
        // Carry the *patched* Arrow IPC schema bytes (Duration → Int64 with the
        // `duration_unit` marker) so an ARROW_BASED consumer decoding
        // `arrowSchema` doesn't hit apache-arrow@13's `Unrecognized type
        // "Duration" (18)`. Matches what the per-batch fetch path already does.
        arrowSchema: patchIpcBytes(arrowSchemaIpc.ipcBytes),
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
    // A client-side cancel/close wins over any server state.
    if (this.lifecycle.isCancelled) {
      return { state: OperationState.Cancelled, hasResultSet: true };
    }
    if (this.lifecycle.isClosed) {
      return { state: OperationState.Closed, hasResultSet: true };
    }
    if (this.asyncStatement) {
      // Async query path: report the real kernel state (single
      // GetStatementStatus RPC — no polling here; `waitUntilReady` owns the
      // poll loop).
      const state = statusStringToOperationState(await this.asyncStatement.status());
      return { state, hasResultSet: true };
    }
    if (this.cancellableExecution) {
      // Sync (`runAsync: false`) path: the kernel `execute()` blocks and polls
      // server-side; there is no per-status RPC to query while it runs. Report
      // Running until `result()` has materialised the terminal statement, then
      // Succeeded — mirroring the kernel's blocking-then-terminal lifecycle.
      const state = this.fetchHandlePromise ? OperationState.Succeeded : OperationState.Running;
      return { state, hasResultSet: true };
    }
    // Metadata path: the kernel statement is already terminal.
    return { state: OperationState.Succeeded, hasResultSet: true };
  }

  public async waitUntilReady(options?: IOperationBackendWaitOptions): Promise<void> {
    if (this.asyncStatement) {
      return this.waitUntilReadyAsync(options);
    }
    if (this.cancellableExecution) {
      return this.waitUntilReadyCancellable(options);
    }
    // Metadata path: the kernel statement has already resolved, so there is
    // nothing to poll. seaFinished fires the progress callback once with a
    // synthesised completion tick, matching the Thrift path's final tick.
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
   * Poll the kernel `AsyncStatement` to a terminal state on a fixed 100ms
   * cadence, mirroring the Thrift backend's `waitUntilReady` loop. We poll
   * `status()` (a cheap GetStatementStatus RPC) rather than awaiting
   * `awaitResult()` directly so that `status()` reports the real
   * Pending/Running/Succeeded state to a progress callback each tick, and so a
   * JS-initiated `cancel()`/`close()` is observed between ticks via
   * `failIfNotActive`. On success it materialises the result handle (so the
   * first fetch is free); on a server-driven terminal state it throws the typed
   * error the `IOperationBackend` contract requires.
   *
   * Terminal errors are thrown as `OperationStateError` (NOT plain
   * `HiveDriverError`) for Cancelled/Closed/Unknown, because the DBSQLOperation
   * facade only mirrors its `cancelled`/`closed` flags when
   * `err instanceof OperationStateError` — exactly as the Thrift backend does.
   * The Failed branch surfaces the kernel's typed SQL-error envelope via
   * `awaitResult()`.
   */
  private async waitUntilReadyAsync(options?: IOperationBackendWaitOptions): Promise<void> {
    // Already materialised → terminal-and-ready, nothing to wait for.
    if (this.fetchHandlePromise) {
      return;
    }
    // Client-side timeout deadline: the kernel ignores queryTimeoutSecs on the
    // async submit path, so we enforce the public `queryTimeout` here.
    const deadline = this.queryTimeoutMs !== undefined ? Date.now() + this.queryTimeoutMs : undefined;
    for (;;) {
      // A JS-initiated cancel/close short-circuits before the next poll.
      failIfNotActive(this.lifecycle);

      // eslint-disable-next-line no-await-in-loop
      const state = statusStringToOperationState(await this.asyncStatement!.status());

      if (options?.callback) {
        // eslint-disable-next-line no-await-in-loop
        await Promise.resolve(options.callback({ state, hasResultSet: true }));
      }

      switch (state) {
        case OperationState.Pending:
        case OperationState.Running:
          break;
        case OperationState.Succeeded:
          // Materialise the result stream now so the first fetch/metadata call
          // doesn't pay an extra await_result round-trip.
          // eslint-disable-next-line no-await-in-loop
          await this.getFetchHandle();
          return;
        case OperationState.Failed:
          // `status()` collapses Failed to the variant name only; the real
          // SQL-error envelope (sql_state / error_code / query_id) rides on
          // `awaitResult()`'s rejection — drive it to surface the typed error.
          // eslint-disable-next-line no-await-in-loop
          await this.throwAsyncError();
          break;
        case OperationState.Cancelled:
          throw new OperationStateError(OperationStateErrorCode.Canceled);
        case OperationState.Closed:
          throw new OperationStateError(OperationStateErrorCode.Closed);
        default:
          throw new OperationStateError(OperationStateErrorCode.Unknown);
      }

      // Still Pending/Running — enforce the client-side timeout before sleeping.
      if (deadline !== undefined && Date.now() >= deadline) {
        // Best-effort server-side cancel so the statement doesn't keep running
        // after we stop waiting; never mask the timeout with a cancel failure.
        // eslint-disable-next-line no-await-in-loop
        await this.cancel().catch(() => undefined);
        throw new OperationStateError(OperationStateErrorCode.Timeout);
      }

      // eslint-disable-next-line no-await-in-loop
      await delay(STATUS_POLL_INTERVAL_MS);
    }
  }

  /**
   * Sync (`runAsync: false`) execute path. Drives the blocking
   * `CancellableExecution.result()` to a terminal `Statement` (the kernel polls
   * to completion server-side, honouring `queryTimeoutSecs` on this path). The
   * await is interruptible: a JS-initiated `cancel()` fires the detached
   * canceller, the server flips the statement terminal, and the parked
   * `result()` rejects with `Cancelled` — which we map to the typed
   * `OperationStateError(Canceled)`.
   *
   * Unlike the async path there is no status poll loop (the kernel owns
   * polling), so the progress callback fires once on completion, matching the
   * metadata path's single completion tick.
   */
  private async waitUntilReadyCancellable(options?: IOperationBackendWaitOptions): Promise<void> {
    // Already materialised → terminal-and-ready, nothing to wait for.
    if (this.fetchHandlePromise) {
      return;
    }
    // A JS-initiated cancel/close before we start short-circuits to the typed
    // state error rather than dispatching the blocking execute.
    failIfNotActive(this.lifecycle);
    // `getFetchHandle()` drives `result()` and memoises the resolved Statement
    // (also stored on `blockingStatement` so `close()` can reach it).
    await this.getFetchHandle();
    // Single completion tick, matching the metadata path.
    if (options?.callback) {
      await Promise.resolve(options.callback({ state: OperationState.Succeeded, hasResultSet: true }));
    }
  }

  /**
   * Drive `awaitResult()` on a Failed statement to surface the kernel's typed
   * SQL-error envelope. Falls back to a generic error if `awaitResult()`
   * unexpectedly resolves instead of rejecting.
   */
  private async throwAsyncError(): Promise<never> {
    try {
      await this.asyncStatement!.awaitResult();
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    throw new HiveDriverError(`SEA operation ${this._id} reported Failed but produced a result.`);
  }

  /**
   * Resolve (and memoise) the fetch handle: `awaitResult()`'s `AsyncResultHandle`
   * on the query path, or the already-terminal `Statement` on the metadata path.
   */
  private getFetchHandle(): Promise<SeaFetchHandle> {
    if (!this.fetchHandlePromise) {
      if (this.asyncStatement) {
        this.fetchHandlePromise = this.asyncStatement.awaitResult().catch((err) => {
          throw decodeNapiKernelError(err);
        }) as Promise<SeaNativeAsyncResultHandle>;
      } else if (this.cancellableExecution) {
        // Sync (`runAsync: false`) path: drive the blocking `result()` to the
        // terminal `Statement`. Store it on `blockingStatement` so `close()` can
        // reach it post-execute, and so a subsequent fetch uses it directly.
        this.fetchHandlePromise = this.cancellableExecution
          .result()
          .then((stmt) => {
            this.blockingStatement = stmt as unknown as SeaOperationStatement;
            // Capture the now-known server statement id (stable on the resolved
            // Statement) so `id` reports it for the rest of the lifecycle.
            this.resolvedStatementId = this.blockingStatement.statementId ?? this.resolvedStatementId;
            return stmt as unknown as SeaFetchHandle;
          })
          .catch((err) => {
            const mapped = decodeNapiKernelError(err);
            // A cancel-induced rejection surfaces as the kernel's Cancelled
            // error; map it to the typed `OperationStateError(Canceled)` so the
            // `DBSQLOperation` facade mirrors its cancelled flag (it only does so
            // for `OperationStateError`), matching the Thrift path. If the
            // operation was cancelled client-side, prefer the typed code
            // regardless of the kernel error text.
            if (this.lifecycle.isCancelled) {
              throw new OperationStateError(OperationStateErrorCode.Canceled);
            }
            throw mapped;
          });
      } else {
        const stmt = this.blockingStatement!;
        if (!stmt.fetchNextBatch) {
          throw new HiveDriverError('SeaOperationBackend: statement.fetchNextBatch() is not available on this handle');
        }
        this.fetchHandlePromise = Promise.resolve(stmt as unknown as SeaFetchHandle);
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
    // SeaResultsProvider consumes only `fetchNextBatch`; both the async result
    // handle and the blocking statement satisfy that surface.
    this.resultsProvider = new SeaResultsProvider(handle as unknown as SeaStatement);
    const converter = new ArrowResultConverter(this.context, this.resultsProvider, metadata);
    this.resultSlicer = new ResultSlicer(this.context, converter);
    return this.resultSlicer;
  }
}
