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
 * `IOperationBackend` implementation for the kernel path.
 *
 * Combines:
 * - **Fetch pipeline (from kernel-results):**
 *   `napi.Statement.fetchNextBatch()` → `KernelResultsProvider` →
 *   `ArrowResultConverter` (Phase 1 + Phase 2; reused unchanged) →
 *   `ResultSlicer` (chunk-size normalisation; reused unchanged). The M0
 *   row shape is byte-identical to the thrift path for every M0
 *   datatype (parity gate exercised by `tests/e2e/kernel/results-e2e.test.ts`).
 *
 * - **Lifecycle (from kernel-operation):** `cancel()` / `close()` /
 *   `finished()` (alias of `waitUntilReady`) delegate to the helpers
 *   in `KernelOperationLifecycle.ts`. The helpers handle idempotency,
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
import { Schema, TypeMap } from 'apache-arrow';
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
import KernelResultsProvider, { KernelBufferHandoffOptions, KernelFetchMode } from './KernelResultsProvider';
import { arrowSchemaToThriftSchema, decodeIpcSchema, patchIpcBytes } from './KernelArrowIpc';
import { isSchemaZeroCopySupported } from './KernelArrowImport';
import { decodeNapiKernelError } from './KernelErrorMapping';
import {
  KernelStatement,
  KernelNativeAsyncStatement,
  KernelNativeAsyncResultHandle,
  KernelNativeCancellableExecution,
} from './KernelNativeLoader';
import {
  KernelStatementHandle,
  KernelOperationLifecycleState,
  createLifecycleState,
  kernelCancel,
  kernelClose,
  kernelFinished,
  failIfNotActive,
} from './KernelOperationLifecycle';

/**
 * Structural union of the lifecycle surface (cancel/close) and the
 * fetch surface (fetchNextBatch/schema). The real napi `Statement`
 * implements both; lifecycle-only test stubs implement only the
 * cancel/close half — fetch methods are accessed lazily and the
 * lifecycle tests never reach that path.
 */
export type KernelOperationStatement = KernelStatementHandle & Partial<KernelStatement>;

/**
 * The fetch surface shared by the blocking metadata `Statement` and the async
 * query path's `AsyncResultHandle` (from `awaitResult()`): both expose
 * `fetchNextBatch()` + a synchronous `schema()`, so the results pipeline
 * (`KernelResultsProvider` → `ArrowResultConverter` → `ResultSlicer`) consumes
 * either interchangeably.
 */
type KernelFetchHandle = Pick<KernelStatement, 'fetchNextBatch' | 'schema'> &
  Partial<Pick<KernelStatement, 'fetchNextBatchCopycage'>>;

/**
 * The rich operation-status surface the kernel exposes on a terminal sync
 * `Statement` (`numModifiedRows` / `displayMessage` / `diagnosticInfo` /
 * `errorDetailsJson`). These accessors live ONLY on the blocking `Statement`
 * (metadata path + sync `runAsync:false` path once `result()` resolves) — the
 * async `AsyncStatement` / `AsyncResultHandle` do not expose them — so the
 * reader below is best-effort and returns an empty record when the handle
 * predates this surface or the operation never resolved to a `Statement`.
 */
type KernelStatusFieldsHandle = Pick<
  KernelStatement,
  'numModifiedRows' | 'displayMessage' | 'diagnosticInfo' | 'errorDetailsJson'
>;

/**
 * The rich operation-status fields, as the kernel returns them (each `null`
 * when the server didn't supply it — e.g. `numModifiedRows` is null for a
 * SELECT). Carried onto the neutral `OperationStatus` and ultimately into the
 * Thrift `TGetOperationStatusResp` so kernel reports parity with the Thrift path.
 */
interface KernelRichStatusFields {
  numModifiedRows: number | null;
  displayMessage: string | null;
  diagnosticInfo: string | null;
  errorDetailsJson: string | null;
}

/** Poll cadence for the async `status()` loop — matches the Thrift backend's 100ms. */
const STATUS_POLL_INTERVAL_MS = 100;

/**
 * Resolve the kernel fetch mode from the environment and, for the copycage
 * mode, package it with the decoded Arrow `schema` the buffer-handoff import
 * needs.
 *
 * `KERNEL_FETCH_MODE` ∈ {ipc, copycage} (default ipc). Returns `undefined`
 * for the IPC path (the provider then re-encodes IPC).
 *
 * Copycage is gated on `isSchemaZeroCopySupported`: if the result schema
 * carries any type the buffer-handoff importer cannot reconstruct
 * (dictionary / union / 64-bit-offset Large variant), the whole result falls
 * back to IPC so it still decodes correctly rather than risk a mis-decode.
 */
function resolveFetchMode(schema: Schema<TypeMap>): KernelBufferHandoffOptions | undefined {
  const mode = (process.env.KERNEL_FETCH_MODE ?? '').toLowerCase() as KernelFetchMode | '';
  if (mode === 'copycage' && isSchemaZeroCopySupported(schema)) {
    return { mode, schema };
  }
  return undefined;
}

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
// Hoisted out of the (hot, 100ms) async poll loop — `Object.values` would
// otherwise allocate a fresh array on every status tick.
const OPERATION_STATE_VALUES = Object.values(OperationState) as string[];

function statusStringToOperationState(state: string): OperationState {
  if (state === 'Canceled') {
    return OperationState.Cancelled;
  }
  if (OPERATION_STATE_VALUES.includes(state)) {
    return state as OperationState;
  }
  return OperationState.Unknown;
}

/**
 * Constructor options for `KernelOperationBackend`. Exactly one of
 * `asyncStatement` (query path — `Connection.submitStatement`) or `statement`
 * (metadata path — `Connection.list*` / `get*`, already terminal) must be set.
 */
export interface KernelOperationBackendOptions {
  /** The pending napi `AsyncStatement` from `Connection.submitStatement(...)`. */
  asyncStatement?: KernelNativeAsyncStatement;
  /** The terminal napi `Statement` from a metadata call. */
  statement?: KernelOperationStatement;
  /**
   * The pending napi `CancellableExecution` from
   * `Connection.executeStatementCancellable(...)` — the sync (`runAsync: false`)
   * query path. `result()` drives the blocking `execute()` to a terminal
   * `Statement` (the fetch handle); `cancel()` fires a detached canceller that
   * interrupts a still-running `result()` mid-COMPUTE. Exactly one of
   * `asyncStatement`, `statement`, or `cancellableExecution` must be set.
   */
  cancellableExecution?: KernelNativeCancellableExecution;
  context: IClientContext;
  /**
   * Optional override for `id`. Defaults to the napi statement-id when the
   * handle exposes one, else a fresh UUIDv4.
   */
  id?: string;
}

export default class KernelOperationBackend implements IOperationBackend {
  // Async query path: pending async statement we poll to terminal. Undefined on
  // the metadata / sync-execute paths.
  private readonly asyncStatement?: KernelNativeAsyncStatement;

  // Sync query path (`runAsync: false`): pending cancellable execution whose
  // `result()` drives the blocking `execute()` to a terminal `Statement`.
  // Undefined on the async / metadata paths.
  private readonly cancellableExecution?: KernelNativeCancellableExecution;

  // Metadata path: terminal statement. Also the resolved fetch handle on the
  // sync-execute path once `cancellableExecution.result()` settles.
  private blockingStatement?: KernelOperationStatement;

  // Memoized rich-status read. `readRichStatusFields()` is only ever invoked
  // once the operation is terminal (the Succeeded branches of `status()` and
  // the `kernelFinished` progress callback), and the kernel's terminal response is
  // immutable, so the FFI accessors are read exactly once and the result
  // reused — re-`status()`-ing a completed operation is then free.
  private richStatusFieldsPromise?: Promise<KernelRichStatusFields>;

  // The cancel/close surface — whichever handle backs this operation. Both
  // `AsyncStatement` and `Statement` expose `cancel()` / `close()`; the
  // sync-execute path uses a composite that routes `cancel()` to the
  // cancellable execution (mid-compute) and `close()` to the resolved statement.
  private readonly lifecycleHandle: KernelStatementHandle;

  private readonly context: IClientContext;

  private readonly _id: string;

  private readonly lifecycle: KernelOperationLifecycleState = createLifecycleState();

  private resultSlicer?: ResultSlicer<any>;

  private resultsProvider?: KernelResultsProvider;

  private metadata?: ResultMetadata;

  // The result's decoded Arrow schema, captured in `getResultMetadata`. The
  // copycage buffer-handoff path needs it to drive `makeData` reconstruction
  // (`KernelArrowImport`); undefined until metadata has been fetched.
  private arrowSchema?: Schema<TypeMap>;

  private metadataPromise?: Promise<ResultMetadata>;

  // Memoised fetch handle: on the async path it is `awaitResult()`'s result
  // (resolved once the statement is terminal); on the metadata path it is the
  // already-terminal statement. Drives both fetch and result-metadata.
  private fetchHandlePromise?: Promise<KernelFetchHandle>;

  constructor({ asyncStatement, statement, cancellableExecution, context, id }: KernelOperationBackendOptions) {
    // Exactly one of the three handle kinds must be supplied.
    const providedCount =
      (asyncStatement !== undefined ? 1 : 0) +
      (statement !== undefined ? 1 : 0) +
      (cancellableExecution !== undefined ? 1 : 0);
    if (providedCount !== 1) {
      throw new HiveDriverError(
        'KernelOperationBackend: exactly one of `asyncStatement`, `statement`, or `cancellableExecution` must be provided',
      );
    }
    this.asyncStatement = asyncStatement;
    this.cancellableExecution = cancellableExecution;
    this.blockingStatement = statement;
    // Lifecycle surface. The async/metadata handles expose both cancel/close.
    // The sync-execute path uses a composite: `cancel()` always routes to the
    // cancellable execution (lock-free, interrupts a running `result()`
    // mid-compute and is a no-op once terminal); `close()` closes the resolved
    // terminal statement once `result()` produced it, OR — if `result()` is
    // still in flight — proactively cancels the running execution so the server
    // stops computing immediately rather than running on until the kernel's
    // drop-guard fires whenever this handle is eventually GC'd.
    this.lifecycleHandle = cancellableExecution
      ? {
          cancel: () => cancellableExecution.cancel(),
          close: () => (this.blockingStatement ? this.blockingStatement.close() : cancellableExecution.cancel()),
        }
      : ((asyncStatement ?? statement) as KernelStatementHandle);
    this.context = context;
    this._id =
      id ?? asyncStatement?.statementId ?? statement?.statementId ?? cancellableExecution?.statementId ?? uuidv4();
  }

  public get id(): string {
    // STABLE for the operation's lifetime. The facade keys telemetry start/
    // complete on this value (DBSQLOperation → MetricsAggregator), so it must
    // NOT mutate — a sync op's server statement_id isn't known until `result()`
    // resolves (mid-execute), and flipping `id` then would split the start/
    // complete records across two keys and silently drop the summary. The
    // resolved server statement_id is instead surfaced via a debug log (see
    // `getFetchHandle`) for server/kernel log correlation. On the async path
    // `_id` already IS the server id (available at submit).
    return this._id;
  }

  public hasResultSet(): boolean {
    // M0 only routes through KernelOperationBackend for executeStatement
    // calls. DDL/DML without a result set is not exercised through kernel
    // for M0; the napi Statement still produces a schema (empty) in
    // that case, which the converter renders as zero rows. Reporting
    // `true` keeps the facade's fetch path enabled for M0 parity.
    return true;
  }

  // ---------------------------------------------------------------------------
  // Fetch / metadata (owned by the kernel-results pipeline).
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
      // If close() ALSO fails, kernelClose has reset isClosed back to false and
      // the kernel-side statement handle is now leaked (the stream is already
      // wedged, so nothing downstream forces another close). We still don't
      // mask the original fetch error, but log the close failure at warn so
      // the leak is diagnosable rather than completely invisible.
      await kernelClose(this.lifecycle, this.lifecycleHandle, this.context, this._id).catch((closeErr) => {
        const cause = closeErr instanceof Error ? closeErr.message : String(closeErr);
        this.context
          .getLogger()
          .log(
            LogLevel.warn,
            `kernel fetch-error cleanup: close() failed for operation ${this._id}; the server-side ` +
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
        throw new HiveDriverError('KernelOperationBackend: schema() is not available on this handle');
      }
      // `schema()` is a synchronous napi getter (returns `ArrowSchema`, not a
      // Promise) — no `await` needed.
      const arrowSchemaIpc = handle.schema();
      const arrowSchema = decodeIpcSchema(arrowSchemaIpc.ipcBytes);
      // Cache the decoded Arrow schema for the copycage buffer-handoff path
      // (`getResultSlicer` → `resolveFetchMode`), which rebuilds `RecordBatch`es
      // from the kernel's in-cage buffers via `makeData`.
      this.arrowSchema = arrowSchema;
      // `ResultMetadata.schema` keeps the Thrift `TTableSchema` shape for
      // back-compat with the public `IOperation.getSchema()` surface.
      const thriftSchema: TTableSchema = arrowSchemaToThriftSchema(arrowSchema);
      const meta: ResultMetadata = {
        schema: thriftSchema,
        // kernel inline + CloudFetch both surface to JS as Arrow batches;
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
  // Status / lifecycle (owned by the kernel-operation lifecycle helpers).
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
      // poll loop). On a terminal Succeeded state, that same GetStatement poll
      // carried the rich status fields (the kernel derives a DML's
      // `numModifiedRows` from the inline count on the terminal response), so
      // surface them — mirroring the sync path. Reading them does not fetch the
      // result.
      const state = statusStringToOperationState(await this.asyncStatement.status());
      if (state === OperationState.Succeeded) {
        return { state, hasResultSet: true, ...(await this.readRichStatusFields()) };
      }
      return { state, hasResultSet: true };
    }
    if (this.cancellableExecution) {
      // Sync (`runAsync: false`) path: the kernel `execute()` blocks and polls
      // server-side; there is no per-status RPC to query while it runs. Report
      // Running until `result()` has materialised the terminal statement, then
      // Succeeded — mirroring the kernel's blocking-then-terminal lifecycle.
      //
      // Gate on `blockingStatement`, NOT on `fetchHandlePromise`: the latter is
      // assigned synchronously when `getFetchHandle()` is first called (e.g. by
      // a concurrent fetch or `waitUntilReady`), while `result()` may still be
      // pending — `blockingStatement` is only set inside the resolve handler.
      // Using the promise's *existence* as a completion proxy would make a
      // concurrent `status()` poll both report Succeeded early AND block on the
      // pending `result()` via `readRichStatusFields()`.
      if (!this.blockingStatement) {
        return { state: OperationState.Running, hasResultSet: true };
      }
      // The blocking `result()` has resolved a terminal `Statement` — surface
      // its rich status fields alongside the Succeeded state.
      return { state: OperationState.Succeeded, hasResultSet: true, ...(await this.readRichStatusFields()) };
    }
    // Metadata path: the kernel statement is already terminal — read its rich
    // fields too (they are `null` for metadata results, by design).
    return { state: OperationState.Succeeded, hasResultSet: true, ...(await this.readRichStatusFields()) };
  }

  public async waitUntilReady(options?: IOperationBackendWaitOptions): Promise<void> {
    if (this.asyncStatement) {
      return this.waitUntilReadyAsync(options);
    }
    if (this.cancellableExecution) {
      return this.waitUntilReadyCancellable(options);
    }
    // Metadata path: the kernel statement has already resolved, so there is
    // nothing to poll. kernelFinished fires the progress callback once with a
    // synthesised completion tick, matching the Thrift path's final tick. The
    // rich-field reader is passed lazily so it only runs when a callback is
    // wired (metadata statements report all-null, but the surface stays
    // consistent with the query paths).
    return kernelFinished(this.lifecycle, options, () => this.readRichStatusFields());
  }

  public async cancel(): Promise<Status> {
    return kernelCancel(this.lifecycle, this.lifecycleHandle, this.context, this._id);
  }

  public async close(): Promise<Status> {
    return kernelClose(this.lifecycle, this.lifecycleHandle, this.context, this._id);
  }

  // ---------------------------------------------------------------------------
  // Internals.
  // ---------------------------------------------------------------------------

  /**
   * Read the kernel's rich operation-status fields (`numModifiedRows` /
   * `displayMessage` / `diagnosticInfo` / `errorDetailsJson`) off the terminal
   * sync `Statement`. These accessors live only on the blocking `Statement`
   * (metadata path, or the sync `runAsync:false` path once `result()` has
   * resolved) — not on the async `AsyncStatement` / `AsyncResultHandle` — so:
   *
   * - on the async path we have no `Statement`, so we return all-null;
   * - on the sync path we await `getFetchHandle()` first, which both drives
   *   `result()` to completion and stores the resolved `Statement` on
   *   `blockingStatement` (the handle that backs the accessors);
   * - if the (older) binding predates these accessors we degrade to all-null
   *   rather than throwing — `getOperationStatus()` must never fail just
   *   because the rich fields are unavailable.
   *
   * Errors from the individual accessors are swallowed to null: a failed
   * status-field read must not turn a successful operation's status query into
   * a throw. The fields are best-effort metadata, not the operation outcome.
   */
  private readRichStatusFields(): Promise<KernelRichStatusFields> {
    if (!this.richStatusFieldsPromise) {
      this.richStatusFieldsPromise = this.computeRichStatusFields();
    }
    return this.richStatusFieldsPromise;
  }

  private async computeRichStatusFields(): Promise<KernelRichStatusFields> {
    const empty: KernelRichStatusFields = {
      numModifiedRows: null,
      displayMessage: null,
      diagnosticInfo: null,
      errorDetailsJson: null,
    };

    // Async (submit/poll) path: a DML's `numModifiedRows` rides in the result
    // set, which kernel delivers on the terminal `GetStatement` poll. The kernel
    // derives it off that poll (no extra fetch) and exposes it on the
    // `AsyncStatement`'s status accessors — so read them directly. The value is
    // populated once the statement has reached a terminal state via
    // `status()` / `waitUntilReady` (which polled it); it stays null before
    // that and for SELECTs. Reading does NOT force a result materialisation, so
    // async streaming is untouched.
    if (this.asyncStatement) {
      return this.readStatusFieldsFrom(this.asyncStatement);
    }

    // Ensure the sync path's blocking `result()` has resolved and stored the
    // terminal `Statement` on `blockingStatement` (no-op on the metadata path,
    // where `blockingStatement` was set at construction).
    if (this.cancellableExecution) {
      try {
        await this.getFetchHandle();
      } catch {
        // The operation failed/cancelled — its outcome surfaces through the
        // wait/fetch path; status-field reads have nothing to add.
        return empty;
      }
    }

    return this.readStatusFieldsFrom(this.blockingStatement);
  }

  /**
   * Read the four rich-status accessors (`numModifiedRows` / `displayMessage` /
   * `diagnosticInfo` / `errorDetailsJson`) off a kernel handle — the terminal
   * sync `Statement` or the async `AsyncStatement`, which expose the same
   * accessor shape. Per-field read errors are swallowed to `null`: a failed
   * status-field read must never turn a successful operation's status query
   * into a throw. Degrades to all-null for a missing handle or a binding that
   * predates the accessors.
   */
  private async readStatusFieldsFrom(handle: unknown): Promise<KernelRichStatusFields> {
    const empty: KernelRichStatusFields = {
      numModifiedRows: null,
      displayMessage: null,
      diagnosticInfo: null,
      errorDetailsJson: null,
    };

    const candidate = handle as Partial<KernelStatusFieldsHandle> | undefined;
    if (!candidate || typeof candidate.numModifiedRows !== 'function') {
      return empty;
    }
    const richHandle = candidate as KernelStatusFieldsHandle;

    const readOrNull = async <T>(read: () => Promise<T | null>): Promise<T | null> => {
      try {
        return await read();
      } catch (err) {
        this.context
          .getLogger()
          .log(
            LogLevel.debug,
            `kernel status-field read failed for operation ${this._id}; reporting null. Cause: ` +
              `${err instanceof Error ? err.message : String(err)}`,
          );
        return null;
      }
    };

    const [numModifiedRows, displayMessage, diagnosticInfo, errorDetailsJson] = await Promise.all([
      readOrNull(() => richHandle.numModifiedRows()),
      readOrNull(() => richHandle.displayMessage()),
      readOrNull(() => richHandle.diagnosticInfo()),
      readOrNull(() => richHandle.errorDetailsJson()),
    ]);

    return { numModifiedRows, displayMessage, diagnosticInfo, errorDetailsJson };
  }

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
    for (;;) {
      // A JS-initiated cancel/close short-circuits before the next poll.
      failIfNotActive(this.lifecycle);

      // eslint-disable-next-line no-await-in-loop
      const state = statusStringToOperationState(await this.asyncStatement!.status());

      if (options?.callback) {
        // On the terminal Succeeded tick, carry the rich status fields
        // (numModifiedRows / displayMessage / diagnosticInfo / errorDetailsJson)
        // so the async path's progress callback matches the sync path
        // (`waitUntilReadyCancellable`). The kernel has populated the
        // AsyncStatement's accessors off the just-completed terminal poll, and
        // the read is memoised + does not force result materialisation.
        // eslint-disable-next-line no-await-in-loop
        const richFields = state === OperationState.Succeeded ? await this.readRichStatusFields() : {};
        // eslint-disable-next-line no-await-in-loop
        await Promise.resolve(options.callback({ state, hasResultSet: true, ...richFields }));
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
          // `awaitResult()`'s rejection — drive it to surface the typed error,
          // then best-effort close the leaked statement before it propagates.
          try {
            // eslint-disable-next-line no-await-in-loop
            await this.throwAsyncError();
          } catch (failErr) {
            // eslint-disable-next-line no-await-in-loop
            await this.bestEffortClose();
            throw failErr;
          }
          break;
        case OperationState.Cancelled:
          // eslint-disable-next-line no-await-in-loop
          await this.bestEffortClose();
          throw new OperationStateError(OperationStateErrorCode.Canceled);
        case OperationState.Closed:
          // eslint-disable-next-line no-await-in-loop
          await this.bestEffortClose();
          throw new OperationStateError(OperationStateErrorCode.Closed);
        default:
          // eslint-disable-next-line no-await-in-loop
          await this.bestEffortClose();
          throw new OperationStateError(OperationStateErrorCode.Unknown);
      }

      // Still Pending/Running — sleep before the next poll. (There is no
      // client-side query-timeout deadline: `queryTimeout` is a no-op on kernel.)
      // eslint-disable-next-line no-await-in-loop
      await delay(STATUS_POLL_INTERVAL_MS);
    }
  }

  /**
   * Sync (`runAsync: false`) execute path. Drives the blocking
   * `CancellableExecution.result()` to a terminal `Statement` (the kernel polls
   * to completion server-side). The
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
    // Single completion tick, matching the metadata path — carrying the rich
    // status fields (numModifiedRows etc.) read off the now-terminal Statement.
    if (options?.callback) {
      const richFields = await this.readRichStatusFields();
      await Promise.resolve(options.callback({ state: OperationState.Succeeded, hasResultSet: true, ...richFields }));
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
    throw new HiveDriverError(`kernel operation ${this._id} reported Failed but produced a result.`);
  }

  /**
   * Best-effort close of the kernel statement when the poll loop ends on a
   * server-driven terminal error (Failed/Cancelled/Closed/Unknown/Timeout).
   * Without it the kernel-side statement handle leaks until session close (the
   * poll loop, unlike `fetchChunk`, otherwise just throws). Never masks the
   * original error; warn-logs a close failure so the leak is diagnosable.
   */
  private async bestEffortClose(): Promise<void> {
    await kernelClose(this.lifecycle, this.lifecycleHandle, this.context, this._id).catch((closeErr) => {
      const cause = closeErr instanceof Error ? closeErr.message : String(closeErr);
      this.context
        .getLogger()
        .log(
          LogLevel.warn,
          `kernel poll-loop cleanup: close() failed for operation ${this._id}; the server-side ` +
            `statement may leak until the session is closed. Cause: ${cause}`,
        );
    });
  }

  /**
   * Resolve (and memoise) the fetch handle: `awaitResult()`'s `AsyncResultHandle`
   * on the query path, or the already-terminal `Statement` on the metadata path.
   */
  private getFetchHandle(): Promise<KernelFetchHandle> {
    if (!this.fetchHandlePromise) {
      if (this.asyncStatement) {
        this.fetchHandlePromise = this.asyncStatement.awaitResult().catch((err) => {
          throw decodeNapiKernelError(err);
        }) as Promise<KernelNativeAsyncResultHandle>;
      } else if (this.cancellableExecution) {
        // Sync (`runAsync: false`) path: drive the blocking `result()` to the
        // terminal `Statement`. Store it on `blockingStatement` so `close()` can
        // reach it post-execute, and so a subsequent fetch uses it directly.
        this.fetchHandlePromise = this.cancellableExecution
          .result()
          .then((stmt) => {
            this.blockingStatement = stmt as unknown as KernelOperationStatement;
            // Log the now-known server statement id (NOT surfaced via `id`,
            // which must stay stable for telemetry correlation) so a sync op is
            // correlatable to server/kernel logs by its client operation id.
            const serverId = this.blockingStatement.statementId;
            if (serverId && serverId !== this._id) {
              this.context
                .getLogger()
                .log(LogLevel.debug, `kernel operation ${this._id} resolved to server statement_id ${serverId}`);
            }
            return stmt as unknown as KernelFetchHandle;
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
          throw new HiveDriverError(
            'KernelOperationBackend: statement.fetchNextBatch() is not available on this handle',
          );
        }
        this.fetchHandlePromise = Promise.resolve(stmt as unknown as KernelFetchHandle);
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
    // KernelResultsProvider consumes `fetchNextBatch` (IPC) plus an optional
    // `fetchNextBatchCopycage`; both the async result handle and the blocking
    // statement satisfy that surface.
    //
    // Copycage fetch (in-cage Arrow buffer copies, no IPC re-encode) is opt-in
    // via `KERNEL_FETCH_MODE=copycage` (default ipc). It needs the decoded
    // Arrow schema to rebuild `RecordBatch`es and is gated on the schema being
    // reconstructible (`resolveFetchMode` → `isSchemaZeroCopySupported`); the
    // provider further gates on the binding exposing `fetchNextBatchCopycage`.
    const bufferHandoff = this.arrowSchema !== undefined ? resolveFetchMode(this.arrowSchema) : undefined;
    this.resultsProvider = new KernelResultsProvider(handle as unknown as KernelStatement, bufferHandoff);
    // DECIMAL/BIGINT precision preservation is opt-in via the
    // `preserveBigNumericPrecision` connection option (default off). The kernel
    // always delivers native Arrow Decimal128 / Int64, so when enabled the
    // converter renders DECIMAL as an exact string and BIGINT as a `bigint`.
    const converter = new ArrowResultConverter(this.context, this.resultsProvider, metadata, {
      preserveBigNumericPrecision: this.context.getConfig().preserveBigNumericPrecision ?? false,
    });
    this.resultSlicer = new ResultSlicer(this.context, converter);
    return this.resultSlicer;
  }
}
