/**
 * Backend-neutral operation state. Mirrors the kernel/pyo3 `StatementStatus`
 * and the Python connector's `CommandState`, so a SEA `IOperationBackend`
 * implementer can return these without depending on the Thrift wire enum.
 *
 * Thrift mapping (in `ThriftOperationBackend.adaptOperationStatus`):
 *   - INITIALIZED_STATE, PENDING_STATE → Pending
 *   - RUNNING_STATE                    → Running
 *   - FINISHED_STATE                   → Succeeded
 *   - CANCELED_STATE                   → Cancelled
 *   - CLOSED_STATE                     → Closed
 *   - ERROR_STATE, TIMEDOUT_STATE      → Failed
 *   - UKNOWN_STATE / anything else     → Unknown
 */
export enum OperationState {
  Pending = 'Pending',
  Running = 'Running',
  Succeeded = 'Succeeded',
  Failed = 'Failed',
  Cancelled = 'Cancelled',
  Closed = 'Closed',
  Unknown = 'Unknown',
}

/**
 * Neutral status snapshot returned by `IOperationBackend.status()`. Backends
 * adapt their wire format at the boundary; callers in `DBSQLOperation` and
 * `IOperationBackend.waitUntilReady` switch on `state` alone.
 *
 * Fields beyond `state` are best-effort and may be undefined depending on
 * what the backend exposes.
 */
export interface OperationStatus {
  /** Current operation state. */
  state: OperationState;

  /**
   * Whether this operation has produced (or is producing) a result set.
   * Some backends only know this after the operation reaches a terminal
   * state — undefined means "no signal from this backend".
   */
  hasResultSet?: boolean;

  /** Human-readable error/display message, if the backend supplied one. */
  errorMessage?: string;

  /** SQL state code (e.g. "42000"), if available. */
  sqlState?: string;

  /**
   * Opaque progress payload as returned by the backend when callers pass
   * `progress: true`. Treated as untyped by the facade — passed through
   * to `WaitUntilReadyOptions.callback` for the consumer to interpret.
   */
  progressUpdateResponse?: unknown;

  /**
   * Number of rows modified by a DML statement (UPDATE / INSERT / DELETE /
   * MERGE). `undefined`/`null` for SELECT and on backends/warehouses that do
   * not surface the counter. Mirrors Thrift's
   * `TGetOperationStatusResp.numModifiedRows`.
   */
  numModifiedRows?: number | null;

  /**
   * Server-supplied user-facing message, when the backend exposes one. Mirrors
   * Thrift's `TGetOperationStatusResp.displayMessage`. May contain SQL
   * fragments or parameter values — treat as potentially sensitive.
   */
  displayMessage?: string | null;

  /**
   * Server-supplied diagnostic detail (multi-line operator / stack context),
   * when available. Mirrors Thrift's `TGetOperationStatusResp.diagnosticInfo`.
   * For support surfaces, not user-facing.
   */
  diagnosticInfo?: string | null;

  /**
   * Server-supplied JSON blob with extended error details, when available.
   * Mirrors Thrift's `TGetOperationStatusResp.errorDetailsJson`. Pass-through
   * string — callers parse with `JSON.parse` if they need structured access.
   */
  errorDetailsJson?: string | null;
}
