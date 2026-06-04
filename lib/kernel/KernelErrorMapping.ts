import HiveDriverError from '../errors/HiveDriverError';
import AuthenticationError from '../errors/AuthenticationError';
import OperationStateError, { OperationStateErrorCode } from '../errors/OperationStateError';
import ParameterError from '../errors/ParameterError';

/**
 * Sentinel prefix the napi binding's `napi_err_from_kernel` puts on
 * `Error.message` when the underlying failure was a structured kernel
 * `Error` rather than a plain napi `InvalidArg` from binding-side
 * validation. Defined here (and in `native/kernel/src/error.rs:44`) — the
 * two MUST stay in lockstep.
 */
const ERROR_SENTINEL = '__databricks_error__:';

/**
 * Shape of the kernel error surfaced by the napi-binding's `napi_err_from_kernel`.
 *
 * The Rust kernel's `kernel_error::Error` is exposed as a `JsError` whose
 * properties mirror the Rust struct: the `ErrorCode` variant name (as a string),
 * the message, and an optional SQLSTATE (either taken from the structured
 * server response or recovered via `extract_sqlstate_from_message`).
 */
export interface KernelErrorShape {
  /** Kernel `ErrorCode` variant name, e.g. `"Unauthenticated"`, `"SqlError"`. */
  code: string;
  /** Human-readable error message. */
  message: string;
  /** Optional SQLSTATE — five-char alphanumeric, when the kernel was able to surface it. */
  sqlstate?: string;
}

/**
 * Kernel `ErrorCode` variants — the 13 variants of the `#[non_exhaustive]` enum
 * defined in `src/kernel_error.rs:66-134`.
 *
 * Kept here as a literal type rather than an `enum` so test exhaustiveness checks
 * and runtime `code` strings are guaranteed to stay in lockstep with the kernel.
 */
export type KernelErrorCode =
  | 'InvalidArgument'
  | 'Unauthenticated'
  | 'PermissionDenied'
  | 'NotFound'
  | 'ResourceExhausted'
  | 'Unavailable'
  | 'Timeout'
  | 'Cancelled'
  | 'DataLoss'
  | 'Internal'
  | 'InvalidStatementHandle'
  | 'NetworkError'
  | 'SqlError';

/**
 * Optional metadata fields the kernel may attach via the
 * `__databricks_error__:` envelope (per `native/kernel/src/error.rs:50-89`).
 *
 * `errorCode` is namespaced under `kernelMetadata` rather than placed at
 * the top level because `OperationStateError` already declares a top-level
 * `errorCode: enum` field, and `DBSQLOperation.ts` switches on it
 * (`err.errorCode === OperationStateErrorCode.Canceled`, around `:374`). Top-level
 * defineProperty would clobber that enum with a kernel string and break
 * cancel/close detection.
 */
export interface KernelMetadata {
  errorCode?: string;
  vendorCode?: number;
  httpStatus?: number;
  retryable?: boolean;
  queryId?: string;
}

/**
 * An `Error` carrying optional SEA-side kernel context. `sqlState` is
 * exposed at the top level (no collision in the existing driver error
 * tree); the remaining envelope fields live under a `kernelMetadata`
 * namespace to avoid clobbering pre-existing `errorCode` semantics on
 * `OperationStateError`.
 */
export interface ErrorWithSqlState extends Error {
  sqlState?: string;
  kernelMetadata?: KernelMetadata;
}

/**
 * Attach a non-enumerable own-property to the error. The shape matches
 * Node's convention for attaching `.code` to system errors:
 * non-enumerable (clean `JSON.stringify`) but readable via direct
 * property access and `Object.getOwnPropertyDescriptor`. One helper for
 * both the top-level `sqlState` and the namespaced `kernelMetadata`
 * object so the `defineProperty` flags live in exactly one place.
 */
function defineErrorMetadata<K extends string, V>(error: Error, key: K, value: V): void {
  Object.defineProperty(error, key, {
    value,
    writable: true,
    enumerable: false,
    configurable: true,
  });
}

/**
 * Map a kernel error (as surfaced by the napi-binding) to the appropriate JS
 * driver error class.
 *
 * M0 mapping table:
 *   Unauthenticated, PermissionDenied → AuthenticationError
 *   Cancelled                          → OperationStateError(Canceled)
 *   Timeout                            → OperationStateError(Timeout)
 *   InvalidArgument                    → ParameterError
 *   NetworkError, Unavailable,
 *   NotFound, ResourceExhausted,
 *   DataLoss, Internal,
 *   InvalidStatementHandle, SqlError   → HiveDriverError
 *
 * Unknown `code` values (e.g. if the kernel adds a new variant) fall through
 * to HiveDriverError so the driver never silently drops an error. The kernel's
 * `ErrorCode` is `#[non_exhaustive]` so this can legitimately happen.
 *
 * SQLSTATE, when present, is attached on `error.sqlState` regardless of which
 * class is returned.
 */
export function mapKernelErrorToJsError(kErr: KernelErrorShape): ErrorWithSqlState {
  const { code, message, sqlstate } = kErr;

  let error: ErrorWithSqlState;

  switch (code as KernelErrorCode) {
    case 'Unauthenticated':
    case 'PermissionDenied':
      error = new AuthenticationError(message);
      break;

    case 'Cancelled':
      // OperationStateError with the Canceled code carries the kernel message
      // through the response.displayMessage fallback path.
      error = new OperationStateError(OperationStateErrorCode.Canceled);
      error.message = message;
      break;

    case 'Timeout':
      error = new OperationStateError(OperationStateErrorCode.Timeout);
      error.message = message;
      break;

    case 'InvalidArgument':
      error = new ParameterError(message);
      break;

    // All remaining kernel ErrorCode variants map to the base driver error class.
    // M0 intentionally does not introduce new error classes; M1 may add nuance.
    case 'NotFound':
    case 'ResourceExhausted':
    case 'Unavailable':
    case 'DataLoss':
    case 'Internal':
    case 'InvalidStatementHandle':
    case 'NetworkError':
    case 'SqlError':
      error = new HiveDriverError(message);
      break;

    default:
      // Unknown/future kernel variant — never drop the error, surface as base class.
      error = new HiveDriverError(message);
      break;
  }

  if (sqlstate !== undefined) {
    defineErrorMetadata(error, 'sqlState', sqlstate);
  }
  return error;
}

/**
 * Build a {@link KernelMetadata} object from a parsed envelope, applying
 * per-field type validation. A kernel-side bug that emits, say,
 * `retryable: "true"` (string) instead of `true` (boolean) would
 * otherwise leak the wrong-typed value through to JS callers; the
 * type-guard discards the malformed field rather than passing it through.
 */
function buildKernelMetadata(parsed: Record<string, unknown>): KernelMetadata {
  const meta: KernelMetadata = {};
  if (typeof parsed.errorCode === 'string') {
    meta.errorCode = parsed.errorCode;
  }
  if (typeof parsed.vendorCode === 'number') {
    meta.vendorCode = parsed.vendorCode;
  }
  if (typeof parsed.httpStatus === 'number') {
    meta.httpStatus = parsed.httpStatus;
  }
  if (typeof parsed.retryable === 'boolean') {
    meta.retryable = parsed.retryable;
  }
  if (typeof parsed.queryId === 'string') {
    meta.queryId = parsed.queryId;
  }
  return meta;
}

/**
 * Decode a napi-binding error into the typed JS error class.
 *
 * Two paths:
 *  - Structured kernel error: `Error.message` starts with
 *    {@link ERROR_SENTINEL} followed by a JSON envelope. We strip the
 *    sentinel, parse the JSON, route the {@link KernelErrorShape}
 *    through {@link mapKernelErrorToJsError}, and attach the remaining
 *    envelope fields under a single non-enumerable `kernelMetadata`
 *    namespace. Namespacing avoids the collision with
 *    `OperationStateError.errorCode` (an enum already switched on at the
 *    JS layer — see `DBSQLOperation.ts` around `:374`).
 *  - Binding-side error (e.g. `napi::Error::new(InvalidArg, "openSession:
 *    \`token\` is required for the requested auth mode")` produced by
 *    the binding's own validation): returned unchanged. These don't
 *    carry kernel `code` info, so we surface them as-is.
 *
 * Non-`Error` values (e.g. a `Promise.reject('string')`) pass through
 * wrapped in `HiveDriverError` so callers always see an `Error`
 * subclass.
 */
export function decodeNapiKernelError(err: unknown): Error {
  if (!(err instanceof Error)) {
    return new HiveDriverError(typeof err === 'string' ? err : 'SEA backend: unknown error');
  }

  const { message } = err;
  if (typeof message !== 'string' || !message.startsWith(ERROR_SENTINEL)) {
    return err;
  }

  const jsonStr = message.slice(ERROR_SENTINEL.length);
  let parsed: unknown;
  try {
    parsed = JSON.parse(jsonStr);
  } catch {
    // Corrupted envelope — surface the raw post-sentinel payload rather
    // than silently dropping the original error. Strip the internal
    // `__databricks_error__:` prefix; it's a binding/JS-side framing
    // marker, not user-actionable, and leaking it makes the message
    // confusing to operators triaging a malformed kernel response.
    //
    // Mutate in place when possible so the napi-binding's original
    // stack survives — that stack is the only useful triage signal on
    // a malformed-envelope path (where did a sentinel-prefixed
    // non-JSON message come from?). Fall back to a fresh
    // `HiveDriverError` only if a future napi-rs revision makes
    // `Error.message` non-writable (no such guarantee today, but the
    // descriptor contract is implementation-defined).
    try {
      err.message = jsonStr;
      return err;
    } catch {
      return new HiveDriverError(jsonStr);
    }
  }

  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    typeof (parsed as { code?: unknown }).code !== 'string' ||
    typeof (parsed as { message?: unknown }).message !== 'string'
  ) {
    return err;
  }

  const envelope = parsed as Record<string, unknown>;
  const code = envelope.code as string;
  const msg = envelope.message as string;
  const sqlState = typeof envelope.sqlState === 'string' ? envelope.sqlState : undefined;

  const jsErr = mapKernelErrorToJsError({ code, message: msg, sqlstate: sqlState });

  const meta = buildKernelMetadata(envelope);
  // Skip the namespace attachment entirely when no fields validated
  // through — keeps `err.kernelMetadata` absent rather than `{}` for
  // simple envelopes (the common case).
  if (
    meta.errorCode !== undefined ||
    meta.vendorCode !== undefined ||
    meta.httpStatus !== undefined ||
    meta.retryable !== undefined ||
    meta.queryId !== undefined
  ) {
    defineErrorMetadata(jsErr, 'kernelMetadata', meta);
  }
  return jsErr;
}
