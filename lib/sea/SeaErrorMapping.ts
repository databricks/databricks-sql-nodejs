import HiveDriverError from '../errors/HiveDriverError';
import AuthenticationError from '../errors/AuthenticationError';
import OperationStateError, { OperationStateErrorCode } from '../errors/OperationStateError';
import ParameterError from '../errors/ParameterError';

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
 * An `Error` with a preserved SQLSTATE on the `sqlState` property. Used as the
 * narrowed return type of {@link mapKernelErrorToJsError} so callers that need
 * the SQLSTATE can `error.sqlState` without an `any` cast.
 */
export interface ErrorWithSqlState extends Error {
  sqlState?: string;
}

/**
 * Attach the kernel's SQLSTATE to the JS error object via the `sqlState` property.
 * The driver has no pre-existing `sqlState` convention (no other error class
 * sets it today) so this single helper defines it for the SEA path.
 */
function attachSqlState(error: ErrorWithSqlState, sqlstate?: string): ErrorWithSqlState {
  if (sqlstate !== undefined) {
    // Using Object.defineProperty so the property is non-enumerable but still
    // visible via direct access — matches the way Node attaches `.code` to system errors.
    Object.defineProperty(error, 'sqlState', {
      value: sqlstate,
      writable: true,
      enumerable: false,
      configurable: true,
    });
  }
  return error;
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

  return attachSqlState(error, sqlstate);
}
