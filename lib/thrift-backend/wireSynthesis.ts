import Int64 from 'node-int64';
import {
  TGetOperationStatusResp,
  TGetResultSetMetadataResp,
  TOperationState,
  TSparkRowSetType,
  TStatus,
  TStatusCode,
} from '../../thrift/TCLIService_types';
import { OperationState, OperationStatus } from '../contracts/OperationStatus';
import { ResultFormat, ResultMetadata } from '../contracts/ResultMetadata';
import HiveDriverError from '../errors/HiveDriverError';

function synthesizeOkStatus(): TStatus {
  return { statusCode: TStatusCode.SUCCESS_STATUS } as TStatus;
}

/**
 * Map a neutral `OperationStatus` to the Thrift `TStatus` shape consumed by
 * `Status.assert` and other legacy callers. Terminal failure / cancellation
 * states surface as `ERROR_STATUS` so existing user code that branches on
 * `resp.status.statusCode` continues to detect failure on non-Thrift backends;
 * progress / success states pass through as `SUCCESS_STATUS`.
 */
function synthesizeStatusFromOperation(status: OperationStatus): TStatus {
  switch (status.state) {
    case OperationState.Failed:
    case OperationState.Cancelled:
    case OperationState.Closed:
      return {
        statusCode: TStatusCode.ERROR_STATUS,
        errorMessage: status.errorMessage,
        sqlState: status.sqlState,
      } as TStatus;
    default:
      return synthesizeOkStatus();
  }
}

function operationStateToThrift(state: OperationState): TOperationState {
  switch (state) {
    case OperationState.Pending:
      return TOperationState.PENDING_STATE;
    case OperationState.Running:
      return TOperationState.RUNNING_STATE;
    case OperationState.Succeeded:
      return TOperationState.FINISHED_STATE;
    case OperationState.Cancelled:
      return TOperationState.CANCELED_STATE;
    case OperationState.Closed:
      return TOperationState.CLOSED_STATE;
    case OperationState.Failed:
      return TOperationState.ERROR_STATE;
    case OperationState.Unknown:
    default:
      return TOperationState.UKNOWN_STATE;
  }
}

function resultFormatToThrift(format: ResultFormat): TSparkRowSetType {
  switch (format) {
    case ResultFormat.ColumnBased:
      return TSparkRowSetType.COLUMN_BASED_SET;
    case ResultFormat.ArrowBased:
      return TSparkRowSetType.ARROW_BASED_SET;
    case ResultFormat.UrlBased:
      return TSparkRowSetType.URL_BASED_SET;
    default:
      // Aliasing an unknown format to COLUMN_BASED_SET would silently route
      // results through JsonResultHandler and surface garbled rows; refuse
      // instead so a new ResultFormat member added later trips loudly.
      throw new HiveDriverError(`Unknown ResultFormat: ${format as string}`);
  }
}

/**
 * Synthesize a Thrift `TGetOperationStatusResp` from the neutral
 * `OperationStatus` DTO. Used by `DBSQLOperation.status()` when running
 * against a non-Thrift backend (e.g. kernel) so the public API stays Thrift-shaped.
 *
 * Carries the rich status fields when the backend supplies them
 * (`numModifiedRows`, `displayMessage`, `diagnosticInfo`, `errorDetailsJson`)
 * — the kernel backend reads these off the terminal kernel statement, so DML
 * operations report `numModifiedRows` at parity with the Thrift path.
 * `numModifiedRows` is re-boxed as a Thrift `Int64` (`node-int64`) to match the
 * wire shape the Thrift deserializer produces, so consumers can read it
 * uniformly across backends.
 *
 * Still lossy for Thrift-only fields not carried by `OperationStatus`
 * (`taskStatus`, `operationStarted`, `operationCompleted`), which are left
 * undefined.
 */
export function synthesizeThriftStatus(status: OperationStatus): TGetOperationStatusResp {
  return {
    status: synthesizeStatusFromOperation(status),
    operationState: operationStateToThrift(status.state),
    sqlState: status.sqlState,
    errorMessage: status.errorMessage,
    hasResultSet: status.hasResultSet,
    progressUpdateResponse: status.progressUpdateResponse as TGetOperationStatusResp['progressUpdateResponse'],
    // Rich status fields: only present on backends that surface them (kernel on a
    // terminal sync statement). `null` (server didn't supply) maps to
    // `undefined` so the synthesized response matches the Thrift path, where an
    // absent field is simply not set.
    numModifiedRows:
      status.numModifiedRows === undefined || status.numModifiedRows === null
        ? undefined
        : new Int64(status.numModifiedRows),
    displayMessage: status.displayMessage ?? undefined,
    diagnosticInfo: status.diagnosticInfo ?? undefined,
    errorDetailsJson: status.errorDetailsJson ?? undefined,
  } as TGetOperationStatusResp;
}

/**
 * Synthesize a Thrift `TGetResultSetMetadataResp` from the neutral
 * `ResultMetadata` DTO. Used by `DBSQLOperation.getMetadata()` when running
 * against a non-Thrift backend.
 *
 * Lossy: `cacheLookupResult`, `uncompressedBytes`, `compressedBytes` are left
 * undefined; `status` is set to a synthetic OK.
 */
export function synthesizeThriftResultSetMetadata(metadata: ResultMetadata): TGetResultSetMetadataResp {
  return {
    status: synthesizeOkStatus(),
    schema: metadata.schema,
    resultFormat: resultFormatToThrift(metadata.resultFormat),
    lz4Compressed: metadata.lz4Compressed,
    arrowSchema: metadata.arrowSchema,
    isStagingOperation: metadata.isStagingOperation,
  } as TGetResultSetMetadataResp;
}
