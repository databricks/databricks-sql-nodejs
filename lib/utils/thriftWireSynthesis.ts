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

function synthesizeOkStatus(): TStatus {
  return { statusCode: TStatusCode.SUCCESS_STATUS } as TStatus;
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
      return TSparkRowSetType.COLUMN_BASED_SET;
  }
}

/**
 * Synthesize a Thrift `TGetOperationStatusResp` from the neutral
 * `OperationStatus` DTO. Used by `DBSQLOperation.status()` when running
 * against a non-Thrift backend (e.g. SEA) so the public API stays Thrift-shaped.
 *
 * Lossy by design: Thrift-only fields not carried by `OperationStatus`
 * (`taskStatus`, `numModifiedRows`, `operationStarted`, `operationCompleted`,
 * `displayMessage`, `diagnosticInfo`) are left undefined. Consumers that
 * read those fields will see `undefined` on non-Thrift backends.
 */
export function synthesizeThriftStatus(status: OperationStatus): TGetOperationStatusResp {
  return {
    status: synthesizeOkStatus(),
    operationState: operationStateToThrift(status.state),
    sqlState: status.sqlState,
    errorMessage: status.errorMessage,
    hasResultSet: status.hasResultSet,
    progressUpdateResponse: status.progressUpdateResponse as TGetOperationStatusResp['progressUpdateResponse'],
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
