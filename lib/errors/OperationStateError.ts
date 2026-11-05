import HiveDriverError from './HiveDriverError';
import { TGetOperationStatusResp } from '../../thrift/TCLIService_types';

export enum OperationStateErrorCode {
  Canceled = 'CANCELED',
  Closed = 'CLOSED',
  Error = 'ERROR',
  Timeout = 'TIMEOUT',
  Unknown = 'UNKNOWN',
}

const errorMessages: Record<OperationStateErrorCode, string> = {
  [OperationStateErrorCode.Canceled]: 'The operation was canceled by a client',
  [OperationStateErrorCode.Closed]: 'The operation was closed by a client',
  [OperationStateErrorCode.Error]: 'The operation failed due to an error',
  [OperationStateErrorCode.Timeout]: 'The operation is in a timed out state',
  [OperationStateErrorCode.Unknown]: 'The operation is in an unrecognized state',
};

export default class OperationStateError extends HiveDriverError {
  public errorCode: OperationStateErrorCode;

  public response?: TGetOperationStatusResp;

  // SQLSTATE reported by the server for the failed operation (if any)
  public sqlState?: string;

  constructor(errorCode: OperationStateErrorCode, response?: TGetOperationStatusResp) {
    super(response?.displayMessage ?? errorMessages[errorCode]);

    this.errorCode = errorCode;
    this.response = response;

    // Only the top-level field describes the failed operation: `status` is
    // always a successful GetOperationStatus RPC status by the time this is thrown.
    const sqlState = response?.sqlState;
    if (sqlState) {
      // Non-enumerable, matching how the kernel backend attaches `sqlState`
      // (see `defineErrorMetadata` in lib/kernel/KernelErrorMapping.ts), so the
      // same error class looks identical regardless of backend.
      Object.defineProperty(this, 'sqlState', {
        value: sqlState,
        writable: true,
        enumerable: false,
        configurable: true,
      });
    }
  }
}
