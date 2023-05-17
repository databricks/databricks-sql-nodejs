import OperationError, { OperationErrorOptions } from './OperationError';
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

export interface OperationStateErrorOptions extends OperationErrorOptions {
  response?: TGetOperationStatusResp;
}

export default class OperationStateError extends OperationError {
  public errorCode: OperationStateErrorCode;

  public response?: TGetOperationStatusResp;

  constructor(errorCode: OperationStateErrorCode, options: OperationStateErrorOptions = {}) {
    super(errorMessages[errorCode], options);

    this.errorCode = errorCode;
    this.response = options.response;
  }
}
