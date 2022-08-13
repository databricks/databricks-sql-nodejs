import IOperation, { OperationStatusCallback } from '../contracts/IOperation';
import { TOperationState } from '../../thrift/TCLIService_types';
import OperationStateError from '../errors/OperationStateError';

async function isReady(
  operation: IOperation,
  progress?: boolean,
  callback?: OperationStatusCallback,
): Promise<boolean> {
  let response = await operation.status(Boolean(progress));

  if (callback) {
    await Promise.resolve(callback(response));
  }

  switch (response.operationState) {
    case TOperationState.INITIALIZED_STATE:
      return false;
    case TOperationState.RUNNING_STATE:
      return false;
    case TOperationState.FINISHED_STATE:
      return true;
    case TOperationState.CANCELED_STATE:
      throw new OperationStateError('The operation was canceled by a client', response);
    case TOperationState.CLOSED_STATE:
      throw new OperationStateError('The operation was closed by a client', response);
    case TOperationState.ERROR_STATE:
      throw new OperationStateError('The operation failed due to an error', response);
    case TOperationState.PENDING_STATE:
      throw new OperationStateError('The operation is in a pending state', response);
    case TOperationState.TIMEDOUT_STATE:
      throw new OperationStateError('The operation is in a timedout state', response);
    case TOperationState.UKNOWN_STATE:
    default:
      throw new OperationStateError('The operation is in an unrecognized state', response);
  }
}

export default async function waitUntilReady(
  operation: IOperation,
  progress?: boolean,
  callback?: OperationStatusCallback,
): Promise<void> {
  if (operation.finished()) {
    return;
  }
  if (await isReady(operation, progress, callback)) {
    return;
  } else {
    return waitUntilReady(operation, progress, callback);
  }
}
