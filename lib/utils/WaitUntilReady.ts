import IOperation from '../contracts/IOperation';
import { TOperationState, TGetOperationStatusResp } from '../../thrift/TCLIService_types';
import OperationStateError from '../errors/OperationStateError';

export default class WaitUntilReady {
  private operation: IOperation;

  constructor(operation: IOperation) {
    this.operation = operation;
  }

  /**
   * Executes until operation has status finished or has one of the invalid states
   *
   * @param progress flag for operation status command. If it sets true, response will include progressUpdateResponse with progress information
   * @param callback if callback specified it will be called each time the operation status response received and it will be passed as first parameter
   */
  async execute(progress?: boolean, callback?: Function): Promise<IOperation> {
    const response = await this.operation.status(Boolean(progress));

    if (typeof callback === 'function') {
      await this.executeCallback(callback.bind(null, response));
    }

    try {
      const isReady = this.isReady(response);

      if (isReady) {
        return this.operation;
      } else {
        return this.execute(progress, callback);
      }
    } catch (error) {
      throw error;
    }
  }

  private isReady(response: TGetOperationStatusResp): boolean {
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

  private executeCallback(callback: Function): Promise<any> {
    const result = callback();

    if (result instanceof Promise) {
      return result;
    } else {
      return Promise.resolve(result);
    }
  }
}
