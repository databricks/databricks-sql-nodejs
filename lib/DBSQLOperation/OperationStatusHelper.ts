import { TOperationHandle, TOperationState, TGetOperationStatusResp } from '../../thrift/TCLIService_types';
import HiveDriver from '../hive/HiveDriver';
import StatusFactory from '../factory/StatusFactory';
import { OperationStatusCallback } from '../contracts/IOperation';
import OperationStateError from '../errors/OperationStateError';

export default class OperationStatusHelper {
  private driver: HiveDriver;

  private operationHandle: TOperationHandle;

  private statusFactory = new StatusFactory();

  private state: number = TOperationState.INITIALIZED_STATE;

  hasResultSet: boolean = false;

  constructor(driver: HiveDriver, operationHandle: TOperationHandle, operationStatus?: TGetOperationStatusResp) {
    this.driver = driver;
    this.operationHandle = operationHandle;
    this.hasResultSet = operationHandle.hasResultSet;

    if (operationStatus) {
      this.processOperationStatusResponse(operationStatus);
    }
  }

  private processOperationStatusResponse(response: TGetOperationStatusResp) {
    this.statusFactory.create(response.status);

    this.state = response.operationState ?? this.state;

    if (typeof response.hasResultSet === 'boolean') {
      this.hasResultSet = response.hasResultSet;
    }

    return response;
  }

  status(progress: boolean = false) {
    return this.driver
      .getOperationStatus({
        operationHandle: this.operationHandle,
        getProgressUpdate: progress,
      })
      .then((response) => this.processOperationStatusResponse(response));
  }

  private async isReady(progress?: boolean, callback?: OperationStatusCallback): Promise<boolean> {
    const response = await this.status(Boolean(progress));

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

  async waitUntilReady(progress?: boolean, callback?: OperationStatusCallback): Promise<void> {
    if (this.state === TOperationState.FINISHED_STATE) {
      return;
    }
    const isReady = await this.isReady(progress, callback);
    if (!isReady) {
      return this.waitUntilReady(progress, callback);
    }
  }
}
