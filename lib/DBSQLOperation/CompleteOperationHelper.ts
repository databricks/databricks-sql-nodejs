import { TOperationHandle, TStatusCode, TCloseOperationResp } from '../../thrift/TCLIService_types';
import HiveDriver from '../hive/HiveDriver';
import StatusFactory from '../factory/StatusFactory';
import Status from '../dto/Status';

import RestDriver from '../rest/RestDriver';

export default class CompleteOperationHelper {
  private driver: RestDriver;

  private operationHandle: TOperationHandle;

  private statusFactory = new StatusFactory();

  closed: boolean = false;

  cancelled: boolean = false;

  constructor(driver: RestDriver, operationHandle: TOperationHandle, closeOperation?: TCloseOperationResp) {
    this.driver = driver;
    this.operationHandle = operationHandle;

    if (closeOperation) {
      this.statusFactory.create(closeOperation.status);
      this.closed = true;
    }
  }

  async cancel(): Promise<Status> {
    if (this.cancelled) {
      return this.statusFactory.create({
        statusCode: TStatusCode.SUCCESS_STATUS,
      });
    }

    const response = await this.driver.cancelOperation({
      operationHandle: this.operationHandle,
    });
    const status = this.statusFactory.create(response.status);
    this.cancelled = true;
    return status;
  }

  async close(): Promise<Status> {
    if (this.closed) {
      return this.statusFactory.create({
        statusCode: TStatusCode.SUCCESS_STATUS,
      });
    }

    const response = await this.driver.closeOperation({
      operationHandle: this.operationHandle,
    });
    const status = this.statusFactory.create(response.status);
    this.closed = true;
    return status;
  }
}
