import { TOperationHandle, TStatusCode } from '../../thrift/TCLIService_types';
import HiveDriver from '../hive/HiveDriver';
import StatusFactory from '../factory/StatusFactory';
import Status from '../dto/Status';

export default class CompleteOperationHelper {
  private driver: HiveDriver;
  private operationHandle: TOperationHandle;
  private statusFactory = new StatusFactory();

  closed: boolean = false;
  cancelled: boolean = false;

  constructor(driver: HiveDriver, operationHandle: TOperationHandle) {
    this.driver = driver;
    this.operationHandle = operationHandle;
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
