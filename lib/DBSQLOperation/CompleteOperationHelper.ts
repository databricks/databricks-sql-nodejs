import { TOperationHandle, TStatusCode, TCloseOperationResp } from '../../thrift/TCLIService_types';
import HiveDriver from '../hive/HiveDriver';
import StatusFactory from '../factory/StatusFactory';
import Status from '../dto/Status';

export default class CompleteOperationHelper {
  private readonly driver: HiveDriver;

  private readonly operationHandle: TOperationHandle;

  private readonly statusFactory = new StatusFactory();

  public closed: boolean = false;

  public cancelled: boolean = false;

  constructor(driver: HiveDriver, operationHandle: TOperationHandle, closeOperation?: TCloseOperationResp) {
    this.driver = driver;
    this.operationHandle = operationHandle;

    if (closeOperation) {
      this.statusFactory.create(closeOperation.status);
      this.closed = true;
    }
  }

  public async cancel(): Promise<Status> {
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

  public async close(): Promise<Status> {
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
