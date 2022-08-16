import { TOperationHandle, TGetResultSetMetadataResp } from '../../thrift/TCLIService_types';
import HiveDriver from '../hive/HiveDriver';
import StatusFactory from '../factory/StatusFactory';
import { definedOrError } from '../utils';

export default class SchemaFetchingHelper {
  private driver: HiveDriver;
  private operationHandle: TOperationHandle;
  private statusFactory = new StatusFactory();
  private metadata: Promise<TGetResultSetMetadataResp> | null = null;

  constructor(driver: HiveDriver, operationHandle: TOperationHandle) {
    this.driver = driver;
    this.operationHandle = operationHandle;
  }

  async fetch() {
    if (!this.metadata) {
      this.metadata = this.driver.getResultSetMetadata({
        operationHandle: this.operationHandle,
      });
    }

    return this.metadata.then((metadata) => {
      this.statusFactory.create(metadata.status);
      return definedOrError(metadata.schema);
    });
  }
}
