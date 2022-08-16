import { TOperationHandle, TGetResultSetMetadataResp } from '../../thrift/TCLIService_types';
import HiveDriver from '../hive/HiveDriver';
import StatusFactory from '../factory/StatusFactory';
import { definedOrError } from '../utils';

export default class SchemaHelper {
  private driver: HiveDriver;
  private operationHandle: TOperationHandle;
  private statusFactory = new StatusFactory();
  private metadata: TGetResultSetMetadataResp | null = null;

  constructor(driver: HiveDriver, operationHandle: TOperationHandle) {
    this.driver = driver;
    this.operationHandle = operationHandle;
  }

  async fetch() {
    if (!this.metadata) {
      const metadata = await this.driver.getResultSetMetadata({
        operationHandle: this.operationHandle,
      });
      this.statusFactory.create(metadata.status);
      this.metadata = metadata;
    }

    return definedOrError(this.metadata.schema);
  }
}
