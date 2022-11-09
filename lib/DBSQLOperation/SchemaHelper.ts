import { TOperationHandle, TGetResultSetMetadataResp, TSparkRowSetType } from '../../thrift/TCLIService_types';
import HiveDriver from '../hive/HiveDriver';
import StatusFactory from '../factory/StatusFactory';
import IOperationResult from '../result/IOperationResult';
import JsonResult from '../result/JsonResult';
import HiveDriverError from '../errors/HiveDriverError';
import { definedOrError } from '../utils';

export default class SchemaHelper {
  private driver: HiveDriver;

  private operationHandle: TOperationHandle;

  private statusFactory = new StatusFactory();

  private metadata?: TGetResultSetMetadataResp;

  constructor(driver: HiveDriver, operationHandle: TOperationHandle, metadata?: TGetResultSetMetadataResp) {
    this.driver = driver;
    this.operationHandle = operationHandle;
    this.metadata = metadata;
  }

  private async fetchMetadata() {
    if (!this.metadata) {
      const metadata = await this.driver.getResultSetMetadata({
        operationHandle: this.operationHandle,
      });
      this.statusFactory.create(metadata.status);
      this.metadata = metadata;
    }

    return this.metadata;
  }

  async fetch() {
    const metadata = await this.fetchMetadata();
    return definedOrError(metadata.schema);
  }

  async getResultHandler(): Promise<IOperationResult> {
    const metadata = await this.fetchMetadata();
    const schema = definedOrError(metadata.schema);
    const resultFormat = definedOrError(metadata.resultFormat);

    switch (resultFormat) {
      case TSparkRowSetType.COLUMN_BASED_SET:
        return new JsonResult(schema);
      default:
        throw new HiveDriverError(`Unsupported result format: ${TSparkRowSetType[resultFormat]}`);
    }
  }
}
