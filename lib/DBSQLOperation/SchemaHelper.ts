import { TGetResultSetMetadataResp, TOperationHandle, TSparkRowSetType } from '../../thrift/TCLIService_types';
import HiveDriver from '../hive/HiveDriver';
import Status from '../dto/Status';
import IOperationResult from '../result/IOperationResult';
import JsonResult from '../result/JsonResult';
import ArrowResult from '../result/ArrowResult';
import HiveDriverError from '../errors/HiveDriverError';
import { definedOrError } from '../utils';

export default class SchemaHelper {
  private driver: HiveDriver;

  private operationHandle: TOperationHandle;

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
      Status.assert(metadata.status);
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
    const resultFormat = definedOrError(metadata.resultFormat);

    switch (resultFormat) {
      case TSparkRowSetType.COLUMN_BASED_SET:
        return new JsonResult(metadata.schema);
      case TSparkRowSetType.ARROW_BASED_SET:
        return new ArrowResult(metadata.schema, metadata.arrowSchema);
      default:
        throw new HiveDriverError(`Unsupported result format: ${TSparkRowSetType[resultFormat]}`);
    }
  }
}
