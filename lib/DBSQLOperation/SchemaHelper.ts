import { TGetResultSetMetadataResp, TOperationHandle, TSparkRowSetType } from '../../thrift/TCLIService_types';
import HiveDriver from '../hive/HiveDriver';
import Status from '../dto/Status';
import IOperationResult from '../result/IOperationResult';
import JsonResult from '../result/JsonResult';
import ArrowResult from '../result/ArrowResult';
import CloudFetchResult from '../result/CloudFetchResult';
import HiveDriverError from '../errors/HiveDriverError';
import { definedOrError } from '../utils';

export default class SchemaHelper {
  private readonly driver: HiveDriver;

  private readonly operationHandle: TOperationHandle;

  private metadata?: TGetResultSetMetadataResp;

  private resultHandler?: IOperationResult;

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

  public async fetch() {
    const metadata = await this.fetchMetadata();
    return definedOrError(metadata.schema);
  }

  public async getResultHandler(): Promise<IOperationResult> {
    const metadata = await this.fetchMetadata();
    const resultFormat = definedOrError(metadata.resultFormat);

    if (!this.resultHandler) {
      switch (resultFormat) {
        case TSparkRowSetType.COLUMN_BASED_SET:
          this.resultHandler = new JsonResult(metadata.schema);
          break;
        case TSparkRowSetType.ARROW_BASED_SET:
          this.resultHandler = new ArrowResult(metadata.schema, metadata.arrowSchema);
          break;
        case TSparkRowSetType.URL_BASED_SET:
          this.resultHandler = new CloudFetchResult(metadata.schema);
          break;
        default:
          this.resultHandler = undefined;
          break;
      }
    }

    if (!this.resultHandler) {
      throw new HiveDriverError(`Unsupported result format: ${TSparkRowSetType[resultFormat]}`);
    }

    return this.resultHandler;
  }
}
