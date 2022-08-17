import IOperation, { IFetchOptions, defaultFetchOptions } from '../contracts/IOperation';
import HiveDriver from '../hive/HiveDriver';
import {
  TGetOperationStatusResp,
  TOperationHandle,
  TTableSchema,
  TSparkDirectResults,
} from '../../thrift/TCLIService_types';
import Status from '../dto/Status';

import getResult from './getResult';
import OperationStatusHelper from './OperationStatusHelper';
import SchemaHelper from './SchemaHelper';
import FetchResultsHelper from './FetchResultsHelper';
import CompleteOperationHelper from './CompleteOperationHelper';

export default class DBSQLOperation implements IOperation {
  private driver: HiveDriver;
  private operationHandle: TOperationHandle;
  private _status: OperationStatusHelper;
  private _schema: SchemaHelper;
  private _data: FetchResultsHelper;
  private _completeOperation: CompleteOperationHelper;

  constructor(driver: HiveDriver, operationHandle: TOperationHandle, directResults?: TSparkDirectResults) {
    this.driver = driver;
    this.operationHandle = operationHandle;
    this._status = new OperationStatusHelper(this.driver, this.operationHandle, directResults?.operationStatus);
    this._schema = new SchemaHelper(this.driver, this.operationHandle, directResults?.resultSetMetadata);
    this._data = new FetchResultsHelper(this.driver, this.operationHandle, [directResults?.resultSet]);
    this._completeOperation = new CompleteOperationHelper(
      this.driver,
      this.operationHandle,
      directResults?.closeOperation,
    );
  }

  async fetchAll(options?: IFetchOptions): Promise<Array<object>> {
    let data: Array<object> = [];
    do {
      let chunk = await this.fetchChunk(options);
      if (chunk) {
        data.push(...chunk);
      }
    } while (this.hasMoreRows());
    return data;
  }

  async fetchChunk(options: IFetchOptions = defaultFetchOptions): Promise<Array<object>> {
    if (!this._status.hasResultSet) {
      return Promise.resolve([]);
    }

    await this._status.waitUntilReady(options.progress, options.callback);

    return await Promise.all([
      this._schema.fetch(),
      this._data.fetch(options.maxRows || defaultFetchOptions.maxRows),
    ]).then(([schema, data]) => {
      const result = getResult(schema, data ? [data] : []);
      return Promise.resolve(result);
    });
  }

  /**
   * Requests operation status
   * @param progress
   * @throws {StatusError}
   */
  async status(progress: boolean = false): Promise<TGetOperationStatusResp> {
    return this._status.status(progress);
  }

  /**
   * Cancels operation
   * @throws {StatusError}
   */
  cancel(): Promise<Status> {
    return this._completeOperation.cancel();
  }

  /**
   * Closes operation
   * @throws {StatusError}
   */
  close(): Promise<Status> {
    return this._completeOperation.close();
  }

  async finished(): Promise<boolean> {
    await this._status.waitUntilReady();
    return true;
  }

  hasMoreRows(): boolean {
    if (this._completeOperation.closed || this._completeOperation.cancelled) {
      return false;
    }
    return this._data.hasMoreRows;
  }

  async getSchema(): Promise<TTableSchema | null> {
    if (this._status.hasResultSet) {
      return this._schema.fetch();
    }
    return null;
  }
}
