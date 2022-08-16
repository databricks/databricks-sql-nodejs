import IOperation, { IFetchOptions, defaultFetchOptions } from '../contracts/IOperation';
import HiveDriver from '../hive/HiveDriver';
import { TOperationState, TOperationHandle } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';
import StatusFactory from '../factory/StatusFactory';

import waitUntilReady from './waitUntilReady';
import getResult from './getResult';
import SchemaFetchingHelper from './SchemaFetchingHelper';
import DataFetchingHelper from './DataFetchingHelper';

export default class DBSQLOperation implements IOperation {
  private driver: HiveDriver;
  private operationHandle: TOperationHandle;
  private statusFactory = new StatusFactory();
  private schema: SchemaFetchingHelper;
  private data: DataFetchingHelper;

  private state: number = TOperationState.INITIALIZED_STATE;
  private hasResultSet: boolean = false;

  constructor(driver: HiveDriver, operationHandle: TOperationHandle) {
    this.driver = driver;
    this.operationHandle = operationHandle;
    this.hasResultSet = operationHandle.hasResultSet;
    this.schema = new SchemaFetchingHelper(this.driver, this.operationHandle);
    this.data = new DataFetchingHelper(this.driver, this.operationHandle);
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
    if (!this.hasResultSet) {
      return Promise.resolve([]);
    }

    await waitUntilReady(this, options.progress, options.callback);

    return await Promise.all([
      this.schema.fetch(),
      this.data.fetch(options.maxRows || defaultFetchOptions.maxRows),
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
  status(progress: boolean = false) {
    return this.driver
      .getOperationStatus({
        operationHandle: this.operationHandle,
        getProgressUpdate: progress,
      })
      .then((response) => {
        this.statusFactory.create(response.status);

        this.state = response.operationState ?? this.state;

        if (typeof response.hasResultSet === 'boolean') {
          this.hasResultSet = response.hasResultSet;
        }

        return response;
      });
  }

  /**
   * Cancels operation
   * @throws {StatusError}
   */
  cancel(): Promise<Status> {
    return this.driver
      .cancelOperation({
        operationHandle: this.operationHandle,
      })
      .then((response) => {
        return this.statusFactory.create(response.status);
      });
  }

  /**
   * Closes operation
   * @throws {StatusError}
   */
  close(): Promise<Status> {
    return this.driver
      .closeOperation({
        operationHandle: this.operationHandle,
      })
      .then((response) => {
        return this.statusFactory.create(response.status);
      });
  }

  finished(): boolean {
    return this.state === TOperationState.FINISHED_STATE;
  }

  hasMoreRows(): boolean {
    return this.data.hasMoreRows;
  }

  getSchema() {
    return this.schema.fetch();
  }
}
