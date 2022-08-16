import IOperation, { IFetchOptions, defaultFetchOptions } from '../contracts/IOperation';
import HiveDriver from '../hive/HiveDriver';
import {
  TOperationState,
  TStatusCode,
  TFetchOrientation,
  TFetchResultsResp,
  TTableSchema,
  TRowSet,
  TOperationHandle,
} from '../../thrift/TCLIService_types';
import { Int64, FetchType } from '../hive/Types';
import Status from '../dto/Status';
import StatusFactory from '../factory/StatusFactory';
import { definedOrError } from '../utils';

import waitUntilReady from './waitUntilReady';
import checkIfOperationHasMoreRows from './checkIfOperationHasMoreRows';
import getResult from './getResult';
import SchemaFetchingHelper from './SchemaFetchingHelper';

export default class DBSQLOperation implements IOperation {
  private driver: HiveDriver;
  private operationHandle: TOperationHandle;
  private data: Array<TRowSet> = [];
  private statusFactory = new StatusFactory();
  private schema: SchemaFetchingHelper;

  private fetchStarted = false;

  private _hasMoreRows: boolean = false;
  private state: number = TOperationState.INITIALIZED_STATE;
  private hasResultSet: boolean = false;

  constructor(driver: HiveDriver, operationHandle: TOperationHandle) {
    this.driver = driver;
    this.operationHandle = operationHandle;
    this.hasResultSet = operationHandle.hasResultSet;
    this.schema = new SchemaFetchingHelper(this.driver, this.operationHandle);
  }

  /**
   * Fetches result and schema from operation
   * @throws {StatusError}
   */
  fetch(chunkSize = 100000): Promise<Status> {
    if (!this.hasResultSet) {
      return Promise.resolve(
        this.statusFactory.create({
          statusCode: TStatusCode.SUCCESS_STATUS,
        }),
      );
    }

    if (!this.finished()) {
      return Promise.resolve(
        this.statusFactory.create({
          statusCode: TStatusCode.STILL_EXECUTING_STATUS,
        }),
      );
    }

    if (!this.fetchStarted) {
      return this.firstFetch(chunkSize).then((response) => this.processFetchResponse(response));
    } else {
      return this.nextFetch(chunkSize).then((response) => this.processFetchResponse(response));
    }
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

    return await Promise.all([this.schema.fetch(), this.fetch(options.maxRows)]).then(([schema]) => {
      let data = getResult(schema, this.getData());
      this.flush();
      return Promise.resolve(data);
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
    return this._hasMoreRows;
  }

  getSchema() {
    return this.schema.fetch();
  }

  getData() {
    return this.data;
  }

  /**
   * Resets `this.data` buffer.
   * Needs to be called when working with massive data.
   */
  flush(): void {
    this.data = [];
  }

  private firstFetch(chunkSize: number) {
    return this.driver.fetchResults({
      operationHandle: this.operationHandle,
      orientation: TFetchOrientation.FETCH_FIRST,
      maxRows: new Int64(chunkSize),
      fetchType: FetchType.Data,
    });
  }

  private nextFetch(chunkSize: number) {
    return this.driver.fetchResults({
      operationHandle: this.operationHandle,
      orientation: TFetchOrientation.FETCH_NEXT,
      maxRows: new Int64(chunkSize),
      fetchType: FetchType.Data,
    });
  }

  /**
   * @param response
   * @throws {StatusError}
   */
  private processFetchResponse(response: TFetchResultsResp): Status {
    this.fetchStarted = true;

    const status = this.statusFactory.create(response.status);

    this._hasMoreRows = checkIfOperationHasMoreRows(response);

    if (response.results) {
      this.data.push(response.results);
    }

    return status;
  }
}
