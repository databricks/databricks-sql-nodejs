import IOperation from './contracts/IOperation';
import HiveDriver from './hive/HiveDriver';
import {
  TOperationState,
  TStatusCode,
  TFetchOrientation,
  TFetchResultsResp,
  TColumn,
  TTableSchema,
  TRowSet,
  TOperationHandle,
  TRow,
} from '../thrift/TCLIService_types';
import { ColumnCode, Int64 } from './hive/Types';
import Status from './dto/Status';
import StatusFactory from './factory/StatusFactory';
import { definedOrError } from './utils';
import WaitUntilReady from './utils/WaitUntilReady';
import { parse } from 'path';

export default class DBSQLOperation implements IOperation {
  private driver: HiveDriver;
  private operationHandle: TOperationHandle;
  private schema: TTableSchema | null;
  private data: Array<TRowSet>;
  private statusFactory: StatusFactory;

  private maxRows: Int64 = new Int64(100000);
  private fetchType: number = 0;

  private _hasMoreRows: boolean = false;
  private state: number;
  private hasResultSet: boolean = false;

  constructor(driver: HiveDriver, operationHandle: TOperationHandle) {
    this.driver = driver;
    this.operationHandle = operationHandle;
    this.hasResultSet = operationHandle.hasResultSet;
    this.statusFactory = new StatusFactory();
    this.state = TOperationState.INITIALIZED_STATE;

    this.schema = null;
    this.data = [];
  }

  /**
   * Fetches result and schema from operation
   * @throws {StatusError}
   */
  fetch(maxRowSize = this.maxRows): Promise<Status> {
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

    if (this.schema === null) {
      return this.initializeSchema()
        .then((schema) => {
          this.schema = schema;

          return this.firstFetch();
        })
        .then((response) => this.processFetchResponse(response));
    } else {
      return this.nextFetch(maxRowSize).then((response) => this.processFetchResponse(response));
    }
  }

  async fetchAll(): Promise<TRowSet[] | null> {
    return this.fetchChunk(new Int64(Number.MAX_SAFE_INTEGER));
  }

  async fetchChunk(chunkSize: Int64): Promise<TRowSet[] | null>  {
    let rowsWritten = new Int64(0);
    let resultSet = this.data;
    if (!this.hasResultSet) {
      return Promise.resolve(
        null,
      );
    }

    await new WaitUntilReady(this).execute();

    let fetchSize = new Int64(Math.min(Number(this.maxRows), Number(chunkSize) - Number(rowsWritten)));
    
    // Need initial fetch to populate _hasMoreRows
    //
    await this.fetch(fetchSize).then(() => {
      while(this.hasMoreRows()) {
        rowsWritten = new Int64(Number(rowsWritten) + Number(fetchSize));
        if(rowsWritten >= chunkSize) {
          this.flush();
          return Promise.resolve(
            resultSet,);
        }
        fetchSize = new Int64(Math.min(Number(this.maxRows), Number(chunkSize) - Number(rowsWritten)));
        this.fetch(fetchSize);
      }
      this.flush();
      return Promise.resolve(
        resultSet,);
      });
    return null;
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

  setMaxRows(maxRows: number): void {
    this.maxRows = new Int64(maxRows);
  }

  setFetchType(fetchType: number): void {
    this.fetchType = fetchType;
  }

  getSchema() {
    return this.schema;
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

  /**
   * Retrieves schema
   * @throws {StatusError}
   */
  private initializeSchema(): Promise<TTableSchema> {
    return this.driver
      .getResultSetMetadata({
        operationHandle: this.operationHandle,
      })
      .then((schema) => {
        this.statusFactory.create(schema.status);

        return definedOrError(schema.schema);
      });
  }

  private firstFetch() {
    return this.driver.fetchResults({
      operationHandle: this.operationHandle,
      orientation: TFetchOrientation.FETCH_FIRST,
      maxRows: this.maxRows,
      fetchType: this.fetchType,
    });
  }

  private nextFetch(maxRowSize = this.maxRows) {
    return this.driver.fetchResults({
      operationHandle: this.operationHandle,
      orientation: TFetchOrientation.FETCH_NEXT,
      maxRows: maxRowSize,
      fetchType: this.fetchType,
    });
  }

  /**
   * @param response
   * @throws {StatusError}
   */
  private processFetchResponse(response: TFetchResultsResp): Status {
    const status = this.statusFactory.create(response.status);

    this._hasMoreRows = this.checkIfOperationHasMoreRows(response);

    if (response.results) {
      this.data.push(response.results);
    }

    return status;
  }

  private checkIfOperationHasMoreRows(response: TFetchResultsResp): boolean {
    if (response.hasMoreRows) {
      return true;
    }

    const columns = response.results?.columns || [];

    if (!columns.length) {
      return false;
    }

    const column: TColumn = columns[0];

    const columnValue =
      column[ColumnCode.binaryVal] ||
      column[ColumnCode.boolVal] ||
      column[ColumnCode.byteVal] ||
      column[ColumnCode.doubleVal] ||
      column[ColumnCode.i16Val] ||
      column[ColumnCode.i32Val] ||
      column[ColumnCode.i64Val] ||
      column[ColumnCode.stringVal];

    return (columnValue?.values?.length || 0) > 0;
  }
}
