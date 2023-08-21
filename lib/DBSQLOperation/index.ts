import { stringify, NIL, parse } from 'uuid';
import IOperation, {
  FetchOptions,
  FinishedOptions,
  GetSchemaOptions,
  WaitUntilReadyOptions,
} from '../contracts/IOperation';
import HiveDriver from '../hive/HiveDriver';
import {
  TGetOperationStatusResp,
  TOperationHandle,
  TTableSchema,
  TSparkDirectResults,
} from '../../thrift/TCLIService_types';
import Status from '../dto/Status';
import OperationStatusHelper from './OperationStatusHelper';
import SchemaHelper from './SchemaHelper';
import FetchResultsHelper from './FetchResultsHelper';
import IDBSQLLogger, { LogLevel } from '../contracts/IDBSQLLogger';
import OperationStateError, { OperationStateErrorCode } from '../errors/OperationStateError';

const defaultMaxRows = 100000;

interface DBSQLOperationConstructorOptions {
  logger: IDBSQLLogger;
}

export default class DBSQLOperation implements IOperation {
  private readonly driver: HiveDriver;

  private readonly operationHandle: TOperationHandle;

  private readonly logger: IDBSQLLogger;

  public onClose?: () => void;

  private readonly _status: OperationStatusHelper;

  private readonly _schema: SchemaHelper;

  private readonly _data: FetchResultsHelper;

  private readonly directResults?: TSparkDirectResults;

  private closed: boolean = false;

  private cancelled: boolean = false;

  constructor(
    driver: HiveDriver,
    operationHandle: TOperationHandle,
    { logger }: DBSQLOperationConstructorOptions,
    directResults?: TSparkDirectResults,
  ) {
    this.driver = driver;
    this.operationHandle = operationHandle;
    this.logger = logger;

    const useOnlyPrefetchedResults = Boolean(directResults?.closeOperation);

    this._status = new OperationStatusHelper(this.driver, this.operationHandle, directResults?.operationStatus);
    this._schema = new SchemaHelper(this.driver, this.operationHandle, directResults?.resultSetMetadata);
    this._data = new FetchResultsHelper(
      this.driver,
      this.operationHandle,
      [directResults?.resultSet],
      useOnlyPrefetchedResults,
    );
    this.directResults = directResults;
    this.logger.log(LogLevel.debug, `Operation created with id: ${this.getId()}`);
  }

  public getId() {
    return stringify(this.operationHandle?.operationId?.guid || parse(NIL));
  }

  /**
   * Fetches all data
   * @public
   * @param options - maxRows property can be set to limit chunk size
   * @returns Array of data with length equal to option.maxRows
   * @throws {StatusError}
   * @example
   * const result = await queryOperation.fetchAll();
   */
  public async fetchAll(options?: FetchOptions): Promise<Array<object>> {
    const data: Array<Array<object>> = [];
    do {
      // eslint-disable-next-line no-await-in-loop
      const chunk = await this.fetchChunk(options);
      data.push(chunk);
    } while (await this.hasMoreRows()); // eslint-disable-line no-await-in-loop
    this.logger?.log(LogLevel.debug, `Fetched all data from operation with id: ${this.getId()}`);

    return data.flat();
  }

  /**
   * Fetches chunk of data
   * @public
   * @param options - maxRows property sets chunk size
   * @returns Array of data with length equal to option.maxRows
   * @throws {StatusError}
   * @example
   * const result = await queryOperation.fetchChunk({maxRows: 1000});
   */
  public async fetchChunk(options?: FetchOptions): Promise<Array<object>> {
    await this.failIfClosed();

    if (!this._status.hasResultSet) {
      return [];
    }

    await this.waitUntilReady(options);

    const [resultHandler, data] = await Promise.all([
      this._schema.getResultHandler(),
      this._data.fetch(options?.maxRows || defaultMaxRows),
    ]);

    await this.failIfClosed();

    const result = await resultHandler.getValue(data ? [data] : []);
    this.logger?.log(
      LogLevel.debug,
      `Fetched chunk of size: ${options?.maxRows || defaultMaxRows} from operation with id: ${this.getId()}`,
    );
    return result;
  }

  /**
   * Requests operation status
   * @param progress
   * @throws {StatusError}
   */
  public async status(progress: boolean = false): Promise<TGetOperationStatusResp> {
    await this.failIfClosed();
    this.logger?.log(LogLevel.debug, `Fetching status for operation with id: ${this.getId()}`);
    return this._status.status(progress);
  }

  /**
   * Cancels operation
   * @throws {StatusError}
   */
  public async cancel(): Promise<Status> {
    if (this.closed || this.cancelled) {
      return Status.success();
    }

    this.logger?.log(LogLevel.debug, `Cancelling operation with id: ${this.getId()}`);

    const response = await this.driver.cancelOperation({
      operationHandle: this.operationHandle,
    });
    Status.assert(response.status);
    this.cancelled = true;
    const result = new Status(response.status);

    // Cancelled operation becomes unusable, similarly to being closed
    this.onClose?.();
    return result;
  }

  /**
   * Closes operation
   * @throws {StatusError}
   */
  public async close(): Promise<Status> {
    if (this.closed || this.cancelled) {
      return Status.success();
    }

    this.logger?.log(LogLevel.debug, `Closing operation with id: ${this.getId()}`);

    const response =
      this.directResults?.closeOperation ??
      (await this.driver.closeOperation({
        operationHandle: this.operationHandle,
      }));
    Status.assert(response.status);
    this.closed = true;
    const result = new Status(response.status);

    this.onClose?.();
    return result;
  }

  public async finished(options?: FinishedOptions): Promise<void> {
    await this.failIfClosed();
    await this.waitUntilReady(options);
  }

  public async hasMoreRows(): Promise<boolean> {
    // If operation is closed or cancelled - we should not try to get data from it
    if (this.closed || this.cancelled) {
      return false;
    }

    // Return early if there are still data available for fetching
    if (this._data.hasMoreRows) {
      return true;
    }

    // If we fetched all the data from server - check if there's anything buffered in result handler
    const resultHandler = await this._schema.getResultHandler();
    return resultHandler.hasPendingData();
  }

  public async getSchema(options?: GetSchemaOptions): Promise<TTableSchema | null> {
    await this.failIfClosed();

    if (!this._status.hasResultSet) {
      return null;
    }

    await this.waitUntilReady(options);

    this.logger?.log(LogLevel.debug, `Fetching schema for operation with id: ${this.getId()}`);
    return this._schema.fetch();
  }

  private async failIfClosed(): Promise<void> {
    if (this.closed) {
      throw new OperationStateError(OperationStateErrorCode.Closed);
    }
    if (this.cancelled) {
      throw new OperationStateError(OperationStateErrorCode.Canceled);
    }
  }

  private async waitUntilReady(options?: WaitUntilReadyOptions) {
    try {
      await this._status.waitUntilReady(options);
    } catch (error) {
      if (error instanceof OperationStateError) {
        if (error.errorCode === OperationStateErrorCode.Canceled) {
          this.cancelled = true;
        }
        if (error.errorCode === OperationStateErrorCode.Closed) {
          this.closed = true;
        }
      }
      throw error;
    }
  }
}
