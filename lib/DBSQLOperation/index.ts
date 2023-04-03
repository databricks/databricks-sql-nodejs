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
import CompleteOperationHelper from './CompleteOperationHelper';
import IDBSQLLogger, { LogLevel } from '../contracts/IDBSQLLogger';
import StatusFactory from '../factory/StatusFactory';
import OperationStateError, { OperationStateErrorCode } from '../errors/OperationStateError';

const defaultMaxRows = 100000;

interface DBSQLOperationConstructorOptions {
  logger: IDBSQLLogger;
}

export default class DBSQLOperation implements IOperation {
  private readonly driver: HiveDriver;

  private readonly operationHandle: TOperationHandle;

  private statusFactory = new StatusFactory();

  private logger: IDBSQLLogger;

  public onClose?: () => void;

  private _status: OperationStatusHelper;

  private _schema: SchemaHelper;

  private _data: FetchResultsHelper;

  private _completeOperation: CompleteOperationHelper;

  constructor(
    driver: HiveDriver,
    operationHandle: TOperationHandle,
    { logger }: DBSQLOperationConstructorOptions,
    directResults?: TSparkDirectResults,
  ) {
    this.driver = driver;
    this.operationHandle = operationHandle;
    this.logger = logger;
    this._status = new OperationStatusHelper(this.driver, this.operationHandle, directResults?.operationStatus);
    this._schema = new SchemaHelper(this.driver, this.operationHandle, directResults?.resultSetMetadata);
    this._data = new FetchResultsHelper(this.driver, this.operationHandle, [directResults?.resultSet]);
    this._completeOperation = new CompleteOperationHelper(
      this.driver,
      this.operationHandle,
      directResults?.closeOperation,
    );
    this.logger.log(LogLevel.debug, `Operation created with id: ${this.getId()}`);
  }

  getId() {
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

    return Promise.all([this._schema.getResultHandler(), this._data.fetch(options?.maxRows || defaultMaxRows)])
      .then(async (results) => {
        await this.failIfClosed();
        return results;
      })
      .then(async ([resultHandler, data]) => {
        const result = resultHandler.getValue(data ? [data] : []);
        this.logger?.log(
          LogLevel.debug,
          `Fetched chunk of size: ${options?.maxRows || defaultMaxRows} from operation with id: ${this.getId()}`,
        );
        return result;
      });
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
    if (this._completeOperation.closed || this._completeOperation.cancelled) {
      return this.statusFactory.success();
    }

    this.logger?.log(LogLevel.debug, `Cancelling operation with id: ${this.getId()}`);
    const result = this._completeOperation.cancel();

    // Cancelled operation becomes unusable, similarly to being closed
    this.onClose?.();
    return result;
  }

  /**
   * Closes operation
   * @throws {StatusError}
   */
  public async close(): Promise<Status> {
    if (this._completeOperation.closed || this._completeOperation.cancelled) {
      return this.statusFactory.success();
    }

    this.logger?.log(LogLevel.debug, `Closing operation with id: ${this.getId()}`);
    const result = await this._completeOperation.close();

    this.onClose?.();
    return result;
  }

  public async finished(options?: FinishedOptions): Promise<void> {
    await this.failIfClosed();
    await this.waitUntilReady(options);
  }

  public async hasMoreRows(): Promise<boolean> {
    if (this._completeOperation.closed || this._completeOperation.cancelled) {
      return false;
    }
    return this._data.hasMoreRows;
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
    if (this._completeOperation.closed) {
      throw new OperationStateError(OperationStateErrorCode.Closed);
    }
    if (this._completeOperation.cancelled) {
      throw new OperationStateError(OperationStateErrorCode.Canceled);
    }
  }

  private async waitUntilReady(options?: WaitUntilReadyOptions) {
    try {
      await this._status.waitUntilReady(options);
    } catch (error) {
      if (error instanceof OperationStateError) {
        if (error.errorCode === OperationStateErrorCode.Canceled) {
          this._completeOperation.cancelled = true;
        }
        if (error.errorCode === OperationStateErrorCode.Closed) {
          this._completeOperation.closed = true;
        }
      }
      throw error;
    }
  }
}
