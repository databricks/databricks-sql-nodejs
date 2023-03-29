import { stringify, NIL, parse } from 'uuid';
import IOperation, { FetchOptions, GetSchemaOptions, FinishedOptions } from '../contracts/IOperation';
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

const defaultMaxRows = 100000;

interface DBSQLOperationConstructorOptions {
  logger: IDBSQLLogger;
  onClose: (operation: IOperation) => void;
}

export default class DBSQLOperation implements IOperation {
  private readonly driver: HiveDriver;

  private readonly operationHandle: TOperationHandle;

  private statusFactory = new StatusFactory();

  private logger: IDBSQLLogger;

  private onClose: (operation: IOperation) => void;

  private _status: OperationStatusHelper;

  private _schema: SchemaHelper;

  private _data: FetchResultsHelper;

  private _completeOperation: CompleteOperationHelper;

  constructor(
    driver: HiveDriver,
    operationHandle: TOperationHandle,
    { logger, onClose }: DBSQLOperationConstructorOptions,
    directResults?: TSparkDirectResults,
  ) {
    this.driver = driver;
    this.operationHandle = operationHandle;
    this.logger = logger;
    this.onClose = onClose;
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
  async fetchAll(options?: FetchOptions): Promise<Array<object>> {
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
  async fetchChunk(options?: FetchOptions): Promise<Array<object>> {
    if (!this._status.hasResultSet) {
      return [];
    }

    await this._status.waitUntilReady(options);

    return Promise.all([this._schema.getResultHandler(), this._data.fetch(options?.maxRows || defaultMaxRows)]).then(
      ([resultHandler, data]) => {
        const result = resultHandler.getValue(data ? [data] : []);
        this.logger?.log(
          LogLevel.debug,
          `Fetched chunk of size: ${options?.maxRows || defaultMaxRows} from operation with id: ${this.getId()}`,
        );
        return Promise.resolve(result);
      },
    );
  }

  /**
   * Requests operation status
   * @param progress
   * @throws {StatusError}
   */
  async status(progress: boolean = false): Promise<TGetOperationStatusResp> {
    this.logger?.log(LogLevel.debug, `Fetching status for operation with id: ${this.getId()}`);
    return this._status.status(progress);
  }

  /**
   * Cancels operation
   * @throws {StatusError}
   */
  cancel(): Promise<Status> {
    this.logger?.log(LogLevel.debug, `Operation with id: ${this.getId()} canceled.`);
    return this._completeOperation.cancel();
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

    this.onClose(this);
    return result;
  }

  async finished(options?: FinishedOptions): Promise<void> {
    await this._status.waitUntilReady(options);
  }

  async hasMoreRows(): Promise<boolean> {
    if (this._completeOperation.closed || this._completeOperation.cancelled) {
      return false;
    }
    return this._data.hasMoreRows;
  }

  async getSchema(options?: GetSchemaOptions): Promise<TTableSchema | null> {
    if (!this._status.hasResultSet) {
      return null;
    }

    await this._status.waitUntilReady(options);
    this.logger?.log(LogLevel.debug, `Fetching schema for operation with id: ${this.getId()}`);

    return this._schema.fetch();
  }
}
