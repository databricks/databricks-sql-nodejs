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
  TGetResultSetMetadataResp,
  TSparkRowSetType,
  TCloseOperationResp,
  TOperationState,
} from '../../thrift/TCLIService_types';
import Status from '../dto/Status';
import FetchResultsHelper from './FetchResultsHelper';
import { LogLevel } from '../contracts/IDBSQLLogger';
import OperationStateError, { OperationStateErrorCode } from '../errors/OperationStateError';
import IOperationResult from '../result/IOperationResult';
import JsonResult from '../result/JsonResult';
import ArrowResult from '../result/ArrowResult';
import CloudFetchResult from '../result/CloudFetchResult';
import { definedOrError } from '../utils';
import HiveDriverError from '../errors/HiveDriverError';
import IClientContext from '../contracts/IClientContext';

const defaultMaxRows = 100000;

interface DBSQLOperationConstructorOptions {
  handle: TOperationHandle;
  driver: HiveDriver;
  directResults?: TSparkDirectResults;
  context: IClientContext;
}

async function delay(ms?: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, ms);
  });
}

export default class DBSQLOperation implements IOperation {
  private readonly context: IClientContext;

  private readonly driver: HiveDriver;

  private readonly operationHandle: TOperationHandle;

  public onClose?: () => void;

  private readonly _data: FetchResultsHelper;

  private readonly closeOperation?: TCloseOperationResp;

  private closed: boolean = false;

  private cancelled: boolean = false;

  private metadata?: TGetResultSetMetadataResp;

  private state: number = TOperationState.INITIALIZED_STATE;

  // Once operation is finished or fails - cache status response, because subsequent calls
  // to `getOperationStatus()` may fail with irrelevant errors, e.g. HTTP 404
  private operationStatus?: TGetOperationStatusResp;

  private hasResultSet: boolean = false;

  private resultHandler?: IOperationResult;

  constructor({ driver, handle, directResults, context }: DBSQLOperationConstructorOptions) {
    this.driver = driver;
    this.operationHandle = handle;
    this.context = context;

    const useOnlyPrefetchedResults = Boolean(directResults?.closeOperation);

    this.hasResultSet = this.operationHandle.hasResultSet;
    if (directResults?.operationStatus) {
      this.processOperationStatusResponse(directResults.operationStatus);
    }

    this.metadata = directResults?.resultSetMetadata;
    this._data = new FetchResultsHelper(
      this.driver,
      this.operationHandle,
      [directResults?.resultSet],
      useOnlyPrefetchedResults,
    );
    this.closeOperation = directResults?.closeOperation;
    this.context.getLogger().log(LogLevel.debug, `Operation created with id: ${this.getId()}`);
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
    this.context.getLogger().log(LogLevel.debug, `Fetched all data from operation with id: ${this.getId()}`);

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

    if (!this.hasResultSet) {
      return [];
    }

    await this.waitUntilReady(options);

    const [resultHandler, data] = await Promise.all([
      this.getResultHandler(),
      this._data.fetch(options?.maxRows || defaultMaxRows),
    ]);

    await this.failIfClosed();

    const result = await resultHandler.getValue(data ? [data] : []);
    this.context
      .getLogger()
      .log(
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
    this.context.getLogger().log(LogLevel.debug, `Fetching status for operation with id: ${this.getId()}`);

    if (this.operationStatus) {
      return this.operationStatus;
    }

    const response = await this.driver.getOperationStatus({
      operationHandle: this.operationHandle,
      getProgressUpdate: progress,
    });

    return this.processOperationStatusResponse(response);
  }

  /**
   * Cancels operation
   * @throws {StatusError}
   */
  public async cancel(): Promise<Status> {
    if (this.closed || this.cancelled) {
      return Status.success();
    }

    this.context.getLogger().log(LogLevel.debug, `Cancelling operation with id: ${this.getId()}`);

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

    this.context.getLogger().log(LogLevel.debug, `Closing operation with id: ${this.getId()}`);

    const response =
      this.closeOperation ??
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
    const resultHandler = await this.getResultHandler();
    return resultHandler.hasPendingData();
  }

  public async getSchema(options?: GetSchemaOptions): Promise<TTableSchema | null> {
    await this.failIfClosed();

    if (!this.hasResultSet) {
      return null;
    }

    await this.waitUntilReady(options);

    this.context.getLogger().log(LogLevel.debug, `Fetching schema for operation with id: ${this.getId()}`);
    const metadata = await this.fetchMetadata();
    return metadata.schema ?? null;
  }

  public async getMetadata(): Promise<TGetResultSetMetadataResp> {
    await this.failIfClosed();
    await this.waitUntilReady();
    return this.fetchMetadata();
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
    if (this.state === TOperationState.FINISHED_STATE) {
      return;
    }

    let isReady = false;

    while (!isReady) {
      // eslint-disable-next-line no-await-in-loop
      const response = await this.status(Boolean(options?.progress));

      if (options?.callback) {
        // eslint-disable-next-line no-await-in-loop
        await Promise.resolve(options.callback(response));
      }

      switch (response.operationState) {
        // For these states do nothing and continue waiting
        case TOperationState.INITIALIZED_STATE:
        case TOperationState.PENDING_STATE:
        case TOperationState.RUNNING_STATE:
          break;

        // Operation is completed, so exit the loop
        case TOperationState.FINISHED_STATE:
          isReady = true;
          break;

        // Operation was cancelled, so set a flag and exit the loop (throw an error)
        case TOperationState.CANCELED_STATE:
          this.cancelled = true;
          throw new OperationStateError(OperationStateErrorCode.Canceled, response);

        // Operation was closed, so set a flag and exit the loop (throw an error)
        case TOperationState.CLOSED_STATE:
          this.closed = true;
          throw new OperationStateError(OperationStateErrorCode.Closed, response);

        // Error states - throw and exit the loop
        case TOperationState.ERROR_STATE:
          throw new OperationStateError(OperationStateErrorCode.Error, response);
        case TOperationState.TIMEDOUT_STATE:
          throw new OperationStateError(OperationStateErrorCode.Timeout, response);
        case TOperationState.UKNOWN_STATE:
        default:
          throw new OperationStateError(OperationStateErrorCode.Unknown, response);
      }

      // If not ready yet - make some delay before the next status requests
      if (!isReady) {
        // eslint-disable-next-line no-await-in-loop
        await delay(100);
      }
    }
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

  private async getResultHandler(): Promise<IOperationResult> {
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

  private processOperationStatusResponse(response: TGetOperationStatusResp) {
    Status.assert(response.status);

    this.state = response.operationState ?? this.state;

    if (typeof response.hasResultSet === 'boolean') {
      this.hasResultSet = response.hasResultSet;
    }

    const isInProgress = [
      TOperationState.INITIALIZED_STATE,
      TOperationState.PENDING_STATE,
      TOperationState.RUNNING_STATE,
    ].includes(this.state);

    if (!isInProgress) {
      this.operationStatus = response;
    }

    return response;
  }
}
