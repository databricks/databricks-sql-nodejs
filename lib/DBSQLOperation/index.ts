import { stringify, NIL, parse } from 'uuid';
import IOperation, {
  FetchOptions,
  FinishedOptions,
  GetSchemaOptions,
  WaitUntilReadyOptions,
} from '../contracts/IOperation';
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
import { LogLevel } from '../contracts/IDBSQLLogger';
import OperationStateError, { OperationStateErrorCode } from '../errors/OperationStateError';
import IResultsProvider from '../result/IResultsProvider';
import RowSetProvider from '../result/RowSetProvider';
import JsonResultHandler from '../result/JsonResultHandler';
import ArrowResultHandler from '../result/ArrowResultHandler';
import CloudFetchResultHandler from '../result/CloudFetchResultHandler';
import ArrowResultConverter from '../result/ArrowResultConverter';
import ResultSlicer from '../result/ResultSlicer';
import { definedOrError } from '../utils';
import HiveDriverError from '../errors/HiveDriverError';
import IClientContext from '../contracts/IClientContext';

const defaultMaxRows = 100000;

interface DBSQLOperationConstructorOptions {
  handle: TOperationHandle;
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

  private readonly operationHandle: TOperationHandle;

  public onClose?: () => void;

  private readonly _data: RowSetProvider;

  private readonly closeOperation?: TCloseOperationResp;

  private closed: boolean = false;

  private cancelled: boolean = false;

  private metadata?: TGetResultSetMetadataResp;

  private state: number = TOperationState.INITIALIZED_STATE;

  // Once operation is finished or fails - cache status response, because subsequent calls
  // to `getOperationStatus()` may fail with irrelevant errors, e.g. HTTP 404
  private operationStatus?: TGetOperationStatusResp;

  private hasResultSet: boolean = false;

  private resultHandler?: ResultSlicer<any>;

  constructor({ handle, directResults, context }: DBSQLOperationConstructorOptions) {
    this.operationHandle = handle;
    this.context = context;

    const useOnlyPrefetchedResults = Boolean(directResults?.closeOperation);

    this.hasResultSet = this.operationHandle.hasResultSet;
    if (directResults?.operationStatus) {
      this.processOperationStatusResponse(directResults.operationStatus);
    }

    this.metadata = directResults?.resultSetMetadata;
    this._data = new RowSetProvider(
      this.context,
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

    const fetchChunkOptions = {
      ...options,
      // Tell slicer to return raw chunks. We're going to process all of them anyway,
      // so no need to additionally buffer and slice chunks returned by server
      disableBuffering: true,
    };

    do {
      // eslint-disable-next-line no-await-in-loop
      const chunk = await this.fetchChunk(fetchChunkOptions);
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

    const resultHandler = await this.getResultHandler();
    await this.failIfClosed();

    const result = resultHandler.fetchNext({
      limit: options?.maxRows || defaultMaxRows,
      disableBuffering: options?.disableBuffering,
    });
    await this.failIfClosed();

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

    const driver = await this.context.getDriver();
    const response = await driver.getOperationStatus({
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

    const driver = await this.context.getDriver();
    const response = await driver.cancelOperation({
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

    const driver = await this.context.getDriver();
    const response =
      this.closeOperation ??
      (await driver.closeOperation({
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

    // If we fetched all the data from server - check if there's anything buffered in result handler
    const resultHandler = await this.getResultHandler();
    return resultHandler.hasMore();
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
      const driver = await this.context.getDriver();
      const metadata = await driver.getResultSetMetadata({
        operationHandle: this.operationHandle,
      });
      Status.assert(metadata.status);
      this.metadata = metadata;
    }

    return this.metadata;
  }

  private async getResultHandler(): Promise<ResultSlicer<any>> {
    const metadata = await this.fetchMetadata();
    const resultFormat = definedOrError(metadata.resultFormat);

    if (!this.resultHandler) {
      let resultSource: IResultsProvider<Array<any>> | undefined;

      switch (resultFormat) {
        case TSparkRowSetType.COLUMN_BASED_SET:
          resultSource = new JsonResultHandler(this.context, this._data, metadata.schema);
          break;
        case TSparkRowSetType.ARROW_BASED_SET:
          resultSource = new ArrowResultConverter(
            this.context,
            new ArrowResultHandler(this.context, this._data, metadata.arrowSchema),
            metadata.schema,
          );
          break;
        case TSparkRowSetType.URL_BASED_SET:
          resultSource = new ArrowResultConverter(
            this.context,
            new CloudFetchResultHandler(this.context, this._data),
            metadata.schema,
          );
          break;
        // no default
      }

      if (resultSource) {
        this.resultHandler = new ResultSlicer(this.context, resultSource);
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
