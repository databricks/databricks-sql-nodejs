import { stringify, NIL } from 'uuid';
import {
  TGetOperationStatusResp,
  TOperationHandle,
  TSparkDirectResults,
  TGetResultSetMetadataResp,
  TSparkRowSetType,
  TCloseOperationResp,
  TOperationState,
} from '../../thrift/TCLIService_types';
import IOperationBackend, { IOperationBackendWaitOptions } from '../contracts/IOperationBackend';
import IClientContext from '../contracts/IClientContext';
import { OperationStatus, OperationState } from '../contracts/OperationStatus';
import { ResultMetadata, ResultFormat } from '../contracts/ResultMetadata';
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

interface ThriftOperationBackendOptions {
  handle: TOperationHandle;
  directResults?: TSparkDirectResults;
  context: IClientContext;
}

async function delay(ms?: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function thriftStateToOperationState(state: TOperationState | undefined | null): OperationState {
  switch (state) {
    case TOperationState.INITIALIZED_STATE:
    case TOperationState.PENDING_STATE:
      return OperationState.Pending;
    case TOperationState.RUNNING_STATE:
      return OperationState.Running;
    case TOperationState.FINISHED_STATE:
      return OperationState.Succeeded;
    case TOperationState.CANCELED_STATE:
      return OperationState.Cancelled;
    case TOperationState.CLOSED_STATE:
      return OperationState.Closed;
    case TOperationState.ERROR_STATE:
    case TOperationState.TIMEDOUT_STATE:
      return OperationState.Failed;
    case TOperationState.UKNOWN_STATE:
    default:
      return OperationState.Unknown;
  }
}

function thriftRowSetTypeToResultFormat(type: TSparkRowSetType): ResultFormat {
  switch (type) {
    case TSparkRowSetType.COLUMN_BASED_SET:
      return ResultFormat.ColumnBased;
    case TSparkRowSetType.ARROW_BASED_SET:
      return ResultFormat.ArrowBased;
    case TSparkRowSetType.URL_BASED_SET:
      return ResultFormat.UrlBased;
    default:
      throw new HiveDriverError(`Unsupported result format: ${TSparkRowSetType[type]}`);
  }
}

export default class ThriftOperationBackend implements IOperationBackend {
  private readonly context: IClientContext;

  private readonly operationHandle: TOperationHandle;

  private readonly _data: RowSetProvider;

  private readonly closeOperation?: TCloseOperationResp;

  private metadata?: TGetResultSetMetadataResp;

  private metadataPromise?: Promise<TGetResultSetMetadataResp>;

  private state: TOperationState = TOperationState.INITIALIZED_STATE;

  private operationStatus?: TGetOperationStatusResp;

  private resultHandler?: ResultSlicer<any>;

  constructor({ handle, directResults, context }: ThriftOperationBackendOptions) {
    this.operationHandle = handle;
    this.context = context;

    const useOnlyPrefetchedResults = Boolean(directResults?.closeOperation);

    if (directResults?.operationStatus) {
      this.processOperationStatusResponse(directResults.operationStatus);
    }

    this.metadata = directResults?.resultSetMetadata;
    this._data = new RowSetProvider(
      this.context,
      this.operationHandle,
      [directResults?.resultSet],
      useOnlyPrefetchedResults,
      this.id,
    );
    this.closeOperation = directResults?.closeOperation;
  }

  public get id(): string {
    const operationId = this.operationHandle?.operationId?.guid;
    return operationId ? stringify(operationId) : NIL;
  }

  public get operationType() {
    return this.operationHandle.operationType;
  }

  public get dataProvider() {
    return this._data;
  }

  public hasResultSet(): boolean {
    return Boolean(this.operationHandle.hasResultSet);
  }

  public async fetchChunk({
    limit,
    disableBuffering,
    isClosed,
  }: {
    limit: number;
    disableBuffering?: boolean;
    isClosed?: () => boolean;
  }): Promise<Array<object>> {
    const resultHandler = await this.getResultHandler();

    // All the library code is Promise-based, however, since Promises are microtasks,
    // enqueueing a lot of promises may block macrotasks execution for a while.
    // Usually, there are no much microtasks scheduled, however, when fetching query
    // results (especially CloudFetch ones) it's quite easy to block event loop for
    // long enough to break a lot of things. For example, with CloudFetch, after first
    // set of files are downloaded and being processed immediately one by one, event
    // loop easily gets blocked for enough time to break connection pool. `http.Agent`
    // stops receiving socket events, and marks all sockets invalid on the next attempt
    // to use them. See these similar issues that helped to debug this particular case -
    // https://github.com/nodejs/node/issues/47130 and https://github.com/node-fetch/node-fetch/issues/1735
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });

    // Mid-flight cancel/close probe. The pre-refactor facade had a
    // `failIfClosed()` between metadata-prep and the data RPC; that guard moved
    // out of the facade when fetchChunk became atomic at this layer. Restore it
    // by polling the facade's flag here and bailing before the data RPC. The
    // facade re-checks `failIfClosed()` after we return and raises the
    // user-visible OperationStateError.
    if (isClosed?.()) {
      return [];
    }

    return resultHandler.fetchNext({ limit, disableBuffering });
  }

  public async hasMore(): Promise<boolean> {
    const resultHandler = await this.getResultHandler();
    return resultHandler.hasMore();
  }

  public async status(progress: boolean): Promise<OperationStatus> {
    const response = await this.thriftStatusResponse(progress);
    return this.adaptOperationStatus(response);
  }

  /**
   * Thrift-specific accessor that returns the raw `TGetOperationStatusResp`.
   *
   * Used internally to drive the Thrift state machine + attach the wire
   * response to `OperationStateError`. Also called by the public
   * `DBSQLOperation.status()` facade (zero-loss fast path) so existing user
   * code that reads `taskStatus`, `numModifiedRows`, etc. continues to work
   * verbatim against the Thrift backend.
   *
   * Not declared on `IOperationBackend` — non-Thrift backends do not
   * implement it. The facade reaches it via `instanceof ThriftOperationBackend`.
   */
  public async thriftStatusResponse(progress: boolean): Promise<TGetOperationStatusResp> {
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

  public async waitUntilReady(options?: IOperationBackendWaitOptions): Promise<void> {
    if (this.state === TOperationState.FINISHED_STATE) {
      return;
    }

    let isReady = false;

    while (!isReady) {
      // eslint-disable-next-line no-await-in-loop
      const response = await this.thriftStatusResponse(Boolean(options?.progress));

      if (options?.callback) {
        // The backend-facing callback is neutral (OperationStatus). Adapt the
        // Thrift wire response down to the neutral DTO; the facade's wrapper
        // synthesizes back up to TGetOperationStatusResp for the user's
        // public Thrift-shaped callback.
        // eslint-disable-next-line no-await-in-loop
        await Promise.resolve(options.callback(this.adaptOperationStatus(response)));
      }

      switch (response.operationState) {
        case TOperationState.INITIALIZED_STATE:
        case TOperationState.PENDING_STATE:
        case TOperationState.RUNNING_STATE:
          break;

        case TOperationState.FINISHED_STATE:
          isReady = true;
          break;

        case TOperationState.CANCELED_STATE:
          throw new OperationStateError(OperationStateErrorCode.Canceled, response);

        case TOperationState.CLOSED_STATE:
          throw new OperationStateError(OperationStateErrorCode.Closed, response);

        case TOperationState.ERROR_STATE:
          throw new OperationStateError(OperationStateErrorCode.Error, response);
        case TOperationState.TIMEDOUT_STATE:
          throw new OperationStateError(OperationStateErrorCode.Timeout, response);
        case TOperationState.UKNOWN_STATE:
        default:
          throw new OperationStateError(OperationStateErrorCode.Unknown, response);
      }

      if (!isReady) {
        // eslint-disable-next-line no-await-in-loop
        await delay(100);
      }
    }
  }

  public async getResultMetadata(): Promise<ResultMetadata> {
    return this.adaptResultMetadata(await this.thriftResultMetadataResponse());
  }

  /**
   * Thrift-specific accessor for the raw `TGetResultSetMetadataResp`.
   *
   * Used internally by `getResultHandler` (dispatches on Thrift `resultFormat`
   * and passes the full Thrift response to the JSON / Arrow / CloudFetch
   * result handlers). Also called by the public `DBSQLOperation.getMetadata()`
   * facade (zero-loss fast path).
   *
   * Not declared on `IOperationBackend` — non-Thrift backends do not implement
   * it. The facade reaches it via `instanceof ThriftOperationBackend`.
   */
  public async thriftResultMetadataResponse(): Promise<TGetResultSetMetadataResp> {
    if (this.metadata) {
      return this.metadata;
    }

    if (this.metadataPromise) {
      return this.metadataPromise;
    }

    this.metadataPromise = (async () => {
      const driver = await this.context.getDriver();
      const metadata = await driver.getResultSetMetadata({
        operationHandle: this.operationHandle,
      });
      Status.assert(metadata.status);
      this.metadata = metadata;
      return metadata;
    })();

    try {
      return await this.metadataPromise;
    } finally {
      this.metadataPromise = undefined;
    }
  }

  public async cancel(): Promise<Status> {
    this.context.getLogger().log(LogLevel.debug, `Cancelling operation with id: ${this.id}`);
    const driver = await this.context.getDriver();
    const response = await driver.cancelOperation({
      operationHandle: this.operationHandle,
    });
    Status.assert(response.status);
    return new Status(response.status);
  }

  public async close(): Promise<Status> {
    this.context.getLogger().log(LogLevel.debug, `Closing operation with id: ${this.id}`);
    const driver = await this.context.getDriver();
    const response =
      this.closeOperation ??
      (await driver.closeOperation({
        operationHandle: this.operationHandle,
      }));
    Status.assert(response.status);
    return new Status(response.status);
  }

  public async getResultHandler(): Promise<ResultSlicer<any>> {
    const metadata = await this.thriftResultMetadataResponse();
    const resultFormat = definedOrError(metadata.resultFormat);

    if (!this.resultHandler) {
      let resultSource: IResultsProvider<Array<any>> | undefined;

      switch (resultFormat) {
        case TSparkRowSetType.COLUMN_BASED_SET:
          resultSource = new JsonResultHandler(this.context, this._data, metadata);
          break;
        case TSparkRowSetType.ARROW_BASED_SET:
          resultSource = new ArrowResultConverter(
            this.context,
            new ArrowResultHandler(this.context, this._data, metadata),
            metadata,
          );
          break;
        case TSparkRowSetType.URL_BASED_SET:
          resultSource = new ArrowResultConverter(
            this.context,
            new CloudFetchResultHandler(this.context, this._data, metadata, this.id),
            metadata,
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
      this.operationHandle.hasResultSet = response.hasResultSet;
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

  private adaptOperationStatus(response: TGetOperationStatusResp): OperationStatus {
    return {
      state: thriftStateToOperationState(response.operationState),
      hasResultSet: typeof response.hasResultSet === 'boolean' ? response.hasResultSet : undefined,
      errorMessage: response.errorMessage ?? response.displayMessage ?? undefined,
      sqlState: response.sqlState ?? undefined,
      progressUpdateResponse: response.progressUpdateResponse,
    };
  }

  // eslint-disable-next-line class-methods-use-this
  private adaptResultMetadata(response: TGetResultSetMetadataResp): ResultMetadata {
    return {
      schema: response.schema,
      resultFormat: thriftRowSetTypeToResultFormat(definedOrError(response.resultFormat)),
      lz4Compressed: response.lz4Compressed,
      arrowSchema: response.arrowSchema,
      isStagingOperation: Boolean(response.isStagingOperation),
    };
  }
}
