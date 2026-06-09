import { Readable } from 'node:stream';
import IOperation, {
  FetchOptions,
  FinishedOptions,
  GetSchemaOptions,
  WaitUntilReadyOptions,
  IteratorOptions,
  IOperationChunksIterator,
  IOperationRowsIterator,
  NodeStreamOptions,
} from './contracts/IOperation';
import { TGetOperationStatusResp, TGetResultSetMetadataResp, TTableSchema } from '../thrift/TCLIService_types';
import Status from './dto/Status';
import { LogLevel } from './contracts/IDBSQLLogger';
import OperationStateError, { OperationStateErrorCode } from './errors/OperationStateError';
import { OperationChunksIterator, OperationRowsIterator } from './utils/OperationIterator';
import IClientContext from './contracts/IClientContext';
import IOperationBackend, { IOperationBackendWaitOptions } from './contracts/IOperationBackend';
import { ResultFormat, ResultMetadata } from './contracts/ResultMetadata';
import ThriftOperationBackend from './thrift-backend/ThriftOperationBackend';
import { synthesizeThriftStatus, synthesizeThriftResultSetMetadata } from './thrift-backend/wireSynthesis';
import { mapOperationTypeToTelemetryType } from './telemetry/telemetryTypeMappers';
import ExceptionClassifier from './telemetry/ExceptionClassifier';
import { safeEmit } from './telemetry/telemetryUtils';

function mapNeutralResultFormatToTelemetryType(resultFormat?: ResultFormat): string | undefined {
  switch (resultFormat) {
    case undefined:
      return undefined;
    case ResultFormat.ArrowBased:
      return 'INLINE_ARROW';
    case ResultFormat.ColumnBased:
      return 'COLUMNAR_INLINE';
    case ResultFormat.UrlBased:
      return 'EXTERNAL_LINKS';
    default:
      return 'FORMAT_UNSPECIFIED';
  }
}

interface DBSQLOperationConstructorOptions {
  backend: IOperationBackend;
  context: IClientContext;
  sessionId?: string;
}

export default class DBSQLOperation implements IOperation {
  private readonly context: IClientContext;

  private readonly backend: IOperationBackend;

  public onClose?: () => void;

  private closed: boolean = false;

  private cancelled: boolean = false;

  public readonly _data?: unknown;

  private metadata?: ResultMetadata;

  private startTime: number = Date.now();

  private pollCount: number = 0;

  private sessionId?: string;

  constructor(options: DBSQLOperationConstructorOptions) {
    this.context = options.context;
    this.backend = options.backend;
    this._data = options.backend.dataProvider;
    this.sessionId = options.sessionId;
    this.context.getLogger().log(LogLevel.debug, `Operation created with id: ${this.id}`);

    // Emit statement.start telemetry event
    this.emitStatementStart();
  }

  public get id() {
    return this.backend.id;
  }

  public iterateChunks(options?: IteratorOptions): IOperationChunksIterator {
    return new OperationChunksIterator(this, options);
  }

  public iterateRows(options?: IteratorOptions): IOperationRowsIterator {
    return new OperationRowsIterator(this, options);
  }

  public toNodeStream(options?: NodeStreamOptions): Readable {
    let iterable: IOperationChunksIterator | IOperationRowsIterator | undefined;

    switch (options?.mode ?? 'chunks') {
      case 'chunks':
        iterable = this.iterateChunks(options?.iteratorOptions);
        break;
      case 'rows':
        iterable = this.iterateRows(options?.iteratorOptions);
        break;
      default:
        throw new Error(`IOperation.toNodeStream: unsupported mode ${options?.mode}`);
    }

    return Readable.from(iterable, options?.streamOptions);
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
      disableBuffering: true,
    };

    do {
      // eslint-disable-next-line no-await-in-loop
      const chunk = await this.fetchChunk(fetchChunkOptions);
      data.push(chunk);
    } while (await this.hasMoreRows()); // eslint-disable-line no-await-in-loop
    this.context.getLogger().log(LogLevel.debug, `Fetched all data from operation with id: ${this.id}`);

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
    return this.withErrorTelemetry(() => this.fetchChunkInternal(options));
  }

  private async fetchChunkInternal(options?: FetchOptions): Promise<Array<object>> {
    await this.failIfClosed();

    if (!this.backend.hasResultSet()) {
      return [];
    }

    await this.waitUntilReadyThroughBackend(options);
    await this.failIfClosed();

    const defaultMaxRows = this.context.getConfig().fetchChunkDefaultMaxRows;
    const limit = options?.maxRows ?? defaultMaxRows;
    const result = await this.backend.fetchChunk({
      limit,
      disableBuffering: options?.disableBuffering,
      isClosed: () => this.closed || this.cancelled,
    });
    await this.failIfClosed();

    this.context.getLogger().log(LogLevel.debug, `Fetched chunk of size: ${limit} from operation with id: ${this.id}`);
    return result;
  }

  /**
   * Requests operation status. Returns the Thrift wire response for
   * back-compat with existing user code. On the Thrift backend the response
   * is returned verbatim; on any other backend (e.g. kernel) the response is
   * synthesized from the neutral {@link IOperationBackend.status} result,
   * with Thrift-only fields (`taskStatus`, `numModifiedRows`, etc.) left
   * undefined.
   *
   * @param progress
   * @throws {StatusError}
   */
  public async status(progress: boolean = false): Promise<TGetOperationStatusResp> {
    await this.failIfClosed();
    this.context.getLogger().log(LogLevel.debug, `Fetching status for operation with id: ${this.id}`);
    this.pollCount += 1;
    if (this.backend instanceof ThriftOperationBackend) {
      // Zero-loss path: the Thrift backend has the wire response on hand.
      return this.backend.thriftStatusResponse(progress);
    }
    // Non-Thrift backend: synthesize the Thrift-shaped response from the
    // neutral OperationStatus DTO.
    const status = await this.backend.status(progress);
    return synthesizeThriftStatus(status);
  }

  /**
   * Cancels operation
   * @throws {StatusError}
   */
  public async cancel(): Promise<Status> {
    return this.withErrorTelemetry(() => this.cancelInternal());
  }

  private async cancelInternal(): Promise<Status> {
    if (this.closed || this.cancelled) {
      return Status.success();
    }
    const result = await this.backend.cancel();
    this.cancelled = true;
    this.onClose?.();
    return result;
  }

  /**
   * Closes operation
   * @throws {StatusError}
   */
  public async close(): Promise<Status> {
    return this.withErrorTelemetry(() => this.closeInternal());
  }

  private async closeInternal(): Promise<Status> {
    if (this.closed || this.cancelled) {
      return Status.success();
    }
    const result = await this.backend.close();
    this.closed = true;

    // Emit statement.complete telemetry event
    await this.emitStatementComplete();

    this.onClose?.();
    return result;
  }

  public async finished(options?: FinishedOptions): Promise<void> {
    return this.withErrorTelemetry(async () => {
      await this.failIfClosed();
      await this.waitUntilReadyThroughBackend(options);
    });
  }

  public async hasMoreRows(): Promise<boolean> {
    return this.withErrorTelemetry(async () => {
      // If operation is closed or cancelled - we should not try to get data from it
      if (this.closed || this.cancelled) {
        return false;
      }

      // Wait for operation to finish before checking for more rows
      // This ensures metadata can be fetched successfully
      if (this.backend.hasResultSet()) {
        await this.waitUntilReadyThroughBackend();
      }

      // If we fetched all the data from server - check if there's anything buffered in result handler
      return this.backend.hasMore();
    });
  }

  public async getSchema(options?: GetSchemaOptions): Promise<TTableSchema | null> {
    return this.withErrorTelemetry(async () => {
      await this.failIfClosed();

      if (!this.backend.hasResultSet()) {
        return null;
      }

      await this.waitUntilReadyThroughBackend(options);

      this.context.getLogger().log(LogLevel.debug, `Fetching schema for operation with id: ${this.id}`);
      if (this.backend instanceof ThriftOperationBackend) {
        const metadata = await this.backend.thriftResultMetadataResponse();
        return metadata.schema ?? null;
      }
      const metadata = await this.getResultMetadata();
      return metadata.schema ?? null;
    });
  }

  /**
   * Thrift-only compatibility hook used by existing e2e tests to assert the
   * concrete result handler selected for a result format.
   *
   * Not part of the public `IOperation` contract.
   */
  public async getResultHandler(): Promise<unknown> {
    if (this.backend instanceof ThriftOperationBackend) {
      return this.backend.getResultHandler();
    }
    throw new Error('DBSQLOperation.getResultHandler is only available for the Thrift backend');
  }

  public async getResultMetadata(): Promise<ResultMetadata> {
    await this.failIfClosed();
    await this.waitUntilReadyThroughBackend();
    this.metadata = await this.backend.getResultMetadata();
    return this.metadata;
  }

  /**
   * Fetch result-set metadata as the Thrift wire response. Kept for
   * back-compat with existing user code. On the Thrift backend the wire
   * response is returned verbatim; on any other backend the response is
   * synthesized from the neutral {@link ResultMetadata}, with Thrift-only
   * fields (`cacheLookupResult`, `uncompressedBytes`, `compressedBytes`,
   * `status`) left undefined / defaulted.
   *
   * @deprecated Use {@link DBSQLOperation.getResultMetadata}; this method
   * synthesizes Thrift-only fields as `undefined` on non-Thrift backends and
   * couples callers to the Thrift wire shape.
   */
  public async getMetadata(): Promise<TGetResultSetMetadataResp> {
    return this.withErrorTelemetry(async () => {
      await this.failIfClosed();
      await this.waitUntilReadyThroughBackend();
      if (this.backend instanceof ThriftOperationBackend) {
        const thriftMetadata = await this.backend.thriftResultMetadataResponse();
        this.metadata = await this.backend.getResultMetadata();
        return thriftMetadata;
      }
      this.metadata = await this.backend.getResultMetadata();
      return synthesizeThriftResultSetMetadata(this.metadata);
    });
  }

  /**
   * Wrap a public IOperation method so any thrown error is captured as an
   * error telemetry event before being rethrown to the caller. Telemetry
   * never alters the throw semantics.
   */
  private async withErrorTelemetry<T>(fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (err: any) {
      this.emitErrorEvent(err);
      throw err;
    }
  }

  private async failIfClosed(): Promise<void> {
    if (this.closed) {
      throw new OperationStateError(OperationStateErrorCode.Closed);
    }
    if (this.cancelled) {
      throw new OperationStateError(OperationStateErrorCode.Canceled);
    }
  }

  private async waitUntilReadyThroughBackend(options?: WaitUntilReadyOptions) {
    // The backend-facing `waitUntilReady` takes a neutral
    // `IOperationBackendWaitOptions` whose `callback` receives an
    // `OperationStatus`. The public `WaitUntilReadyOptions.callback` is
    // Thrift-shaped — synthesize the wire response from the neutral status
    // at this boundary so the backend impl doesn't have to know about Thrift
    // IDL.
    const userCallback = options?.callback;
    const backendOptions: IOperationBackendWaitOptions = {
      progress: options?.progress,
      callback: (status) => {
        this.pollCount += 1;
        if (userCallback) {
          return userCallback(synthesizeThriftStatus(status));
        }
        return undefined;
      },
    };
    try {
      await this.backend.waitUntilReady(backendOptions);
    } catch (err) {
      // Reflect terminal states back into facade flags so subsequent calls
      // short-circuit via failIfClosed().
      if (err instanceof OperationStateError) {
        if (err.errorCode === OperationStateErrorCode.Canceled) {
          this.cancelled = true;
        } else if (err.errorCode === OperationStateErrorCode.Closed) {
          this.closed = true;
        }
      }
      throw err;
    }
  }

  /**
   * Emit statement.start telemetry event.
   * CRITICAL: All exceptions swallowed and logged at LogLevel.debug ONLY.
   */
  private emitStatementStart(): void {
    safeEmit(this.context, (emitter) => {
      emitter.emitStatementStart({
        statementId: this.id,
        // Pass `undefined` when sessionId is unknown rather than `''`. All
        // emit sites in this class use the same default so the aggregator's
        // per-statement state doesn't end up split between `''` and
        // `undefined` (which would synthesize two ghost sessions for a
        // single operation that briefly lacked a sessionId).
        sessionId: this.sessionId,
        operationType: mapOperationTypeToTelemetryType(this.backend.operationType),
      });
    });
  }

  /**
   * Emit statement.complete telemetry event and complete aggregation.
   * CRITICAL: All exceptions swallowed and logged at LogLevel.debug ONLY.
   */
  private async emitStatementComplete(): Promise<void> {
    safeEmit(this.context, (emitter) => {
      const aggregator = this.context.getTelemetryAggregator?.();
      if (!aggregator) return;

      // Use whatever metadata was already fetched by the result-handling
      // path. Do NOT trigger a `getMetadata()` here — that issues a Thrift
      // RPC on every close (doubles close latency for short DDL/DML) AND
      // throws if the operation is already in an error/closed state, which
      // would then fire spurious error telemetry from `getMetadata`'s error
      // wrapper.
      const resultFormat = mapNeutralResultFormatToTelemetryType(this.metadata?.resultFormat);
      const latencyMs = Date.now() - this.startTime;

      emitter.emitStatementComplete({
        statementId: this.id,
        sessionId: this.sessionId,
        latencyMs,
        resultFormat,
        pollCount: this.pollCount,
      });

      aggregator.completeStatement(this.id);
    });
  }

  /**
   * Emit a telemetry error event for an exception thrown by an operation.
   * Terminal errors (per `ExceptionClassifier`) trigger an immediate flush
   * in the aggregator; retryable errors are buffered until the statement
   * completes. All exceptions from this method itself are swallowed at
   * debug level — telemetry must never break the driver.
   */
  private emitErrorEvent(error: Error): void {
    safeEmit(this.context, (emitter) => {
      emitter.emitError({
        statementId: this.id,
        sessionId: this.sessionId,
        errorName: error.name || 'Error',
        errorMessage: error.message || 'Unknown error',
        errorStack: error.stack,
        isTerminal: ExceptionClassifier.isTerminal(error),
      });
    });
  }
}
