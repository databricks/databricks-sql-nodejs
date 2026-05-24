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
import {
  TGetOperationStatusResp,
  TGetResultSetMetadataResp,
  TTableSchema,
} from '../thrift/TCLIService_types';
import Status from './dto/Status';
import { LogLevel } from './contracts/IDBSQLLogger';
import OperationStateError, { OperationStateErrorCode } from './errors/OperationStateError';
import { OperationChunksIterator, OperationRowsIterator } from './utils/OperationIterator';
import IClientContext from './contracts/IClientContext';
import IOperationBackend from './contracts/IOperationBackend';
import { ResultMetadata } from './contracts/ResultMetadata';
import ThriftOperationBackend from './thrift-backend/ThriftOperationBackend';
import { synthesizeThriftStatus, synthesizeThriftResultSetMetadata } from './utils/thriftWireSynthesis';

interface DBSQLOperationConstructorOptions {
  backend: IOperationBackend;
  context: IClientContext;
}

export default class DBSQLOperation implements IOperation {
  private readonly context: IClientContext;

  private readonly backend: IOperationBackend;

  public onClose?: () => void;

  private closed: boolean = false;

  private cancelled: boolean = false;

  constructor(options: DBSQLOperationConstructorOptions) {
    this.context = options.context;
    this.backend = options.backend;
    this.context.getLogger().log(LogLevel.debug, `Operation created with id: ${this.id}`);
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
    await this.failIfClosed();

    if (!this.backend.hasResultSet) {
      return [];
    }

    await this.waitUntilReadyThroughBackend(options);
    await this.failIfClosed();

    const defaultMaxRows = this.context.getConfig().fetchChunkDefaultMaxRows;
    const limit = options?.maxRows ?? defaultMaxRows;
    const result = await this.backend.fetchChunk({ limit, disableBuffering: options?.disableBuffering });
    await this.failIfClosed();

    this.context.getLogger().log(LogLevel.debug, `Fetched chunk of size: ${limit} from operation with id: ${this.id}`);
    return result;
  }

  /**
   * Requests operation status. Returns the Thrift wire response for
   * back-compat with existing user code. On the Thrift backend the response
   * is returned verbatim; on any other backend (e.g. SEA) the response is
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
    if (this.closed || this.cancelled) {
      return Status.success();
    }
    const result = await this.backend.close();
    this.closed = true;
    this.onClose?.();
    return result;
  }

  public async finished(options?: FinishedOptions): Promise<void> {
    await this.failIfClosed();
    await this.waitUntilReadyThroughBackend(options);
  }

  public async hasMoreRows(): Promise<boolean> {
    if (this.closed || this.cancelled) {
      return false;
    }

    if (this.backend.hasResultSet) {
      await this.waitUntilReadyThroughBackend();
    }

    return this.backend.hasMore();
  }

  public async getSchema(options?: GetSchemaOptions): Promise<TTableSchema | null> {
    await this.failIfClosed();

    if (!this.backend.hasResultSet) {
      return null;
    }

    await this.waitUntilReadyThroughBackend(options);

    this.context.getLogger().log(LogLevel.debug, `Fetching schema for operation with id: ${this.id}`);
    const metadata = await this.backend.getResultMetadata();
    return metadata.schema ?? null;
  }

  public async getResultMetadata(): Promise<ResultMetadata> {
    await this.failIfClosed();
    await this.waitUntilReadyThroughBackend();
    return this.backend.getResultMetadata();
  }

  /**
   * Fetch result-set metadata as the Thrift wire response. Kept for
   * back-compat with existing user code. On the Thrift backend the wire
   * response is returned verbatim; on any other backend the response is
   * synthesized from the neutral {@link ResultMetadata}, with Thrift-only
   * fields (`cacheLookupResult`, `uncompressedBytes`, `compressedBytes`,
   * `status`) left undefined / defaulted.
   *
   * Prefer {@link DBSQLOperation.getResultMetadata} in new code.
   */
  public async getMetadata(): Promise<TGetResultSetMetadataResp> {
    await this.failIfClosed();
    await this.waitUntilReadyThroughBackend();
    if (this.backend instanceof ThriftOperationBackend) {
      return this.backend.thriftResultMetadataResponse();
    }
    return synthesizeThriftResultSetMetadata(await this.backend.getResultMetadata());
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
    try {
      await this.backend.waitUntilReady(options);
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
}
