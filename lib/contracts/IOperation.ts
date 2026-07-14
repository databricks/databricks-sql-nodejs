import { Readable, ReadableOptions } from 'node:stream';
import { TGetOperationStatusResp, TTableSchema } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';
import { ResultMetadata } from './ResultMetadata';

export type OperationStatusCallback = (progress: TGetOperationStatusResp) => unknown;

export interface WaitUntilReadyOptions {
  progress?: boolean;
  callback?: OperationStatusCallback;
}

export interface FinishedOptions extends WaitUntilReadyOptions {
  // no other options
}

export interface FetchOptions extends WaitUntilReadyOptions {
  maxRows?: number;
  // Disables internal buffer used to ensure a consistent chunks size.
  // When set to `true`, returned chunks size may vary (and may differ from `maxRows`)
  disableBuffering?: boolean;
}

export interface GetSchemaOptions extends WaitUntilReadyOptions {
  // no other options
}

export interface IteratorOptions extends FetchOptions {
  autoClose?: boolean; // defaults to `false`
}

export interface IOperationChunksIterator extends AsyncIterableIterator<Array<object>> {
  readonly operation: IOperation;
}

export interface IOperationRowsIterator extends AsyncIterableIterator<object> {
  readonly operation: IOperation;
}

export interface NodeStreamOptions {
  mode?: 'chunks' | 'rows'; // defaults to 'chunks'
  iteratorOptions?: IteratorOptions;
  streamOptions?: ReadableOptions;
}

export default interface IOperation {
  /**
   * Operation identifier
   */
  readonly id: string;

  /**
   * Fetch a portion of data
   */
  fetchChunk(options?: FetchOptions): Promise<Array<object>>;

  /**
   * Fetch all the data
   */
  fetchAll(options?: FetchOptions): Promise<Array<object>>;

  /**
   * Request status of operation. Returns the Thrift wire response for
   * back-compat. New code should prefer {@link IOperation.getResultMetadata}
   * for metadata and may consume the neutral `IOperationBackend.status` via
   * a typed downcast when implementing alternative backends.
   *
   * @param progress
   */
  status(progress?: boolean): Promise<TGetOperationStatusResp>;

  /**
   * Cancel operation
   */
  cancel(): Promise<Status>;

  /**
   * Close operation
   */
  close(): Promise<Status>;

  /**
   * Waits until operation is finished
   */
  finished(options?: FinishedOptions): Promise<void>;

  /**
   * Check if operation hasMoreRows
   */
  hasMoreRows(): Promise<boolean>;

  /**
   * Fetch schema
   */
  getSchema(options?: GetSchemaOptions): Promise<TTableSchema | null>;

  /**
   * Fetch result-set metadata in the backend-neutral `ResultMetadata` shape.
   * Prefer this over the Thrift-shaped surface for new code.
   */
  getResultMetadata(): Promise<ResultMetadata>;

  iterateChunks(options?: IteratorOptions): IOperationChunksIterator;

  iterateRows(options?: IteratorOptions): IOperationRowsIterator;

  toNodeStream(options?: NodeStreamOptions): Readable;
}
