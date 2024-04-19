import { TGetOperationStatusResp, TTableSchema } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';

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

export default interface IOperation {
  /**
   * Fetch a portion of data
   */
  fetchChunk(options?: FetchOptions): Promise<Array<object>>;

  /**
   * Fetch all the data
   */
  fetchAll(options?: FetchOptions): Promise<Array<object>>;

  /**
   * Request status of operation
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

  iterateChunks(options?: IteratorOptions): IOperationChunksIterator;

  iterateRows(options?: IteratorOptions): IOperationRowsIterator;
}
