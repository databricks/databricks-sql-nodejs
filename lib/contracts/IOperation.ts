import { TGetOperationStatusResp, TTableSchema } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';

export type OperationStatusCallback = (progress: TGetOperationStatusResp) => unknown;

export interface FetchOptions {
  maxRows?: number;
  progress?: boolean;
  callback?: OperationStatusCallback;
}

export interface GetSchemaOptions {
  progress?: boolean;
  callback?: OperationStatusCallback;
}

export const defaultFetchOptions = {
  maxRows: 100000,
};

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
  finished(): Promise<void>;

  /**
   * Check if operation hasMoreRows
   */
  hasMoreRows(): Promise<boolean>;

  /**
   * Fetch schema
   */
  getSchema(options?: GetSchemaOptions): Promise<TTableSchema | null>;
}
