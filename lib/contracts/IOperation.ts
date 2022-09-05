import { TGetOperationStatusResp, TTableSchema } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';

export type OperationStatusCallback = (progress: TGetOperationStatusResp) => unknown;

export interface IFetchOptions {
  maxRows?: number;
  progress?: boolean;
  callback?: OperationStatusCallback;
}

export const defaultFetchOptions = {
  maxRows: 100000,
};

export default interface IOperation {
  /**
   * Fetch schema and a portion of data
   */
  fetchChunk(options?: IFetchOptions): Promise<Array<object>>;

  /**
   * Fetch schema and all the data
   */
  fetchAll(options?: IFetchOptions): Promise<Array<object>>;

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
  hasMoreRows(): boolean;

  /**
   * Return retrieved schema
   */
  getSchema(): Promise<TTableSchema | null>;
}
