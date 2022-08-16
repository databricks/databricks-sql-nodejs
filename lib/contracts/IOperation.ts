import { TGetOperationStatusResp, TTableSchema, TRowSet } from '../../thrift/TCLIService_types';
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
  status(progress: boolean): Promise<TGetOperationStatusResp>;

  /**
   * Cancel operation
   */
  cancel(): Promise<Status>;

  /**
   * Close operation
   */
  close(): Promise<Status>;

  /**
   * Check if operation is finished
   */
  finished(): boolean;

  /**
   * Check if operation hasMoreRows
   */
  hasMoreRows(): boolean;

  /**
   * Return retrieved schema
   */
  getSchema(): Promise<TTableSchema>;
}
