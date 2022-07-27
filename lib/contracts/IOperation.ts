import { TGetOperationStatusResp, TTableSchema, TRowSet } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';

export default interface IOperation {
  /**
   * Fetch schema and a portion of data
   */
  fetch(): Promise<Status>;

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
   * Set the max fetch size
   */
  setMaxRows(maxRows: number): void;

  /**
   * Return retrieved schema
   */
  getSchema(): TTableSchema | null;

  /**
   * Return retrieved data
   */
  getData(): Array<TRowSet>;
}
