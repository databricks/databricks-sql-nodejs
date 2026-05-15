import { TGetOperationStatusResp, TGetResultSetMetadataResp } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';
import { WaitUntilReadyOptions } from './IOperation';

/**
 * What a `DBSQLOperation` needs from its backend. Returned by
 * `ISessionBackend.executeStatement` and the metadata methods.
 */
export default interface IOperationBackend {
  readonly id: string;

  readonly hasResultSet: boolean;

  fetchChunk(options: { limit: number; disableBuffering?: boolean }): Promise<Array<object>>;

  hasMore(): Promise<boolean>;

  waitUntilReady(options?: WaitUntilReadyOptions): Promise<void>;

  status(progress: boolean): Promise<TGetOperationStatusResp>;

  getResultMetadata(): Promise<TGetResultSetMetadataResp>;

  cancel(): Promise<Status>;

  close(): Promise<Status>;
}
