import { TGetOperationStatusResp, TGetResultSetMetadataResp } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';

/**
 * What a `DBSQLOperation` needs from its backend. Returned by
 * `ISessionBackend.executeStatement` and the metadata methods.
 */
export default interface IOperationBackend {
  readonly id: string;

  readonly hasResultSet: boolean;

  fetchChunk(options: { limit: number; disableBuffering?: boolean }): Promise<Array<object>>;

  hasMore(): Promise<boolean>;

  waitUntilReady(options?: {
    progress?: boolean;
    callback?: (progress: TGetOperationStatusResp) => unknown;
  }): Promise<void>;

  status(progress: boolean): Promise<TGetOperationStatusResp>;

  getResultMetadata(): Promise<TGetResultSetMetadataResp>;

  cancel(): Promise<Status>;

  close(): Promise<Status>;
}
