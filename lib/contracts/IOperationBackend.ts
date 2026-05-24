import Status from '../dto/Status';
import { WaitUntilReadyOptions } from './IOperation';
import { OperationStatus } from './OperationStatus';
import { ResultMetadata } from './ResultMetadata';

/**
 * What a `DBSQLOperation` needs from its backend. Returned by
 * `ISessionBackend.executeStatement` and the metadata methods.
 */
export default interface IOperationBackend {
  /** Operation identifier. */
  readonly id: string;

  /**
   * Whether this operation has a result set. Initial value may be derived
   * from the create-operation response; implementations MUST refresh it
   * from terminal status responses (the Thrift impl updates
   * `operationHandle.hasResultSet` inside `processOperationStatusResponse`).
   * `readonly` here means external callers cannot reassign the property —
   * not that the underlying value is fixed at construction time.
   */
  readonly hasResultSet: boolean;

  /** Fetch the next chunk of result rows. */
  fetchChunk(options: { limit: number; disableBuffering?: boolean }): Promise<Array<object>>;

  /** Whether more rows are available beyond what has been fetched. */
  hasMore(): Promise<boolean>;

  /**
   * Poll the backend until the operation reaches a terminal state.
   *
   * MUST throw `OperationStateError` (with one of `OperationStateErrorCode.{Canceled,
   * Closed, Error, Timeout, Unknown}`) on terminal non-success states. The
   * `DBSQLOperation` facade depends on `Canceled` and `Closed` codes to mirror
   * the operation into its closed/cancelled flags; future implementations must
   * use the same error type for the facade to stay in sync.
   */
  waitUntilReady(options?: WaitUntilReadyOptions): Promise<void>;

  /**
   * Fetch operation status as a neutral `OperationStatus`. Pass `progress: true`
   * to request that the backend include a progress payload.
   */
  status(progress: boolean): Promise<OperationStatus>;

  /** Fetch result-set metadata (schema, format, lz4 flag, arrow schema, staging flag). */
  getResultMetadata(): Promise<ResultMetadata>;

  /** Cancel the operation. */
  cancel(): Promise<Status>;

  /** Close the operation. Idempotent. */
  close(): Promise<Status>;
}
