import Status from '../dto/Status';
import { TOperationType } from '../../thrift/TCLIService_types';
import { OperationStatus } from './OperationStatus';
import { ResultMetadata } from './ResultMetadata';

/**
 * Backend-facing wait-loop options. Neutral by design — the `callback`
 * receives an {@link OperationStatus}, not a Thrift wire struct, so a
 * non-Thrift backend doesn't have to depend on Thrift IDL or synthesize a
 * fake wire response just to invoke it. The facade `DBSQLOperation` adapts
 * the user's public Thrift-shaped `OperationStatusCallback` into a neutral
 * callback at the facade boundary.
 */
export interface IOperationBackendWaitOptions {
  /** Request progress payload from the backend on each poll. */
  progress?: boolean;
  /** Neutral progress callback invoked once per poll. */
  callback?: (status: OperationStatus) => unknown;
}

/**
 * What a `DBSQLOperation` needs from its backend. Returned by
 * `ISessionBackend.executeStatement` and the metadata methods.
 *
 * Facade downcast policy: `DBSQLOperation.status()` and `getMetadata()`
 * use `instanceof ThriftOperationBackend` to return the verbatim Thrift wire
 * response on the Thrift path (zero-loss back-compat for legacy user code).
 * These two are grandfathered. Do NOT add further `instanceof <BackendClass>`
 * downcasts in the facade — extend this interface (or add an optional
 * method) instead, so the abstraction stays neutral as more backends land.
 */
export default interface IOperationBackend {
  /** Operation identifier. */
  readonly id: string;

  /** Optional Thrift operation type used for statement telemetry classification. */
  readonly operationType?: TOperationType;

  /**
   * Optional raw data provider exposed only for legacy tests that inspect
   * result-handler internals. Not part of the public operation contract.
   */
  readonly dataProvider?: unknown;

  /**
   * Whether this operation has a result set.
   *
   * Method-form (rather than property) because the value is state-dependent:
   * the initial value may be derived from the create-operation response, but
   * implementations MUST refresh it from terminal status responses (the
   * Thrift impl updates `operationHandle.hasResultSet` inside
   * `processOperationStatusResponse`). The method form signals to
   * implementers that this is a live read, not a constructor-time const.
   */
  hasResultSet(): boolean;

  /**
   * Fetch the next chunk of result rows.
   *
   * `isClosed`, when supplied, lets the facade probe its `closed`/`cancelled`
   * flags from inside the backend at a safe yield point between metadata
   * preparation and the data RPC. Implementations SHOULD invoke it after any
   * macrotask yield they introduce and short-circuit (return `[]`) when it
   * returns `true`, so a concurrent `cancel()`/`close()` does not run the data
   * RPC to completion needlessly. The facade re-checks `failIfClosed()` after
   * `fetchChunk` returns and throws the appropriate `OperationStateError`, so
   * returning `[]` is the correct way to bail — the user-visible error is
   * still raised by the facade.
   */
  fetchChunk(options: { limit: number; disableBuffering?: boolean; isClosed?: () => boolean }): Promise<Array<object>>;

  /** Whether more rows are available beyond what has been fetched. */
  hasMore(): Promise<boolean>;

  /**
   * Poll the backend until the operation reaches a terminal state.
   *
   * Receives neutral {@link IOperationBackendWaitOptions} — implementations
   * MUST invoke `options.callback` (when present) with a neutral
   * {@link OperationStatus}, not a Thrift wire struct. The public Thrift-
   * shaped `OperationStatusCallback` is adapted at the facade boundary in
   * `DBSQLOperation`.
   *
   * MUST throw `OperationStateError` (with one of `OperationStateErrorCode.{Canceled,
   * Closed, Error, Timeout, Unknown}`) on terminal non-success states. The
   * `DBSQLOperation` facade depends on `Canceled` and `Closed` codes to mirror
   * the operation into its closed/cancelled flags; future implementations must
   * use the same error type for the facade to stay in sync.
   */
  waitUntilReady(options?: IOperationBackendWaitOptions): Promise<void>;

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
