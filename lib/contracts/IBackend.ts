import { ConnectionOptions, OpenSessionRequest } from './IDBSQLClient';
import ISessionBackend from './ISessionBackend';

/**
 * Top-level backend dispatch handle. One instance per `DBSQLClient`,
 * chosen at `connect()` time based on the `useSEA` flag and never
 * re-selected per-call.
 */
export default interface IBackend {
  /**
   * Establish backend-level state before any session is opened. Implementations
   * consume `options` to build backend-specific connection parameters (e.g. the
   * SEA backend derives napi-binding `SeaNativeConnectionOptions` from the auth
   * + host fields here). Transport-layer connection providers are owned by
   * `DBSQLClient` (via `IClientContext`) and exposed to backends through
   * constructor injection.
   */
  connect(options: ConnectionOptions): Promise<void>;

  /**
   * Open a session. Returned `ISessionBackend` is owned by the caller
   * and torn down via its own `close()`.
   */
  openSession(request: OpenSessionRequest): Promise<ISessionBackend>;

  /**
   * Backend-level teardown. Transport-layer cleanup (connection provider,
   * thrift client, auth provider) is owned by `DBSQLClient` and runs
   * after this returns. Implementations release backend-internal resources
   * here, and MUST be safe to call on a partially-initialized backend
   * (i.e. after a failed `connect()`).
   */
  close(): Promise<void>;
}
