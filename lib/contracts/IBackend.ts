import { ConnectionOptions, OpenSessionRequest } from './IDBSQLClient';
import ISessionBackend from './ISessionBackend';

/**
 * Top-level backend dispatch handle. One instance per `DBSQLClient`,
 * chosen at `connect()` time based on the `useSEA` flag and never
 * re-selected per-call.
 */
export default interface IBackend {
  connect(options: ConnectionOptions): Promise<void>;

  openSession(request: OpenSessionRequest): Promise<ISessionBackend>;

  close(): Promise<void>;
}
