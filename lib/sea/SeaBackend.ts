import IBackend from '../contracts/IBackend';
import ISessionBackend from '../contracts/ISessionBackend';
import { ConnectionOptions, OpenSessionRequest } from '../contracts/IDBSQLClient';
import HiveDriverError from '../errors/HiveDriverError';

const NOT_IMPLEMENTED = 'SEA backend not implemented yet — wired in sea-napi-binding feature';

export default class SeaBackend implements IBackend {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars, class-methods-use-this
  public async connect(options: ConnectionOptions): Promise<void> {
    throw new HiveDriverError(NOT_IMPLEMENTED);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars, class-methods-use-this
  public async openSession(request: OpenSessionRequest): Promise<ISessionBackend> {
    throw new HiveDriverError(NOT_IMPLEMENTED);
  }

  // No-op so DBSQLClient.close() can finish its state-clearing block after a
  // failed useSEA: true connect. Real teardown lands with the M1 SEA impl.
  // eslint-disable-next-line @typescript-eslint/no-empty-function, class-methods-use-this
  public async close(): Promise<void> {}
}
