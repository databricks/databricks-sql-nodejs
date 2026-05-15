import IBackend from '../contracts/IBackend';
import ISessionBackend from '../contracts/ISessionBackend';

const NOT_IMPLEMENTED = 'SEA backend not implemented yet — wired in sea-napi-binding feature';

export default class SeaBackend implements IBackend {
  public async connect(): Promise<void> {
    throw new Error(NOT_IMPLEMENTED);
  }

  public async openSession(): Promise<ISessionBackend> {
    throw new Error(NOT_IMPLEMENTED);
  }

  public async close(): Promise<void> {
    throw new Error(NOT_IMPLEMENTED);
  }
}
