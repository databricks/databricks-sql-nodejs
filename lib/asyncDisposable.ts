// Declares `Symbol.asyncDispose` for the emitted typings below
import './contracts/asyncDispose';

/**
 * A closeable resource that can also be bound with `await using`.
 */
export type AsyncDisposableResource<T> = T & {
  [Symbol.asyncDispose](): Promise<void>;
};

/**
 * Returns `resource` typed as async-disposable, so that it can be bound with `await using`.
 *
 * `connect()`, `openSession()` and the session's operation methods return the `IDBSQLClient` /
 * `IDBSQLSession` / `IOperation` interfaces, which don't require `Symbol.asyncDispose` (so custom
 * implementations and subclasses written before it was added keep compiling). The driver's own clients,
 * sessions, and operations implement it, so they are returned unchanged. A custom implementation without it gets a non-enumerable
 * `[Symbol.asyncDispose]()` that calls its `close()` (this throws a `TypeError` if the object isn't
 * extensible, e.g. a frozen object).
 *
 * @example
 * await using session = asyncDisposable(await client.openSession());
 * await using operation = asyncDisposable(await session.executeStatement('SELECT 1'));
 */
export default function asyncDisposable<T extends { close(): Promise<unknown> }>(
  resource: T,
): AsyncDisposableResource<T> {
  const candidate = resource as T & { [Symbol.asyncDispose]?: unknown };
  if (typeof candidate[Symbol.asyncDispose] !== 'function') {
    Object.defineProperty(resource, Symbol.asyncDispose, {
      configurable: true,
      writable: true,
      enumerable: false,
      value: async function asyncDispose(this: T): Promise<void> {
        await this.close();
      },
    });
  }
  return resource as AsyncDisposableResource<T>;
}
