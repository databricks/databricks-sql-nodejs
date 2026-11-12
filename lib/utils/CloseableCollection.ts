export interface ICloseable {
  onClose?: () => void;

  close(): Promise<unknown>;
}

export default class CloseableCollection<T extends ICloseable> {
  private items = new Set<T>();

  public add(item: T) {
    item.onClose = () => {
      this.delete(item);
    };
    this.items.add(item);
  }

  public delete(item: T) {
    if (this.items.has(item)) {
      item.onClose = undefined;
    }
    this.items.delete(item);
  }

  /**
   * Attempts to close every item, even if some of them fail. Successfully closed items are removed from the
   * collection; failed ones are kept. Failures are reported once all items have been attempted
   * (see {@link throwCollectedErrors}).
   */
  public async closeAll() {
    const errors: Array<unknown> = [];
    const items = [...this.items];
    for (const item of items) {
      try {
        await item.close(); // eslint-disable-line no-await-in-loop
        item.onClose = undefined;
        this.items.delete(item);
      } catch (error) {
        errors.push(error);
      }
    }
    throwCollectedErrors(errors, 'Failed to close some resources');
  }
}

// `AggregateError` is available in every supported Node.js version, but isn't part of the ES2018 lib typings
// this project compiles against, so reference it with a local type instead of a global declaration.
const AggregateErrorConstructor = (
  globalThis as unknown as {
    AggregateError: new (errors: Iterable<unknown>, message?: string) => Error & { errors: Array<unknown> };
  }
).AggregateError;

/**
 * Rethrows the collected cleanup errors: nothing if there are none, the error itself if there is exactly one
 * (so callers see the same error as before), or an `AggregateError` holding all of them otherwise.
 */
export function throwCollectedErrors(errors: Array<unknown>, message: string): void {
  if (errors.length === 1) {
    throw errors[0];
  }
  if (errors.length > 1) {
    throw new AggregateErrorConstructor(errors, message);
  }
}
