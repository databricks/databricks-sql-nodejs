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

  public async closeAll() {
    const items = [...this.items];
    for (const item of items) {
      await item.close(); // eslint-disable-line no-await-in-loop
      item.onClose = undefined;
      this.items.delete(item);
    }
  }
}
