import { expect, AssertionError } from 'chai';
import CloseableCollection, { ICloseable } from '../../../lib/utils/CloseableCollection';

describe('CloseableCollection', () => {
  it('should add item if not already added', () => {
    const collection = new CloseableCollection<ICloseable>();
    expect(collection['items'].size).to.be.eq(0);

    const item: ICloseable = {
      close: () => Promise.resolve(),
    };

    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection['items'].size).to.be.eq(1);
  });

  it('should add item if it is already added', () => {
    const collection = new CloseableCollection<ICloseable>();
    expect(collection['items'].size).to.be.eq(0);

    const item: ICloseable = {
      close: () => Promise.resolve(),
    };

    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection['items'].size).to.be.eq(1);

    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection['items'].size).to.be.eq(1);
  });

  it('should delete item if already added', () => {
    const collection = new CloseableCollection<ICloseable>();
    expect(collection['items'].size).to.be.eq(0);

    const item: ICloseable = {
      close: () => Promise.resolve(),
    };

    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection['items'].size).to.be.eq(1);

    collection.delete(item);
    expect(item.onClose).to.be.undefined;
    expect(collection['items'].size).to.be.eq(0);
  });

  it('should delete item if not added', () => {
    const collection = new CloseableCollection<ICloseable>();
    expect(collection['items'].size).to.be.eq(0);

    const item: ICloseable = {
      close: () => Promise.resolve(),
    };

    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection['items'].size).to.be.eq(1);

    const otherItem: ICloseable = {
      onClose: () => {},
      close: () => Promise.resolve(),
    };
    collection.delete(otherItem);
    // if item is not in collection - it should be just skipped
    expect(otherItem.onClose).to.be.not.undefined;
    expect(collection['items'].size).to.be.eq(1);
  });

  it('should delete item if it was closed', async () => {
    const collection = new CloseableCollection<ICloseable>();
    expect(collection['items'].size).to.be.eq(0);

    const item: ICloseable = {
      close() {
        this.onClose?.();
        return Promise.resolve();
      },
    };

    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection['items'].size).to.be.eq(1);

    await item.close();
    expect(item.onClose).to.be.undefined;
    expect(collection['items'].size).to.be.eq(0);
  });

  it('should close all and delete all items', async () => {
    const collection = new CloseableCollection<ICloseable>();
    expect(collection['items'].size).to.be.eq(0);

    const item1: ICloseable = {
      close() {
        this.onClose?.();
        return Promise.resolve();
      },
    };

    const item2: ICloseable = {
      close() {
        this.onClose?.();
        return Promise.resolve();
      },
    };

    collection.add(item1);
    collection.add(item2);
    expect(item1.onClose).to.be.not.undefined;
    expect(item2.onClose).to.be.not.undefined;
    expect(collection['items'].size).to.be.eq(2);

    await collection.closeAll();
    expect(item1.onClose).to.be.undefined;
    expect(item2.onClose).to.be.undefined;
    expect(collection['items'].size).to.be.eq(0);
  });

  it('should attempt to close all items and keep only failed ones', async () => {
    const collection = new CloseableCollection<ICloseable>();
    expect(collection['items'].size).to.be.eq(0);

    const errorMessage = 'Error from item 2';

    const item1: ICloseable = {
      close() {
        this.onClose?.();
        return Promise.resolve();
      },
    };

    const item2: ICloseable = {
      close() {
        // Item should call `.onClose` only if it was successfully closed
        return Promise.reject(new Error(errorMessage));
      },
    };

    let item3Closed = false;
    const item3: ICloseable = {
      close() {
        item3Closed = true;
        this.onClose?.();
        return Promise.resolve();
      },
    };

    collection.add(item1);
    collection.add(item2);
    collection.add(item3);
    expect(item1.onClose).to.be.not.undefined;
    expect(item2.onClose).to.be.not.undefined;
    expect(item3.onClose).to.be.not.undefined;
    expect(collection['items'].size).to.be.eq(3);

    try {
      await collection.closeAll();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      // A single failure is rethrown as is
      expect(error.message).to.eq(errorMessage);
      expect(item1.onClose).to.be.undefined;
      expect(item2.onClose).to.be.not.undefined;
      // Items after the failed one are still closed
      expect(item3Closed).to.be.true;
      expect(item3.onClose).to.be.undefined;
      expect(collection['items'].size).to.be.eq(1);
      expect(collection['items'].has(item2)).to.be.true;
    }
  });

  it('should report multiple failures as an AggregateError', async () => {
    const collection = new CloseableCollection<ICloseable>();
    const error1 = new Error('Error from item 1');
    const error2 = new Error('Error from item 2');

    collection.add({ close: () => Promise.reject(error1) });
    collection.add({ close: () => Promise.reject(error2) });

    try {
      await collection.closeAll();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      // `AggregateError` isn't in the project's ES2018 lib typings
      expect(error).to.be.instanceOf((globalThis as any).AggregateError);
      expect((error as any).errors).to.deep.equal([error1, error2]);
      expect(collection['items'].size).to.be.eq(2);
    }
  });
});
