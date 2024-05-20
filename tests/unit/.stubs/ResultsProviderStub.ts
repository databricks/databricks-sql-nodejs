import IResultsProvider from '../../../lib/result/IResultsProvider';

export default class ResultsProviderStub<T> implements IResultsProvider<T> {
  private readonly items: Array<T>;

  private readonly emptyItem: T;

  constructor(items: Array<T>, emptyItem: T) {
    this.items = [...items];
    this.emptyItem = emptyItem;
  }

  async hasMore() {
    return this.items.length > 0;
  }

  async fetchNext() {
    return this.items.shift() ?? this.emptyItem;
  }
}
