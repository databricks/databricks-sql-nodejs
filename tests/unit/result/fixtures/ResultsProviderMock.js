class ResultsProviderMock {
  constructor(items, emptyItem) {
    this.items = Array.isArray(items) ? [...items] : [];
    this.emptyItem = emptyItem;
  }

  async hasMore() {
    return this.items.length > 0;
  }

  async fetchNext() {
    return this.items.shift() ?? this.emptyItem;
  }
}

module.exports = ResultsProviderMock;
