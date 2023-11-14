class RowSetProviderMock {
  constructor(rowSets) {
    this.rowSets = Array.isArray(rowSets) ? [...rowSets] : [];
  }

  async hasMore() {
    return this.rowSets.length > 0;
  }

  async fetchNext() {
    return this.rowSets.shift();
  }
}

module.exports = RowSetProviderMock;
