class RowSetProviderMock {
  async hasMore() {
    return false;
  }

  async fetchNext() {
    return undefined;
  }
}

module.exports = RowSetProviderMock;
