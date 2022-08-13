const { expect } = require('chai');
const HiveUtils = require('../../../dist/utils/HiveUtils').default;

describe('HiveUtils', () => {
  it('fetchAll', () => {
    const utils = new HiveUtils();
    const operation = {
      n: 0,
      _hasMoreRows: true,
      fetch() {
        this.n++;
        return Promise.resolve();
      },
      hasMoreRows() {
        const result = this._hasMoreRows;
        this._hasMoreRows = false;
        return result;
      },
    };
    return utils.fetchAll(operation).then((operation) => {
      expect(operation.n).to.be.eq(2);
    });
  });

  it('formatProgress', () => {
    const utils = new HiveUtils();
    const result = utils.formatProgress({
      headerNames: [],
      rows: [],
    });
    expect(result).to.be.eq('\n');
  });
});
