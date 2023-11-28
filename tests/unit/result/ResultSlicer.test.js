const { expect } = require('chai');
const sinon = require('sinon');
const ResultSlicer = require('../../../dist/result/ResultSlicer').default;

class ResultsProviderMock {
  constructor(chunks) {
    this.chunks = chunks;
  }

  async hasMore() {
    return this.chunks.length > 0;
  }

  async fetchNext() {
    return this.chunks.shift() ?? [];
  }
}

describe('ResultSlicer', () => {
  it('should return chunks of requested size', async () => {
    const provider = new ResultsProviderMock([
      [10, 11, 12, 13, 14, 15],
      [20, 21, 22, 23, 24, 25],
      [30, 31, 32, 33, 34, 35],
    ]);

    const slicer = new ResultSlicer({}, provider);

    const chunk1 = await slicer.fetchNext({ limit: 4 });
    expect(chunk1).to.deep.eq([10, 11, 12, 13]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk2 = await slicer.fetchNext({ limit: 10 });
    expect(chunk2).to.deep.eq([14, 15, 20, 21, 22, 23, 24, 25, 30, 31]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk3 = await slicer.fetchNext({ limit: 10 });
    expect(chunk3).to.deep.eq([32, 33, 34, 35]);
    expect(await slicer.hasMore()).to.be.false;
  });

  it('should return raw chunks', async () => {
    const provider = new ResultsProviderMock([
      [10, 11, 12, 13, 14, 15],
      [20, 21, 22, 23, 24, 25],
      [30, 31, 32, 33, 34, 35],
    ]);
    sinon.spy(provider, 'fetchNext');

    const slicer = new ResultSlicer({}, provider);

    const chunk1 = await slicer.fetchNext({ limit: 4, disableBuffering: true });
    expect(chunk1).to.deep.eq([10, 11, 12, 13, 14, 15]);
    expect(await slicer.hasMore()).to.be.true;
    expect(provider.fetchNext.callCount).to.be.equal(1);

    const chunk2 = await slicer.fetchNext({ limit: 10, disableBuffering: true });
    expect(chunk2).to.deep.eq([20, 21, 22, 23, 24, 25]);
    expect(await slicer.hasMore()).to.be.true;
    expect(provider.fetchNext.callCount).to.be.equal(2);
  });

  it('should switch between returning sliced and raw chunks', async () => {
    const provider = new ResultsProviderMock([
      [10, 11, 12, 13, 14, 15],
      [20, 21, 22, 23, 24, 25],
      [30, 31, 32, 33, 34, 35],
    ]);

    const slicer = new ResultSlicer({}, provider);

    const chunk1 = await slicer.fetchNext({ limit: 4 });
    expect(chunk1).to.deep.eq([10, 11, 12, 13]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk2 = await slicer.fetchNext({ limit: 10, disableBuffering: true });
    expect(chunk2).to.deep.eq([14, 15]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk3 = await slicer.fetchNext({ limit: 10, disableBuffering: true });
    expect(chunk3).to.deep.eq([20, 21, 22, 23, 24, 25]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk4 = await slicer.fetchNext({ limit: 4 });
    expect(chunk4).to.deep.eq([30, 31, 32, 33]);
    expect(await slicer.hasMore()).to.be.true;

    const chunk5 = await slicer.fetchNext({ limit: 4 });
    expect(chunk5).to.deep.eq([34, 35]);
    expect(await slicer.hasMore()).to.be.false;
  });
});
