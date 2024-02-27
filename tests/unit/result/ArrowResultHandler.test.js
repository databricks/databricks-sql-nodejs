const { expect } = require('chai');
const LZ4 = require('lz4');
const ArrowResultHandler = require('../../../dist/result/ArrowResultHandler').default;
const ResultsProviderMock = require('./fixtures/ResultsProviderMock');

const sampleArrowSchema = Buffer.from([
  255, 255, 255, 255, 208, 0, 0, 0, 16, 0, 0, 0, 0, 0, 10, 0, 14, 0, 6, 0, 13, 0, 8, 0, 10, 0, 0, 0, 0, 0, 4, 0, 16, 0,
  0, 0, 0, 1, 10, 0, 12, 0, 0, 0, 8, 0, 4, 0, 10, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 24, 0, 0, 0,
  0, 0, 18, 0, 24, 0, 20, 0, 0, 0, 19, 0, 12, 0, 0, 0, 8, 0, 4, 0, 18, 0, 0, 0, 20, 0, 0, 0, 80, 0, 0, 0, 88, 0, 0, 0,
  0, 0, 0, 2, 92, 0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 0, 8, 0, 12, 0, 8, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 3, 0,
  0, 0, 73, 78, 84, 0, 22, 0, 0, 0, 83, 112, 97, 114, 107, 58, 68, 97, 116, 97, 84, 121, 112, 101, 58, 83, 113, 108, 78,
  97, 109, 101, 0, 0, 0, 0, 0, 0, 8, 0, 12, 0, 8, 0, 7, 0, 8, 0, 0, 0, 0, 0, 0, 1, 32, 0, 0, 0, 1, 0, 0, 0, 49, 0, 0, 0,
  0, 0, 0, 0,
]);

const sampleArrowBatch = {
  batch: Buffer.from([
    255, 255, 255, 255, 136, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 12, 0, 22, 0, 14, 0, 21, 0, 16, 0, 4, 0, 12, 0, 0, 0, 16,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 16, 0, 0, 0, 0, 3, 10, 0, 24, 0, 12, 0, 8, 0, 4, 0, 10, 0, 0, 0, 20, 0, 0, 0, 56,
    0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0,
    0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
    0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
  ]),
  rowCount: 1,
};

const sampleRowSet1 = {
  startRowOffset: 0,
  arrowBatches: [sampleArrowBatch],
};

const sampleRowSet1LZ4Compressed = {
  startRowOffset: 0,
  arrowBatches: sampleRowSet1.arrowBatches.map((item) => ({
    ...item,
    batch: LZ4.encode(item.batch),
  })),
};

const sampleRowSet2 = {
  startRowOffset: 0,
  arrowBatches: undefined,
};

const sampleRowSet3 = {
  startRowOffset: 0,
  arrowBatches: [],
};

const sampleRowSet4 = {
  startRowOffset: 0,
  arrowBatches: [
    {
      batch: undefined,
      rowCount: 0,
    },
  ],
};

describe('ArrowResultHandler', () => {
  it('should return data', async () => {
    const context = {};
    const rowSetProvider = new ResultsProviderMock([sampleRowSet1]);
    const result = new ArrowResultHandler(context, rowSetProvider, { arrowSchema: sampleArrowSchema });

    const batches = await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;
    expect(await result.hasMore()).to.be.false;

    const expectedBatches = sampleRowSet1.arrowBatches.map(({ batch }) => batch);
    expect(batches).to.deep.eq([sampleArrowSchema, ...expectedBatches]);
  });

  it('should handle LZ4 compressed data', async () => {
    const context = {};
    const rowSetProvider = new ResultsProviderMock([sampleRowSet1LZ4Compressed]);
    const result = new ArrowResultHandler(context, rowSetProvider, {
      arrowSchema: sampleArrowSchema,
      lz4Compressed: true,
    });

    const batches = await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;
    expect(await result.hasMore()).to.be.false;

    const expectedBatches = sampleRowSet1.arrowBatches.map(({ batch }) => batch);
    expect(batches).to.deep.eq([sampleArrowSchema, ...expectedBatches]);
  });

  it('should not buffer any data', async () => {
    const context = {};
    const rowSetProvider = new ResultsProviderMock([sampleRowSet1]);
    const result = new ArrowResultHandler(context, rowSetProvider, { arrowSchema: sampleArrowSchema });
    expect(await rowSetProvider.hasMore()).to.be.true;
    expect(await result.hasMore()).to.be.true;

    await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;
    expect(await result.hasMore()).to.be.false;
  });

  it('should return empty array if no data to process', async () => {
    const context = {};
    case1: {
      const rowSetProvider = new ResultsProviderMock();
      const result = new ArrowResultHandler(context, rowSetProvider, { arrowSchema: sampleArrowSchema });
      expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq([]);
      expect(await result.hasMore()).to.be.false;
    }
    case2: {
      const rowSetProvider = new ResultsProviderMock([sampleRowSet2]);
      const result = new ArrowResultHandler(context, rowSetProvider, { arrowSchema: sampleArrowSchema });
      expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq([]);
      expect(await result.hasMore()).to.be.false;
    }
    case3: {
      const rowSetProvider = new ResultsProviderMock([sampleRowSet3]);
      const result = new ArrowResultHandler(context, rowSetProvider, { arrowSchema: sampleArrowSchema });
      expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq([]);
      expect(await result.hasMore()).to.be.false;
    }
    case4: {
      const rowSetProvider = new ResultsProviderMock([sampleRowSet4]);
      const result = new ArrowResultHandler(context, rowSetProvider, { arrowSchema: sampleArrowSchema });
      expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq([]);
      expect(await result.hasMore()).to.be.false;
    }
  });

  it('should return empty array if no schema available', async () => {
    const context = {};
    const rowSetProvider = new ResultsProviderMock([sampleRowSet2]);
    const result = new ArrowResultHandler(context, rowSetProvider, {});
    expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq([]);
    expect(await result.hasMore()).to.be.false;
  });
});
