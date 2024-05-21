import { expect } from 'chai';
import Int64 from 'node-int64';
import LZ4 from 'lz4';
import ArrowResultHandler from '../../../lib/result/ArrowResultHandler';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';
import { TRowSet, TSparkArrowBatch, TStatusCode, TTableSchema } from '../../../thrift/TCLIService_types';

import ClientContextStub from '../.stubs/ClientContextStub';

const sampleArrowSchema = Buffer.from([
  255, 255, 255, 255, 208, 0, 0, 0, 16, 0, 0, 0, 0, 0, 10, 0, 14, 0, 6, 0, 13, 0, 8, 0, 10, 0, 0, 0, 0, 0, 4, 0, 16, 0,
  0, 0, 0, 1, 10, 0, 12, 0, 0, 0, 8, 0, 4, 0, 10, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 24, 0, 0, 0,
  0, 0, 18, 0, 24, 0, 20, 0, 0, 0, 19, 0, 12, 0, 0, 0, 8, 0, 4, 0, 18, 0, 0, 0, 20, 0, 0, 0, 80, 0, 0, 0, 88, 0, 0, 0,
  0, 0, 0, 2, 92, 0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 0, 8, 0, 12, 0, 8, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 3, 0,
  0, 0, 73, 78, 84, 0, 22, 0, 0, 0, 83, 112, 97, 114, 107, 58, 68, 97, 116, 97, 84, 121, 112, 101, 58, 83, 113, 108, 78,
  97, 109, 101, 0, 0, 0, 0, 0, 0, 8, 0, 12, 0, 8, 0, 7, 0, 8, 0, 0, 0, 0, 0, 0, 1, 32, 0, 0, 0, 1, 0, 0, 0, 49, 0, 0, 0,
  0, 0, 0, 0,
]);

const sampleArrowBatch: TSparkArrowBatch = {
  batch: Buffer.from([
    255, 255, 255, 255, 136, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 12, 0, 22, 0, 14, 0, 21, 0, 16, 0, 4, 0, 12, 0, 0, 0, 16,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 16, 0, 0, 0, 0, 3, 10, 0, 24, 0, 12, 0, 8, 0, 4, 0, 10, 0, 0, 0, 20, 0, 0, 0, 56,
    0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0,
    0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
    0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
  ]),
  rowCount: new Int64(1),
};

const sampleRowSet1: TRowSet = {
  startRowOffset: new Int64(0),
  rows: [],
  arrowBatches: [sampleArrowBatch],
};

const sampleRowSet1LZ4Compressed: TRowSet = {
  startRowOffset: new Int64(0),
  rows: [],
  arrowBatches: sampleRowSet1.arrowBatches?.map((item) => ({
    ...item,
    batch: LZ4.encode(item.batch),
  })),
};

const sampleRowSet2: TRowSet = {
  startRowOffset: new Int64(0),
  rows: [],
  arrowBatches: undefined,
};

const sampleRowSet3: TRowSet = {
  startRowOffset: new Int64(0),
  rows: [],
  arrowBatches: [],
};

const sampleRowSet4: TRowSet = {
  startRowOffset: new Int64(0),
  rows: [],
  arrowBatches: [
    {
      batch: undefined as unknown as Buffer,
      rowCount: new Int64(0),
    },
  ],
};

describe('ArrowResultHandler', () => {
  it('should return data', async () => {
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);
    const result = new ArrowResultHandler(new ClientContextStub(), rowSetProvider, {
      arrowSchema: sampleArrowSchema,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    const { batches } = await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;
    expect(await result.hasMore()).to.be.false;

    const expectedBatches = sampleRowSet1.arrowBatches?.map(({ batch }) => batch) ?? [];
    expect(batches).to.deep.eq([sampleArrowSchema, ...expectedBatches]);
  });

  it('should handle LZ4 compressed data', async () => {
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1LZ4Compressed], undefined);
    const result = new ArrowResultHandler(new ClientContextStub(), rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
      arrowSchema: sampleArrowSchema,
      lz4Compressed: true,
    });

    const { batches } = await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;
    expect(await result.hasMore()).to.be.false;

    const expectedBatches = sampleRowSet1.arrowBatches?.map(({ batch }) => batch) ?? [];
    expect(batches).to.deep.eq([sampleArrowSchema, ...expectedBatches]);
  });

  it('should not buffer any data', async () => {
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);
    const result = new ArrowResultHandler(new ClientContextStub(), rowSetProvider, {
      arrowSchema: sampleArrowSchema,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });
    expect(await rowSetProvider.hasMore()).to.be.true;
    expect(await result.hasMore()).to.be.true;

    await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;
    expect(await result.hasMore()).to.be.false;
  });

  it('should return empty array if no data to process', async () => {
    const expectedResult = {
      batches: [],
      rowCount: 0,
    };

    case1: {
      const rowSetProvider = new ResultsProviderStub([], undefined);
      const result = new ArrowResultHandler(new ClientContextStub(), rowSetProvider, {
        arrowSchema: sampleArrowSchema,
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
      });
      expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq(expectedResult);
      expect(await result.hasMore()).to.be.false;
    }
    case2: {
      const rowSetProvider = new ResultsProviderStub([sampleRowSet2], undefined);
      const result = new ArrowResultHandler(new ClientContextStub(), rowSetProvider, {
        arrowSchema: sampleArrowSchema,
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
      });
      expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq(expectedResult);
      expect(await result.hasMore()).to.be.false;
    }
    case3: {
      const rowSetProvider = new ResultsProviderStub([sampleRowSet3], undefined);
      const result = new ArrowResultHandler(new ClientContextStub(), rowSetProvider, {
        arrowSchema: sampleArrowSchema,
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
      });
      expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq(expectedResult);
      expect(await result.hasMore()).to.be.false;
    }
    case4: {
      const rowSetProvider = new ResultsProviderStub([sampleRowSet4], undefined);
      const result = new ArrowResultHandler(new ClientContextStub(), rowSetProvider, {
        arrowSchema: sampleArrowSchema,
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
      });
      expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq(expectedResult);
      expect(await result.hasMore()).to.be.false;
    }
  });

  it('should return a proper row count in a batch', async () => {
    const rowSetProvider = new ResultsProviderStub(
      [
        {
          ...sampleRowSet1,
          arrowBatches: [
            {
              batch: Buffer.alloc(0),
              rowCount: new Int64(2),
            },
            {
              batch: Buffer.alloc(0),
              rowCount: new Int64(0),
            },
            {
              batch: Buffer.alloc(0),
              rowCount: new Int64(3),
            },
          ],
        },
      ],
      undefined,
    );
    const result = new ArrowResultHandler(new ClientContextStub(), rowSetProvider, {
      arrowSchema: sampleArrowSchema,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    const { rowCount } = await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;
    expect(await result.hasMore()).to.be.false;
    expect(rowCount).to.equal(5);
  });

  it('should infer arrow schema from thrift schema', async () => {
    const rowSetProvider = new ResultsProviderStub([sampleRowSet2], undefined);

    const sampleThriftSchema: TTableSchema = {
      columns: [
        {
          columnName: '1',
          typeDesc: {
            types: [
              {
                primitiveEntry: {
                  type: 3,
                },
              },
            ],
          },
          position: 1,
        },
      ],
    };

    const result = new ArrowResultHandler(new ClientContextStub(), rowSetProvider, {
      schema: sampleThriftSchema,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });
    expect(result['arrowSchema']).to.not.be.undefined;
  });

  it('should return empty array if no schema available', async () => {
    const rowSetProvider = new ResultsProviderStub([sampleRowSet2], undefined);
    const result = new ArrowResultHandler(new ClientContextStub(), rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });
    expect(await result.fetchNext({ limit: 10000 })).to.be.deep.eq({
      batches: [],
      rowCount: 0,
    });
    expect(await result.hasMore()).to.be.false;
  });
});
