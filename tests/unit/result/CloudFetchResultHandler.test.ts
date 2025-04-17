import { expect, AssertionError } from 'chai';
import sinon, { SinonStub } from 'sinon';
import Int64 from 'node-int64';
import LZ4 from 'lz4';
import { Request, Response } from 'node-fetch';
import { ShouldRetryResult } from '../../../lib/connection/contracts/IRetryPolicy';
import { HttpTransactionDetails } from '../../../lib/connection/contracts/IConnectionProvider';
import CloudFetchResultHandler from '../../../lib/result/CloudFetchResultHandler';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';
import { TRowSet, TStatusCode } from '../../../thrift/TCLIService_types';
import BaseClientContextStub from '../.stubs/ClientContextStub';
import { ClientConfig } from '../../../lib/contracts/IClientContext';
import ConnectionProviderStub from '../.stubs/ConnectionProviderStub';

const sampleArrowSchema = Buffer.from([
  255, 255, 255, 255, 208, 0, 0, 0, 16, 0, 0, 0, 0, 0, 10, 0, 14, 0, 6, 0, 13, 0, 8, 0, 10, 0, 0, 0, 0, 0, 4, 0, 16, 0,
  0, 0, 0, 1, 10, 0, 12, 0, 0, 0, 8, 0, 4, 0, 10, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 24, 0, 0, 0,
  0, 0, 18, 0, 24, 0, 20, 0, 0, 0, 19, 0, 12, 0, 0, 0, 8, 0, 4, 0, 18, 0, 0, 0, 20, 0, 0, 0, 80, 0, 0, 0, 88, 0, 0, 0,
  0, 0, 0, 2, 92, 0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 0, 8, 0, 12, 0, 8, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 3, 0,
  0, 0, 73, 78, 84, 0, 22, 0, 0, 0, 83, 112, 97, 114, 107, 58, 68, 97, 116, 97, 84, 121, 112, 101, 58, 83, 113, 108, 78,
  97, 109, 101, 0, 0, 0, 0, 0, 0, 8, 0, 12, 0, 8, 0, 7, 0, 8, 0, 0, 0, 0, 0, 0, 1, 32, 0, 0, 0, 1, 0, 0, 0, 49, 0, 0, 0,
  0, 0, 0, 0,
]);

const sampleArrowBatch = Buffer.from([
  255, 255, 255, 255, 136, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 12, 0, 22, 0, 14, 0, 21, 0, 16, 0, 4, 0, 12, 0, 0, 0, 16,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 16, 0, 0, 0, 0, 3, 10, 0, 24, 0, 12, 0, 8, 0, 4, 0, 10, 0, 0, 0, 20, 0, 0, 0, 56, 0,
  0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0,
  0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
  0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
]);

const defaultLinkExpiryTime = Date.now() + 24 * 60 * 60 * 1000; // 24hr in future

const sampleRowSet1: TRowSet = {
  startRowOffset: new Int64(0),
  rows: [],
  resultLinks: [
    {
      fileLink: 'http://example.com/result/1',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
      startRowOffset: new Int64(0),
      bytesNum: new Int64(0),
    },
    {
      fileLink: 'http://example.com/result/2',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
      startRowOffset: new Int64(0),
      bytesNum: new Int64(0),
    },
  ],
};

const sampleRowSet2: TRowSet = {
  startRowOffset: new Int64(0),
  rows: [],
  resultLinks: [
    {
      fileLink: 'http://example.com/result/3',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
      startRowOffset: new Int64(0),
      bytesNum: new Int64(0),
    },
    {
      fileLink: 'http://example.com/result/4',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
      startRowOffset: new Int64(0),
      bytesNum: new Int64(0),
    },
    {
      fileLink: 'http://example.com/result/5',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
      startRowOffset: new Int64(0),
      bytesNum: new Int64(0),
    },
  ],
};

const sampleEmptyRowSet: TRowSet = {
  startRowOffset: new Int64(0),
  rows: [],
  resultLinks: undefined,
};

const sampleExpiredRowSet: TRowSet = {
  startRowOffset: new Int64(0),
  rows: [],
  resultLinks: [
    {
      fileLink: 'http://example.com/result/6',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
      startRowOffset: new Int64(0),
      bytesNum: new Int64(0),
    },
    {
      fileLink: 'http://example.com/result/7',
      expiryTime: new Int64(Date.now() - 24 * 60 * 60 * 1000), // 24hr in past
      rowCount: new Int64(1),
      startRowOffset: new Int64(0),
      bytesNum: new Int64(0),
    },
  ],
};

class ClientContextStub extends BaseClientContextStub {
  public connectionProvider = sinon.stub(new ConnectionProviderStub());

  public invokeWithRetryStub = sinon.stub<[], Promise<HttpTransactionDetails>>();

  constructor(configOverrides: Partial<ClientConfig> = {}) {
    super(configOverrides);

    this.connectionProvider.getRetryPolicy.callsFake(async () => ({
      shouldRetry: async (): Promise<ShouldRetryResult> => ({ shouldRetry: false }),
      invokeWithRetry: async (): Promise<HttpTransactionDetails> => this.invokeWithRetryStub(),
    }));
  }
}

describe('CloudFetchResultHandler', () => {
  it('should report pending data if there are any', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });
    const rowSetProvider = new ResultsProviderStub([], undefined);
    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    // Test case 1: No pending data
    (result as any).pendingLinks = [];
    (result as any).downloadTasks = [];
    expect(await result.hasMore()).to.be.false;

    // Test case 2: Pending links
    (result as any).pendingLinks = [
      {
        fileLink: '',
        expiryTime: new Int64(0),
        startRowOffset: new Int64(0),
        rowCount: new Int64(0),
        bytesNum: new Int64(0),
      },
    ];
    (result as any).downloadTasks = [];
    expect(await result.hasMore()).to.be.true;

    // Test case 3: Download tasks
    (result as any).pendingLinks = [];
    (result as any).downloadTasks = [
      Promise.resolve({
        batches: [],
        rowCount: 0,
      }),
    ];
    expect(await result.hasMore()).to.be.true;
  });

  it('should extract links from row sets', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 0 });
    const rowSets = [sampleRowSet1, sampleEmptyRowSet, sampleRowSet2];
    const expectedLinksCount = rowSets.reduce((prev, item) => prev + (item.resultLinks?.length ?? 0), 0);
    const rowSetProvider = new ResultsProviderStub(rowSets, undefined);
    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    context.invokeWithRetryStub.callsFake(async () => ({
      request: new Request('localhost'),
      response: new Response(Buffer.concat([sampleArrowSchema, sampleArrowBatch]), { status: 200 }),
    }));

    // Process all row sets
    const processRowSets = async () => {
      let hasMore = await rowSetProvider.hasMore();
      while (hasMore) {
        await result.fetchNext({ limit: 100000 });
        hasMore = await rowSetProvider.hasMore();
      }
    };
    await processRowSets();

    expect((result as any).pendingLinks.length).to.be.equal(expectedLinksCount);
    expect((result as any).downloadTasks.length).to.be.equal(0);
    expect(context.invokeWithRetryStub.called).to.be.false;
  });

  it('should download batches in parallel according to settings', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 3 });
    const clientConfig = context.getConfig();

    const rowSet: TRowSet = {
      startRowOffset: new Int64(0),
      rows: [],
      resultLinks: [
        {
          fileLink: 'http://example.com/result/1',
          expiryTime: new Int64(Date.now() + 3600000),
          rowCount: new Int64(1),
          startRowOffset: new Int64(0),
          bytesNum: new Int64(0),
        },
        {
          fileLink: 'http://example.com/result/2',
          expiryTime: new Int64(Date.now() + 3600000),
          rowCount: new Int64(1),
          startRowOffset: new Int64(0),
          bytesNum: new Int64(0),
        },
      ],
    };
    const rowSetProvider = new ResultsProviderStub([rowSet], undefined);
    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    context.invokeWithRetryStub.callsFake(async () => ({
      request: new Request('localhost'),
      response: new Response(Buffer.concat([sampleArrowSchema, sampleArrowBatch]), { status: 200 }),
    }));

    expect(await rowSetProvider.hasMore()).to.be.true;

    // Initial fetch - should process multiple batches in parallel
    const { batches: initialBatches } = await result.fetchNext({ limit: 10000 });
    expect(initialBatches.length).to.be.gt(0);
    expect(await rowSetProvider.hasMore()).to.be.false;

    // Verify parallel processing
    expect((context.connectionProvider.getRetryPolicy as SinonStub).called).to.be.true;
    expect(context.invokeWithRetryStub.callCount).to.be.equal(2);
  });

  it('should return a proper row count in a batch', async () => {
    const context = new ClientContextStub();

    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      lz4Compressed: false,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    context.invokeWithRetryStub.callsFake(async () => ({
      request: new Request('localhost'),
      response: new Response(Buffer.alloc(0), { status: 200 }),
    }));

    expect(await rowSetProvider.hasMore()).to.be.true;

    const { rowCount } = await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;

    // it should use retry policy for all requests
    expect((context.connectionProvider.getRetryPolicy as SinonStub).called).to.be.true;
    expect(context.invokeWithRetryStub.called).to.be.true;
    expect(rowCount).to.equal(2);
  });

  it('should handle LZ4 compressed data', async () => {
    const context = new ClientContextStub();

    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      lz4Compressed: true,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    const expectedBatch = Buffer.concat([sampleArrowSchema, sampleArrowBatch]);

    context.invokeWithRetryStub.callsFake(async () => ({
      request: new Request('localhost'),
      response: new Response(LZ4.encode(expectedBatch), { status: 200 }),
    }));

    expect(await rowSetProvider.hasMore()).to.be.true;

    const { batches } = await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;

    expect((context.connectionProvider.getRetryPolicy as SinonStub).called).to.be.true;
    expect(context.invokeWithRetryStub.called).to.be.true;
    expect(batches[0]).to.deep.equal(expectedBatch);
  });

  it('should handle HTTP errors', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);
    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    context.invokeWithRetryStub.callsFake(async () => {
      const response = new Response(Buffer.concat([sampleArrowSchema, sampleArrowBatch]), {
        status: 500,
        statusText: 'Internal Server Error',
      });
      return {
        request: new Request('localhost'),
        response,
      };
    });

    try {
      await result.fetchNext({ limit: 10000 });
      throw new Error('Expected error to be thrown');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      console.log('[SHIVAM] error.message', error.message);
      expect(error.message).to.contain('CloudFetch HTTP error 500 Internal Server Error');
      expect((context.connectionProvider.getRetryPolicy as SinonStub).called).to.be.true;
      expect(context.invokeWithRetryStub.callCount).to.be.equal(1);
    }
  });

  it('should handle expired links', async () => {
    const context = new ClientContextStub();
    const expiredRowSet: TRowSet = {
      startRowOffset: new Int64(0),
      rows: [],
      resultLinks: [
        {
          fileLink: 'http://example.com/result/1',
          expiryTime: new Int64(Date.now() - 3600000), // 1 hour in past
          rowCount: new Int64(1),
          startRowOffset: new Int64(0),
          bytesNum: new Int64(0),
        },
      ],
    };
    const rowSetProvider = new ResultsProviderStub([expiredRowSet], undefined);
    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    context.invokeWithRetryStub.callsFake(async () => ({
      request: new Request('localhost'),
      response: new Response(Buffer.concat([sampleArrowSchema, sampleArrowBatch]), { status: 200 }),
    }));

    try {
      await result.fetchNext({ limit: 10000 });
      throw new Error('Expected error to be thrown');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error.message).to.contain('CloudFetch link has expired');
      // expect((context.connectionProvider.getRetryPolicy as SinonStub).called).to.be.true;
      expect(context.invokeWithRetryStub.callCount).to.be.equal(0);
    }
  });

  // it('should handle data without LZ4 compression', async () => {
  //   const context = new ClientContextStub();
  //   const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

  //   const result = new CloudFetchResultHandler(context, rowSetProvider, {
  //     lz4Compressed: false,
  //     status: { statusCode: TStatusCode.SUCCESS_STATUS },
  //   });

  //   context.invokeWithRetryStub.callsFake(async () => ({
  //     request: new Request('localhost'),
  //     response: new Response(sampleArrowBatch, { status: 200 }), // Return only the batch
  //   }));

  //   const { batches } = await result.fetchNext({ limit: 10000 });

  //   // Ensure the batches array matches the expected structure
  //   expect(batches).to.deep.eq([sampleArrowBatch]);
  // });
});
