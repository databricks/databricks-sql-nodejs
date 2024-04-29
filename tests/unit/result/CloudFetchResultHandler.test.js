const { expect, AssertionError } = require('chai');
const sinon = require('sinon');
const Int64 = require('node-int64');
const LZ4 = require('lz4');
const CloudFetchResultHandler = require('../../../lib/result/CloudFetchResultHandler').default;
const ResultsProviderMock = require('./fixtures/ResultsProviderMock');
const DBSQLClient = require('../../../lib/DBSQLClient').default;

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

const sampleRowSet1 = {
  startRowOffset: 0,
  resultLinks: [
    {
      fileLink: 'http://example.com/result/1',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
    },
    {
      fileLink: 'http://example.com/result/2',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
    },
  ],
};

const sampleRowSet2 = {
  startRowOffset: new Int64(0),
  resultLinks: [
    {
      fileLink: 'http://example.com/result/3',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
    },
    {
      fileLink: 'http://example.com/result/4',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
    },
    {
      fileLink: 'http://example.com/result/5',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
    },
  ],
};

const sampleEmptyRowSet = {
  startRowOffset: new Int64(0),
  resultLinks: undefined,
};

const sampleExpiredRowSet = {
  startRowOffset: new Int64(0),
  resultLinks: [
    {
      fileLink: 'http://example.com/result/6',
      expiryTime: new Int64(defaultLinkExpiryTime),
      rowCount: new Int64(1),
    },
    {
      fileLink: 'http://example.com/result/7',
      expiryTime: new Int64(Date.now() - 24 * 60 * 60 * 1000), // 24hr in past
      rowCount: new Int64(1),
    },
  ],
};

class ClientContextMock {
  constructor(configOverrides) {
    this.configOverrides = configOverrides;
    this.fetchHandler = sinon.stub();

    this.connectionProvider = {
      getAgent: () => Promise.resolve(undefined),
      getRetryPolicy: sinon.stub().returns(
        Promise.resolve({
          shouldRetry: sinon.stub().returns(Promise.resolve({ shouldRetry: false })),
          invokeWithRetry: sinon.stub().callsFake(() => this.fetchHandler().then((response) => ({ response }))),
        }),
      ),
    };
  }

  getConfig() {
    const defaultConfig = DBSQLClient.getDefaultConfig();
    return {
      ...defaultConfig,
      ...this.configOverrides,
    };
  }

  getConnectionProvider() {
    return Promise.resolve(this.connectionProvider);
  }
}

describe('CloudFetchResultHandler', () => {
  it('should report pending data if there are any', async () => {
    const context = new ClientContextMock({ cloudFetchConcurrentDownloads: 1 });
    const rowSetProvider = new ResultsProviderMock();

    const result = new CloudFetchResultHandler(context, rowSetProvider, {});

    case1: {
      result.pendingLinks = [];
      result.downloadTasks = [];
      expect(await result.hasMore()).to.be.false;
    }

    case2: {
      result.pendingLinks = [{}]; // just anything here
      result.downloadTasks = [];
      expect(await result.hasMore()).to.be.true;
    }

    case3: {
      result.pendingLinks = [];
      result.downloadTasks = [{}]; // just anything here
      expect(await result.hasMore()).to.be.true;
    }
  });

  it('should extract links from row sets', async () => {
    const context = new ClientContextMock({ cloudFetchConcurrentDownloads: 0 });

    const rowSets = [sampleRowSet1, sampleEmptyRowSet, sampleRowSet2];
    const expectedLinksCount = rowSets.reduce((prev, item) => prev + (item.resultLinks?.length ?? 0), 0);

    const rowSetProvider = new ResultsProviderMock(rowSets);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {});

    context.fetchHandler.returns(
      Promise.resolve({
        ok: true,
        status: 200,
        statusText: 'OK',
        arrayBuffer: async () => Buffer.concat([sampleArrowSchema, sampleArrowBatch]),
      }),
    );

    do {
      await result.fetchNext({ limit: 100000 });
    } while (await rowSetProvider.hasMore());

    expect(result.pendingLinks.length).to.be.equal(expectedLinksCount);
    expect(result.downloadTasks.length).to.be.equal(0);
    expect(context.fetchHandler.called).to.be.false;
  });

  it('should download batches according to settings', async () => {
    const context = new ClientContextMock({ cloudFetchConcurrentDownloads: 3 });
    const clientConfig = context.getConfig();

    const rowSet = {
      startRowOffset: new Int64(0),
      resultLinks: [...sampleRowSet1.resultLinks, ...sampleRowSet2.resultLinks],
    };
    const expectedLinksCount = rowSet.resultLinks.length; // 5
    const rowSetProvider = new ResultsProviderMock([rowSet]);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {});

    context.fetchHandler.returns(
      Promise.resolve({
        ok: true,
        status: 200,
        statusText: 'OK',
        arrayBuffer: async () => Buffer.concat([sampleArrowSchema, sampleArrowBatch]),
      }),
    );

    expect(await rowSetProvider.hasMore()).to.be.true;

    initialFetch: {
      // `cloudFetchConcurrentDownloads` out of `expectedLinksCount` links should be scheduled immediately
      // first one should be `await`-ed and returned from `fetchNext`
      const { batches } = await result.fetchNext({ limit: 10000 });
      expect(batches.length).to.be.gt(0);
      expect(await rowSetProvider.hasMore()).to.be.false;

      // it should use retry policy for all requests
      expect(context.connectionProvider.getRetryPolicy.called).to.be.true;
      expect(context.fetchHandler.callCount).to.be.equal(clientConfig.cloudFetchConcurrentDownloads);
      expect(result.pendingLinks.length).to.be.equal(expectedLinksCount - clientConfig.cloudFetchConcurrentDownloads);
      expect(result.downloadTasks.length).to.be.equal(clientConfig.cloudFetchConcurrentDownloads - 1);
    }

    secondFetch: {
      // It should return previously fetched batch, and schedule one more
      const { batches } = await result.fetchNext({ limit: 10000 });
      expect(batches.length).to.be.gt(0);
      expect(await rowSetProvider.hasMore()).to.be.false;

      // it should use retry policy for all requests
      expect(context.connectionProvider.getRetryPolicy.called).to.be.true;
      expect(context.fetchHandler.callCount).to.be.equal(clientConfig.cloudFetchConcurrentDownloads + 1);
      expect(result.pendingLinks.length).to.be.equal(
        expectedLinksCount - clientConfig.cloudFetchConcurrentDownloads - 1,
      );
      expect(result.downloadTasks.length).to.be.equal(clientConfig.cloudFetchConcurrentDownloads - 1);
    }

    thirdFetch: {
      // Now buffer should be empty, and it should fetch next batches
      const { batches } = await result.fetchNext({ limit: 10000 });
      expect(batches.length).to.be.gt(0);
      expect(await rowSetProvider.hasMore()).to.be.false;

      // it should use retry policy for all requests
      expect(context.connectionProvider.getRetryPolicy.called).to.be.true;
      expect(context.fetchHandler.callCount).to.be.equal(clientConfig.cloudFetchConcurrentDownloads + 2);
      expect(result.pendingLinks.length).to.be.equal(
        expectedLinksCount - clientConfig.cloudFetchConcurrentDownloads - 2,
      );
      expect(result.downloadTasks.length).to.be.equal(clientConfig.cloudFetchConcurrentDownloads - 1);
    }
  });

  it('should return a proper row count in a batch', async () => {
    const context = new ClientContextMock();

    const rowSetProvider = new ResultsProviderMock([sampleRowSet1]);

    const result = new CloudFetchResultHandler(context, rowSetProvider, { lz4Compressed: false });

    context.fetchHandler.returns(
      Promise.resolve({
        ok: true,
        status: 200,
        statusText: 'OK',
        arrayBuffer: async () => Buffer.alloc(0),
      }),
    );

    expect(await rowSetProvider.hasMore()).to.be.true;

    const { rowCount } = await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;

    // it should use retry policy for all requests
    expect(context.connectionProvider.getRetryPolicy.called).to.be.true;
    expect(context.fetchHandler.called).to.be.true;
    expect(rowCount).to.equal(1);
  });

  it('should handle LZ4 compressed data', async () => {
    const context = new ClientContextMock();

    const rowSetProvider = new ResultsProviderMock([sampleRowSet1]);

    const result = new CloudFetchResultHandler(context, rowSetProvider, { lz4Compressed: true });

    const expectedBatch = Buffer.concat([sampleArrowSchema, sampleArrowBatch]);

    context.fetchHandler.returns(
      Promise.resolve({
        ok: true,
        status: 200,
        statusText: 'OK',
        arrayBuffer: async () => LZ4.encode(expectedBatch),
      }),
    );

    expect(await rowSetProvider.hasMore()).to.be.true;

    const { batches } = await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;

    // it should use retry policy for all requests
    expect(context.connectionProvider.getRetryPolicy.called).to.be.true;
    expect(context.fetchHandler.called).to.be.true;
    expect(batches).to.deep.eq([expectedBatch]);
  });

  it('should handle HTTP errors', async () => {
    const context = new ClientContextMock({ cloudFetchConcurrentDownloads: 1 });

    const rowSetProvider = new ResultsProviderMock([sampleRowSet1]);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {});

    context.fetchHandler.returns(
      Promise.resolve({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        arrayBuffer: async () => Buffer.concat([sampleArrowSchema, sampleArrowBatch]),
      }),
    );

    try {
      await result.fetchNext({ limit: 10000 });
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error.message).to.contain('Internal Server Error');
      // it should use retry policy for all requests
      expect(context.connectionProvider.getRetryPolicy.called).to.be.true;
      expect(context.fetchHandler.callCount).to.be.equal(1);
    }
  });

  it('should handle expired links', async () => {
    const context = new ClientContextMock();
    const rowSetProvider = new ResultsProviderMock([sampleExpiredRowSet]);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {});

    context.fetchHandler.returns(
      Promise.resolve({
        ok: true,
        status: 200,
        statusText: 'OK',
        arrayBuffer: async () => Buffer.concat([sampleArrowSchema, sampleArrowBatch]),
      }),
    );

    // There are two link in the batch - first one is valid and second one is expired
    // The first fetch has to be successful, and the second one should fail
    await result.fetchNext({ limit: 10000 });

    try {
      await result.fetchNext({ limit: 10000 });
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error.message).to.contain('CloudFetch link has expired');
      // it should use retry policy for all requests
      expect(context.connectionProvider.getRetryPolicy.called).to.be.true;
      // Row set contains a one valid and one expired link; only valid link should be requested
      expect(context.fetchHandler.callCount).to.be.equal(1);
    }
  });
});
