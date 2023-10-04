const { expect, AssertionError } = require('chai');
const sinon = require('sinon');
const Int64 = require('node-int64');
const CloudFetchResult = require('../../../dist/result/CloudFetchResult').default;
const globalConfig = require('../../../dist/globalConfig').default;

const sampleThriftSchema = {
  columns: [
    {
      columnName: '1',
      typeDesc: {
        types: [
          {
            primitiveEntry: {
              type: 3,
              typeQualifiers: null,
            },
          },
        ],
      },
      position: 1,
    },
  ],
};

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
    },
    {
      fileLink: 'http://example.com/result/2',
      expiryTime: new Int64(defaultLinkExpiryTime),
    },
  ],
};

const sampleRowSet2 = {
  startRowOffset: 0,
  resultLinks: [
    {
      fileLink: 'http://example.com/result/3',
      expiryTime: new Int64(defaultLinkExpiryTime),
    },
    {
      fileLink: 'http://example.com/result/4',
      expiryTime: new Int64(defaultLinkExpiryTime),
    },
    {
      fileLink: 'http://example.com/result/5',
      expiryTime: new Int64(defaultLinkExpiryTime),
    },
  ],
};

const sampleEmptyRowSet = {
  startRowOffset: 0,
  resultLinks: undefined,
};

const sampleExpiredRowSet = {
  startRowOffset: 0,
  resultLinks: [
    {
      fileLink: 'http://example.com/result/6',
      expiryTime: new Int64(defaultLinkExpiryTime),
    },
    {
      fileLink: 'http://example.com/result/7',
      expiryTime: new Int64(Date.now() - 24 * 60 * 60 * 1000), // 24hr in past
    },
  ],
};

describe('CloudFetchResult', () => {
  let savedConcurrentDownloads;

  beforeEach(() => {
    savedConcurrentDownloads = globalConfig.cloudFetchConcurrentDownloads;
  });

  afterEach(() => {
    globalConfig.cloudFetchConcurrentDownloads = savedConcurrentDownloads;
  });

  it('should report pending data if there are any', async () => {
    const context = {};
    const result = new CloudFetchResult({}, sampleThriftSchema, sampleArrowSchema);

    case1: {
      result.pendingLinks = [];
      result.downloadedBatches = [];
      expect(await result.hasPendingData()).to.be.false;
    }

    case2: {
      result.pendingLinks = [{}]; // just anything here
      result.downloadedBatches = [];
      expect(await result.hasPendingData()).to.be.true;
    }

    case3: {
      result.pendingLinks = [];
      result.downloadedBatches = [{}]; // just anything here
      expect(await result.hasPendingData()).to.be.true;
    }
  });

  it('should extract links from row sets', async () => {
    globalConfig.cloudFetchConcurrentDownloads = 0; // this will prevent it from downloading batches

    const context = {};

    const result = new CloudFetchResult({}, sampleThriftSchema, sampleArrowSchema);

    sinon.stub(result, 'fetch').returns(
      Promise.resolve({
        ok: true,
        status: 200,
        statusText: 'OK',
        arrayBuffer: async () => sampleArrowBatch,
      }),
    );

    const rowSets = [sampleRowSet1, sampleEmptyRowSet, sampleRowSet2];
    const expectedLinksCount = rowSets.reduce((prev, item) => prev + (item.resultLinks?.length ?? 0), 0);

    const batches = await result.getBatches(rowSets);
    expect(batches.length).to.be.equal(0);
    expect(result.fetch.called).to.be.false;
    expect(result.pendingLinks.length).to.be.equal(expectedLinksCount);
  });

  it('should download batches according to settings', async () => {
    globalConfig.cloudFetchConcurrentDownloads = 2;

    const context = {};

    const result = new CloudFetchResult({}, sampleThriftSchema, sampleArrowSchema);

    sinon.stub(result, 'fetch').returns(
      Promise.resolve({
        ok: true,
        status: 200,
        statusText: 'OK',
        arrayBuffer: async () => sampleArrowBatch,
      }),
    );

    const rowSets = [sampleRowSet1, sampleRowSet2];
    const expectedLinksCount = rowSets.reduce((prev, item) => prev + (item.resultLinks?.length ?? 0), 0);

    initialFetch: {
      const batches = await result.getBatches(rowSets);
      expect(batches.length).to.be.equal(1);
      expect(result.fetch.callCount).to.be.equal(globalConfig.cloudFetchConcurrentDownloads);
      expect(result.pendingLinks.length).to.be.equal(expectedLinksCount - globalConfig.cloudFetchConcurrentDownloads);
      expect(result.downloadedBatches.length).to.be.equal(globalConfig.cloudFetchConcurrentDownloads - 1);
    }

    secondFetch: {
      // It should return previously fetched batch, not performing additional network requests
      const batches = await result.getBatches([]);
      expect(batches.length).to.be.equal(1);
      expect(result.fetch.callCount).to.be.equal(globalConfig.cloudFetchConcurrentDownloads); // no new fetches
      expect(result.pendingLinks.length).to.be.equal(expectedLinksCount - globalConfig.cloudFetchConcurrentDownloads);
      expect(result.downloadedBatches.length).to.be.equal(globalConfig.cloudFetchConcurrentDownloads - 2);
    }

    thirdFetch: {
      // Now buffer should be empty, and it should fetch next batches
      const batches = await result.getBatches([]);
      expect(batches.length).to.be.equal(1);
      expect(result.fetch.callCount).to.be.equal(globalConfig.cloudFetchConcurrentDownloads * 2);
      expect(result.pendingLinks.length).to.be.equal(
        expectedLinksCount - globalConfig.cloudFetchConcurrentDownloads * 2,
      );
      expect(result.downloadedBatches.length).to.be.equal(globalConfig.cloudFetchConcurrentDownloads - 1);
    }
  });

  it('should handle HTTP errors', async () => {
    globalConfig.cloudFetchConcurrentDownloads = 1;

    const context = {};

    const result = new CloudFetchResult({}, sampleThriftSchema, sampleArrowSchema);

    sinon.stub(result, 'fetch').returns(
      Promise.resolve({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        arrayBuffer: async () => sampleArrowBatch,
      }),
    );

    const rowSets = [sampleRowSet1];

    try {
      await result.getBatches(rowSets);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error.message).to.contain('Internal Server Error');
      expect(result.fetch.callCount).to.be.equal(1);
    }
  });

  it('should handle expired links', async () => {
    const context = {};

    const result = new CloudFetchResult(context, sampleThriftSchema, sampleArrowSchema);

    sinon.stub(result, 'fetch').returns(
      Promise.resolve({
        ok: true,
        status: 200,
        statusText: 'OK',
        arrayBuffer: async () => sampleArrowBatch,
      }),
    );

    const rowSets = [sampleExpiredRowSet];

    try {
      await result.getBatches(rowSets);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error.message).to.contain('CloudFetch link has expired');
      // Row set contains a one valid and one expired link; only valid link should be requested
      expect(result.fetch.callCount).to.be.equal(1);
    }
  });
});
