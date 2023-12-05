const { expect } = require('chai');
const sinon = require('sinon');
const config = require('./utils/config');
const { DBSQLClient } = require('../..');
const CloudFetchResultHandler = require('../../dist/result/CloudFetchResultHandler').default;
const ArrowResultConverter = require('../../dist/result/ArrowResultConverter').default;
const ResultSlicer = require('../../dist/result/ResultSlicer').default;

async function openSession(customConfig) {
  const client = new DBSQLClient();

  const clientConfig = client.getConfig();
  sinon.stub(client, 'getConfig').returns({
    ...clientConfig,
    ...customConfig,
  });

  const connection = await client.connect({
    host: config.host,
    path: config.path,
    token: config.token,
  });

  return connection.openSession({
    initialCatalog: config.database[0],
    initialSchema: config.database[1],
  });
}

// This suite takes a while to execute, and in this case it's expected.
// If one day it starts to fail with timeouts - you may consider to just increase timeout for it
describe('CloudFetch', () => {
  it('should fetch data', async () => {
    const cloudFetchConcurrentDownloads = 5;
    const session = await openSession({ cloudFetchConcurrentDownloads });

    const queriedRowsCount = 10000000; // result has to be quite big to enable CloudFetch
    const operation = await session.executeStatement(
      `
        SELECT *
        FROM range(0, ${queriedRowsCount}) AS t1
        LEFT JOIN (SELECT 1) AS t2
      `,
      {
        maxRows: null, // disable DirectResults
        useCloudFetch: true, // tell server that we would like to use CloudFetch
      },
    );

    // We're going to examine some internals of operation, so explicitly wait for completion
    await operation.finished();

    // Check if we're actually getting data via CloudFetch
    const resultHandler = await operation.getResultHandler();
    expect(resultHandler).to.be.instanceof(ResultSlicer);
    expect(resultHandler.source).to.be.instanceof(ArrowResultConverter);
    expect(resultHandler.source.source).to.be.instanceOf(CloudFetchResultHandler);

    const cfResultHandler = resultHandler.source.source;

    // Fetch first chunk and check if result handler behaves properly.
    // With the count of rows we queried, there should be at least one row set,
    // containing 8 result links. After fetching the first chunk,
    // result handler should download 5 of them and schedule the rest
    expect(await cfResultHandler.hasMore()).to.be.true;
    expect(cfResultHandler.pendingLinks.length).to.be.equal(0);
    expect(cfResultHandler.downloadedBatches.length).to.be.equal(0);

    sinon.spy(operation._data, 'fetchNext');

    const chunk = await operation.fetchChunk({ maxRows: 100000, disableBuffering: true });
    // Count links returned from server
    const resultSet = await operation._data.fetchNext.firstCall.returnValue;
    const resultLinksCount = resultSet?.resultLinks?.length ?? 0;

    expect(await cfResultHandler.hasMore()).to.be.true;
    // expected batches minus first 5 already fetched
    expect(cfResultHandler.pendingLinks.length).to.be.equal(resultLinksCount - cloudFetchConcurrentDownloads);
    expect(cfResultHandler.downloadedBatches.length).to.be.equal(cloudFetchConcurrentDownloads - 1);

    let fetchedRowCount = chunk.length;
    while (await operation.hasMoreRows()) {
      const chunk = await operation.fetchChunk({ maxRows: 100000, disableBuffering: true });
      fetchedRowCount += chunk.length;
    }

    expect(fetchedRowCount).to.be.equal(queriedRowsCount);
  });
});
