import { expect } from 'chai';
import sinon from 'sinon';
import http from 'http';
import { AddressInfo } from 'net';
import { DBSQLClient } from '../../lib';
import { ClientConfig } from '../../lib/contracts/IClientContext';
import CloudFetchResultHandler from '../../lib/result/CloudFetchResultHandler';
import ArrowResultConverter from '../../lib/result/ArrowResultConverter';
import ResultSlicer from '../../lib/result/ResultSlicer';
import RetryError, { RetryErrorCode } from '../../lib/errors/RetryError';

import config from './utils/config';

async function openSession(customConfig: Partial<ClientConfig> = {}) {
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
    initialCatalog: config.catalog,
    initialSchema: config.schema,
  });
}

// This suite takes a while to execute, and in this case it's expected.
// If one day it starts to fail with timeouts - you may consider to just increase timeout for it
describe('CloudFetch', () => {
  it('should fetch data', async () => {
    const cloudFetchConcurrentDownloads = 5;
    const session = await openSession({
      cloudFetchConcurrentDownloads,
      useLZ4Compression: false,
    });

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
    // @ts-expect-error TS2339: Property getResultHandler does not exist on type IOperation
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
    expect(cfResultHandler.downloadTasks.length).to.be.equal(0);

    // @ts-expect-error TS2339: Property _data does not exist on type IOperation
    sinon.spy(operation._data, 'fetchNext');

    const chunk = await operation.fetchChunk({ maxRows: 100000, disableBuffering: true });
    // Count links returned from server
    // @ts-expect-error TS2339: Property _data does not exist on type IOperation
    const resultSet = await operation._data.fetchNext.firstCall.returnValue;
    const resultLinksCount = resultSet?.resultLinks?.length ?? 0;

    expect(await cfResultHandler.hasMore()).to.be.true;
    // expected batches minus first 5 already fetched
    expect(cfResultHandler.pendingLinks.length).to.be.equal(resultLinksCount - cloudFetchConcurrentDownloads);
    expect(cfResultHandler.downloadTasks.length).to.be.equal(cloudFetchConcurrentDownloads - 1);

    let fetchedRowCount = chunk.length;
    // eslint-disable-next-line no-await-in-loop
    while (await operation.hasMoreRows()) {
      // eslint-disable-next-line no-await-in-loop
      const ch = await operation.fetchChunk({ maxRows: 100000, disableBuffering: true });
      fetchedRowCount += ch.length;
    }

    expect(fetchedRowCount).to.be.equal(queriedRowsCount);
  });

  it('should not use LZ4 compression with cloud fetch', async () => {
    const cloudFetchConcurrentDownloads = 5;
    const session = await openSession({
      cloudFetchConcurrentDownloads,
      useLZ4Compression: true, // This is ignored when cloud fetch is enabled
    });

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
    // @ts-expect-error TS2339: Property getResultHandler does not exist on type IOperation
    const resultHandler = await operation.getResultHandler();
    expect(resultHandler).to.be.instanceof(ResultSlicer);
    expect(resultHandler.source).to.be.instanceof(ArrowResultConverter);
    expect(resultHandler.source.source).to.be.instanceOf(CloudFetchResultHandler);
    // LZ4 compression should not be enabled with cloud fetch
    expect(resultHandler.source.source.isLZ4Compressed).to.be.false;

    const chunk = await operation.fetchChunk({ maxRows: 100000, disableBuffering: true });
    expect(chunk.length).to.be.gt(0);
  });

  it('should surface HTTP status and download context when CloudFetch retries are exhausted', async () => {
    const retryMaxAttempts = 2;
    const session = await openSession({
      cloudFetchConcurrentDownloads: 1,
      useLZ4Compression: false,
      retryMaxAttempts,
      retryDelayMin: 1,
      retryDelayMax: 10,
    });

    let serverHits = 0;
    const server = http.createServer((req, res) => {
      serverHits += 1;
      res.writeHead(500, 'Internal Server Error', { 'Content-Type': 'text/plain' });
      res.end('Internal Server Error');
    });
    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', () => resolve());
    });
    const { port } = server.address() as AddressInfo;

    try {
      const queriedRowsCount = 10000000; // result has to be quite big to enable CloudFetch
      const operation = await session.executeStatement(
        `
          SELECT *
          FROM range(0, ${queriedRowsCount}) AS t1
          LEFT JOIN (SELECT 1) AS t2
        `,
        {
          maxRows: null, // disable DirectResults
          useCloudFetch: true,
        },
      );

      await operation.finished();

      // @ts-expect-error TS2339: Property getResultHandler does not exist on type IOperation
      const resultHandler = await operation.getResultHandler();
      expect(resultHandler.source.source).to.be.instanceOf(CloudFetchResultHandler);
      const cfResultHandler = resultHandler.source.source;

      // Plain-HTTP local server: let node-fetch pick its default agent for the download.
      // The stub is scoped to the `fetchChunk` call below and restored right after it, so
      // later requests on this connection provider (e.g. `session.close()`) keep the real agent
      // (proxy / custom CA / keep-alive config)
      const connectionProvider = await cfResultHandler.context.getConnectionProvider();
      const getAgentStub = sinon.stub(connectionProvider, 'getAgent').resolves(undefined);

      // Redirect every presigned link to the local server that always responds with HTTP 500,
      // keeping the real expiry time and row count returned by the warehouse
      const rowSetProvider = cfResultHandler.source;
      const originalFetchNext = rowSetProvider.fetchNext.bind(rowSetProvider);
      const originalFileLinks: Array<string> = [];
      sinon.stub(rowSetProvider, 'fetchNext').callsFake(async (...args: Array<any>) => {
        const rowSet = await originalFetchNext(...args);
        rowSet?.resultLinks?.forEach((link: any) => {
          originalFileLinks.push(link.fileLink);
          link.fileLink = `http://127.0.0.1:${port}/result`;
          link.httpHeaders = {};
        });
        return rowSet;
      });

      let thrown: unknown;
      try {
        await operation.fetchChunk({ maxRows: 100000, disableBuffering: true });
      } catch (error) {
        thrown = error;
      } finally {
        getAgentStub.restore();
      }

      // Pin the retry-exhaustion path (not the plain `CloudFetch HTTP error ...` thrown by `downloadLink`)
      expect(thrown).to.be.instanceOf(RetryError);
      expect((thrown as RetryError).errorCode).to.be.equal(RetryErrorCode.AttemptsExceeded);
      expect(serverHits).to.be.equal(retryMaxAttempts);
      const { message } = thrown as RetryError;
      expect(message).to.contain('CloudFetch result download failed');
      expect(message).to.contain('Max retry count exceeded');
      expect(message).to.contain('HTTP 500: Internal Server Error');

      // The real presigned link (host and credentialed query string) must not leak into the message
      expect(originalFileLinks.length).to.be.gt(0);
      originalFileLinks.forEach((fileLink) => {
        const { host, search } = new URL(fileLink);
        expect(message).to.not.contain(host);
        if (search) {
          expect(message).to.not.contain(search);
        }
      });
    } finally {
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
      await session.close();
    }
  });
});
