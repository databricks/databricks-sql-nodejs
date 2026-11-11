import { expect, AssertionError } from 'chai';
import sinon, { SinonStub } from 'sinon';
import Int64 from 'node-int64';
import { compressFrameSync } from 'lz4-napi';
import { Request, Response } from 'node-fetch';
import { ShouldRetryResult } from '../../../lib/connection/contracts/IRetryPolicy';
import { HttpTransactionDetails } from '../../../lib/connection/contracts/IConnectionProvider';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import CloudFetchResultHandler from '../../../lib/result/CloudFetchResultHandler';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';
import { TRowSet, TSparkArrowResultLink, TStatusCode } from '../../../thrift/TCLIService_types';
import { ArrowBatch } from '../../../lib/result/utils';
import RetryError, { RetryErrorCode } from '../../../lib/errors/RetryError';

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
      shouldRetry: async (): Promise<ShouldRetryResult> => {
        return { shouldRetry: false };
      },
      invokeWithRetry: async (): Promise<HttpTransactionDetails> => {
        return this.invokeWithRetryStub();
      },
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

    case1: {
      result['pendingLinks'] = [];
      result['downloadTasks'] = [];
      expect(await result.hasMore()).to.be.false;
    }

    case2: {
      result['pendingLinks'] = [
        {
          fileLink: '',
          expiryTime: new Int64(0),
          startRowOffset: new Int64(0),
          rowCount: new Int64(0),
          bytesNum: new Int64(0),
        },
      ];
      result['downloadTasks'] = [];
      expect(await result.hasMore()).to.be.true;
    }

    case3: {
      result['pendingLinks'] = [];
      result['downloadTasks'] = [
        Promise.resolve({
          ok: true,
          batch: {
            batches: [],
            rowCount: 0,
          },
        }),
      ];
      expect(await result.hasMore()).to.be.true;
    }
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

    do {
      await result.fetchNext({ limit: 100000 });
    } while (await rowSetProvider.hasMore());

    expect(result['pendingLinks'].length).to.be.equal(expectedLinksCount);
    expect(result['downloadTasks'].length).to.be.equal(0);
    expect(context.invokeWithRetryStub.called).to.be.false;
  });

  it('should log cloud fetch download speed at debug level', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);
    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    const logStub = sinon.stub(context.logger, 'log');
    context.invokeWithRetryStub.callsFake(async () => ({
      request: new Request('localhost'),
      response: new Response(Buffer.concat([sampleArrowSchema, sampleArrowBatch]), { status: 200 }),
    }));

    await result.fetchNext({ limit: 10000 });

    expect(logStub.calledWith(LogLevel.debug, sinon.match(/Result File Download speed from cloud storage/))).to.be.true;
    expect(logStub.neverCalledWith(LogLevel.info, sinon.match(/Result File Download speed from cloud storage/))).to.be
      .true;
  });

  it('should download batches according to settings', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 3 });
    const clientConfig = context.getConfig();

    const rowSet: TRowSet = {
      startRowOffset: new Int64(0),
      rows: [],
      resultLinks: [...(sampleRowSet1.resultLinks ?? []), ...(sampleRowSet2.resultLinks ?? [])],
    };
    const expectedLinksCount = rowSet.resultLinks?.length ?? 0; // 5
    const rowSetProvider = new ResultsProviderStub([rowSet], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    context.invokeWithRetryStub.callsFake(async () => ({
      request: new Request('localhost'),
      response: new Response(Buffer.concat([sampleArrowSchema, sampleArrowBatch]), { status: 200 }),
    }));

    expect(await rowSetProvider.hasMore()).to.be.true;

    initialFetch: {
      // `cloudFetchConcurrentDownloads` out of `expectedLinksCount` links should be scheduled immediately
      // first one should be `await`-ed and returned from `fetchNext`
      const { batches } = await result.fetchNext({ limit: 10000 });
      expect(batches.length).to.be.gt(0);
      expect(await rowSetProvider.hasMore()).to.be.false;

      // it should use retry policy for all requests
      expect((context.connectionProvider.getRetryPolicy as SinonStub).called).to.be.true;
      expect(context.invokeWithRetryStub.callCount).to.be.equal(clientConfig.cloudFetchConcurrentDownloads);
      expect(result['pendingLinks'].length).to.be.equal(
        expectedLinksCount - clientConfig.cloudFetchConcurrentDownloads,
      );
      expect(result['downloadTasks'].length).to.be.equal(clientConfig.cloudFetchConcurrentDownloads - 1);
    }

    secondFetch: {
      // It should return previously fetched batch, and schedule one more
      const { batches } = await result.fetchNext({ limit: 10000 });
      expect(batches.length).to.be.gt(0);
      expect(await rowSetProvider.hasMore()).to.be.false;

      // it should use retry policy for all requests
      expect((context.connectionProvider.getRetryPolicy as SinonStub).called).to.be.true;
      expect(context.invokeWithRetryStub.callCount).to.be.equal(clientConfig.cloudFetchConcurrentDownloads + 1);
      expect(result['pendingLinks'].length).to.be.equal(
        expectedLinksCount - clientConfig.cloudFetchConcurrentDownloads - 1,
      );
      expect(result['downloadTasks'].length).to.be.equal(clientConfig.cloudFetchConcurrentDownloads - 1);
    }

    thirdFetch: {
      // Now buffer should be empty, and it should fetch next batches
      const { batches } = await result.fetchNext({ limit: 10000 });
      expect(batches.length).to.be.gt(0);
      expect(await rowSetProvider.hasMore()).to.be.false;

      // it should use retry policy for all requests
      expect((context.connectionProvider.getRetryPolicy as SinonStub).called).to.be.true;
      expect(context.invokeWithRetryStub.callCount).to.be.equal(clientConfig.cloudFetchConcurrentDownloads + 2);
      expect(result['pendingLinks'].length).to.be.equal(
        expectedLinksCount - clientConfig.cloudFetchConcurrentDownloads - 2,
      );
      expect(result['downloadTasks'].length).to.be.equal(clientConfig.cloudFetchConcurrentDownloads - 1);
    }
  });

  it('should not leak unhandled rejections from prefetched downloads when a download fails', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 3 });
    const clientConfig = context.getConfig();

    const rowSet: TRowSet = {
      startRowOffset: new Int64(0),
      rows: [],
      resultLinks: [...(sampleRowSet1.resultLinks ?? []), ...(sampleRowSet2.resultLinks ?? [])],
    };
    const rowSetProvider = new ResultsProviderStub([rowSet], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    // Every download fails like a real CloudFetch ECONNRESET / socket hang up.
    context.invokeWithRetryStub.callsFake(async () => {
      throw Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });
    });

    // `cloudFetchConcurrentDownloads` downloads are scheduled at once; the head is awaited and
    // surfaces the error, so `fetchNext` rejects.
    let thrown: any;
    try {
      await result.fetchNext({ limit: 10000 });
    } catch (e) {
      thrown = e;
    }
    expect(thrown?.code).to.be.equal('ECONNRESET');

    // The consumer typically stops iterating once `fetchNext` throws. The remaining prefetched
    // downloads must NOT reject (which would surface as `unhandledRejection`) — they should be
    // reflected into a settled value with the failure captured.
    const remaining = result['downloadTasks'];
    expect(remaining.length).to.be.equal(clientConfig.cloudFetchConcurrentDownloads - 1);
    const settled = await Promise.allSettled(remaining);
    for (const outcome of settled) {
      expect(outcome.status).to.be.equal('fulfilled');
      expect((outcome as PromiseFulfilledResult<any>).value.ok).to.be.equal(false);
    }
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
    expect(rowCount).to.equal(1);
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
      response: new Response(compressFrameSync(expectedBatch), { status: 200 }),
    }));

    expect(await rowSetProvider.hasMore()).to.be.true;

    const { batches } = await result.fetchNext({ limit: 10000 });
    expect(await rowSetProvider.hasMore()).to.be.false;

    // it should use retry policy for all requests
    expect((context.connectionProvider.getRetryPolicy as SinonStub).called).to.be.true;
    expect(context.invokeWithRetryStub.called).to.be.true;
    expect(batches).to.deep.eq([expectedBatch]);
  });

  it('should handle HTTP errors', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });

    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    context.invokeWithRetryStub.callsFake(async () => ({
      request: new Request('localhost'),
      response: new Response(Buffer.concat([sampleArrowSchema, sampleArrowBatch]), { status: 500 }),
    }));

    try {
      await result.fetchNext({ limit: 10000 });
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error.message).to.contain('Internal Server Error');
      // it should use retry policy for all requests
      expect((context.connectionProvider.getRetryPolicy as SinonStub).called).to.be.true;
      expect(context.invokeWithRetryStub.callCount).to.be.equal(1);
    }
  });

  it('should add download context and HTTP status to RetryError when retries are exhausted', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    context.invokeWithRetryStub.callsFake(async () => {
      throw new RetryError(RetryErrorCode.AttemptsExceeded, {
        request: new Request('http://example.com/result/1'),
        response: new Response('', { status: 500, statusText: 'Internal Server Error' }),
      });
    });

    let thrown: any;
    try {
      await result.fetchNext({ limit: 10000 });
    } catch (e) {
      thrown = e;
    }

    expect(thrown).to.be.instanceOf(RetryError);
    expect(thrown.errorCode).to.be.equal(RetryErrorCode.AttemptsExceeded);
    expect(thrown.message).to.contain('CloudFetch');
    expect(thrown.message).to.contain('HTTP 500');
    expect(thrown.message).to.contain('Internal Server Error');
    expect(thrown.message).to.contain('Max retry count exceeded');
    expect(thrown.stack.split('\n')[0]).to.be.equal(`Error: ${thrown.message}`);
    expect(thrown.stack).to.not.contain('describeDownloadError');
  });

  it('should add download context and HTTP status to RetryError when retry timeout is exceeded', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    context.invokeWithRetryStub.callsFake(async () => {
      throw new RetryError(RetryErrorCode.TimeoutExceeded, {
        request: new Request('http://example.com/result/1'),
        response: new Response('', { status: 503, statusText: 'Service Unavailable' }),
      });
    });

    let thrown: any;
    try {
      await result.fetchNext({ limit: 10000 });
    } catch (e) {
      thrown = e;
    }

    expect(thrown).to.be.instanceOf(RetryError);
    expect(thrown.errorCode).to.be.equal(RetryErrorCode.TimeoutExceeded);
    expect(thrown.message).to.be.equal(
      'CloudFetch result download failed: Retry timeout exceeded (last error: HTTP 503: Service Unavailable)',
    );
    expect(thrown.stack.split('\n')[0]).to.be.equal(`Error: ${thrown.message}`);
  });

  it('should add download context and network error cause to RetryError when retries are exhausted', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    const originalError = new RetryError(
      RetryErrorCode.AttemptsExceeded,
      Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }),
    );
    context.invokeWithRetryStub.callsFake(async () => {
      throw originalError;
    });

    let thrown: any;
    try {
      await result.fetchNext({ limit: 10000 });
    } catch (e) {
      thrown = e;
    }

    expect(thrown).to.be.equal(originalError);
    expect(thrown.stack.split('\n')[0]).to.be.equal(`Error: ${thrown.message}`);
    expect(thrown).to.be.instanceOf(RetryError);
    expect(thrown.errorCode).to.be.equal(RetryErrorCode.AttemptsExceeded);
    expect(thrown.message).to.be.equal(
      'CloudFetch result download failed: Max retry count exceeded (last error: network error ECONNRESET)',
    );
  });

  it('should not prefix the same RetryError instance twice when it is re-thrown', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });

    const originalError = new RetryError(
      RetryErrorCode.AttemptsExceeded,
      Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }),
    );
    context.invokeWithRetryStub.callsFake(async () => {
      throw originalError;
    });

    const expectedMessage =
      'CloudFetch result download failed: Max retry count exceeded (last error: network error ECONNRESET)';

    for (let i = 0; i < 2; i += 1) {
      const result = new CloudFetchResultHandler(context, new ResultsProviderStub([sampleRowSet1], undefined), {
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
      });

      let thrown: any;
      try {
        // eslint-disable-next-line no-await-in-loop
        await result.fetchNext({ limit: 10000 });
      } catch (e) {
        thrown = e;
      }

      expect(thrown).to.be.equal(originalError);
      expect(thrown.message).to.be.equal(expectedMessage);
      expect(thrown.stack.split('\n')[0]).to.be.equal(`Error: ${expectedMessage}`);
    }
  });

  it('should not leak presigned URLs from network errors into the RetryError message', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    const presignedUrl = 'https://storage.example.com/results/chunk-0?sig=SECRET_SIGNATURE&se=2026-11-06';
    const originalError = new RetryError(
      RetryErrorCode.AttemptsExceeded,
      Object.assign(new Error(`request to ${presignedUrl} failed, reason: socket hang up`), {
        code: 'ECONNRESET',
        type: 'system',
      }),
    );
    context.invokeWithRetryStub.callsFake(async () => {
      throw originalError;
    });

    let thrown: any;
    try {
      await result.fetchNext({ limit: 10000 });
    } catch (e) {
      thrown = e;
    }

    expect(thrown).to.be.equal(originalError);
    expect(thrown.message).to.contain('ECONNRESET');
    expect(thrown.message).to.not.contain('SECRET_SIGNATURE');
    expect(thrown.message).to.not.contain('storage.example.com');
    expect(thrown.stack.split('\n')[0]).to.not.contain('SECRET_SIGNATURE');
  });

  it('should use the network error type when no code is available', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    const originalError = new RetryError(
      RetryErrorCode.AttemptsExceeded,
      Object.assign(new Error('request to https://storage.example.com/x?sig=SECRET failed'), { type: 'aborted' }),
    );
    context.invokeWithRetryStub.callsFake(async () => {
      throw originalError;
    });

    let thrown: any;
    try {
      await result.fetchNext({ limit: 10000 });
    } catch (e) {
      thrown = e;
    }

    expect(thrown).to.be.equal(originalError);
    expect(thrown.message).to.be.equal(
      'CloudFetch result download failed: Max retry count exceeded (last error: network error (aborted))',
    );
  });

  it('should preserve the original node-fetch request/response payload of a RetryError', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    const presignedUrl = 'https://storage.example.com/results/chunk-0?sig=SECRET_SIGNATURE&se=2026-11-06';
    const request = new Request(presignedUrl);
    const response = new Response('', { status: 503, statusText: 'Service Unavailable', url: presignedUrl } as any);
    const payload = { request, response };
    const originalError = new RetryError(RetryErrorCode.AttemptsExceeded, payload);
    context.invokeWithRetryStub.callsFake(async () => {
      throw originalError;
    });

    let thrown: any;
    try {
      await result.fetchNext({ limit: 10000 });
    } catch (e) {
      thrown = e;
    }

    expect(thrown).to.be.equal(originalError);
    expect(thrown).to.be.instanceOf(RetryError);
    expect(thrown.errorCode).to.be.equal(RetryErrorCode.AttemptsExceeded);
    expect(thrown.message).to.contain('HTTP 503: Service Unavailable');
    expect(thrown.message).to.not.contain('SECRET_SIGNATURE');
    expect(thrown.payload).to.be.equal(payload);
    expect(thrown.payload.request).to.be.equal(request);
    expect(thrown.payload.response).to.be.equal(response);
    expect(thrown.payload.response).to.be.instanceOf(Response);
  });

  it('should re-throw errors other than RetryError unchanged', async () => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 1 });
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    const originalError = Object.assign(new Error('unable to verify the first certificate'), {
      code: 'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
    });
    const originalMessage = originalError.message;
    const originalStack = originalError.stack;
    context.invokeWithRetryStub.callsFake(async () => {
      throw originalError;
    });

    let thrown: any;
    try {
      await result.fetchNext({ limit: 10000 });
    } catch (e) {
      thrown = e;
    }

    expect(thrown).to.be.equal(originalError);
    expect(thrown.code).to.be.equal('UNABLE_TO_VERIFY_LEAF_SIGNATURE');
    expect(thrown.message).to.be.equal(originalMessage);
    expect(thrown.stack).to.be.equal(originalStack);
  });

  it('should handle expired links', async () => {
    const context = new ClientContextStub();
    const rowSetProvider = new ResultsProviderStub([sampleExpiredRowSet], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    context.invokeWithRetryStub.callsFake(async () => ({
      request: new Request('localhost'),
      response: new Response(Buffer.concat([sampleArrowSchema, sampleArrowBatch]), { status: 200 }),
    }));

    // There are two link in the batch - first one is valid and second one is expired
    // The first fetch has to be successful, and the second one should fail
    await result.fetchNext({ limit: 10000 });

    try {
      await result.fetchNext({ limit: 10000 });
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error.message).to.contain('CloudFetch link has expired');
      // it should use retry policy for all requests
      expect((context.connectionProvider.getRetryPolicy as SinonStub).called).to.be.true;
      // Row set contains a one valid and one expired link; only valid link should be requested
      expect(context.invokeWithRetryStub.callCount).to.be.equal(1);
    }
  });

  it('should handle data without LZ4 compression', async () => {
    const context = new ClientContextStub();
    const rowSetProvider = new ResultsProviderStub([sampleRowSet1], undefined);

    const result = new CloudFetchResultHandler(context, rowSetProvider, {
      lz4Compressed: false,
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });

    context.invokeWithRetryStub.callsFake(async () => ({
      request: new Request('localhost'),
      response: new Response(sampleArrowBatch, { status: 200 }), // Return only the batch
    }));

    const { batches } = await result.fetchNext({ limit: 10000 });

    // Ensure the batches array matches the expected structure
    expect(batches).to.deep.eq([sampleArrowBatch]);
  });
});
