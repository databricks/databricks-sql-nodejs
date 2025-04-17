import fetch, { RequestInfo, RequestInit, Request } from 'node-fetch';
import { TGetResultSetMetadataResp, TRowSet, TSparkArrowResultLink } from '../../thrift/TCLIService_types';
import HiveDriverError from '../errors/HiveDriverError';
import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';
import { ArrowBatch } from './utils';
import { LZ4 } from '../utils';

export default class CloudFetchResultHandler implements IResultsProvider<ArrowBatch> {
  private readonly context: IClientContext;

  private readonly source: IResultsProvider<TRowSet | undefined>;

  private readonly isLZ4Compressed: boolean;

  private pendingLinks: Array<TSparkArrowResultLink> = [];

  private downloadTasks: Array<Promise<ArrowBatch>> = [];

  constructor(
    context: IClientContext,
    source: IResultsProvider<TRowSet | undefined>,
    { lz4Compressed }: TGetResultSetMetadataResp,
  ) {
    this.context = context;
    this.source = source;
    this.isLZ4Compressed = lz4Compressed ?? false;

    if (this.isLZ4Compressed && !LZ4) {
      throw new HiveDriverError('Cannot handle LZ4 compressed result: module `lz4` not installed');
    }
  }

  public async hasMore() {
    if (this.pendingLinks.length > 0 || this.downloadTasks.length > 0) {
      return true;
    }
    return this.source.hasMore();
  }

  public async fetchNext(options: ResultsProviderFetchNextOptions) {
    const data = await this.source.fetchNext(options);

    data?.resultLinks?.forEach((link) => {
      this.pendingLinks.push(link);
    });

    const clientConfig = this.context.getConfig();
    const freeTaskSlotsCount = clientConfig.cloudFetchConcurrentDownloads - this.downloadTasks.length;

    if (freeTaskSlotsCount > 0) {
      const links = this.pendingLinks.splice(0, freeTaskSlotsCount);
      const tasks = links.map((link) => this.downloadLink(link));
      this.downloadTasks.push(...tasks);
    }

    // Process multiple batches in parallel
    const batches = await Promise.all(this.downloadTasks.splice(0, clientConfig.cloudFetchConcurrentDownloads));

    if (batches.length === 0) {
      return {
        batches: [],
        rowCount: 0,
      };
    }

    // Combine all batches
    const combinedBatches = batches.reduce(
      (acc, batch) => {
        if (this.isLZ4Compressed) {
          batch.batches = batch.batches.map((buffer) => LZ4!.decode(buffer));
        }
        acc.batches.push(...batch.batches);
        acc.rowCount += batch.rowCount;
        return acc;
      },
      { batches: [] as Buffer[], rowCount: 0 },
    );

    return combinedBatches;
  }

  private async downloadLink(link: TSparkArrowResultLink): Promise<ArrowBatch> {
    if (Date.now() >= link.expiryTime.toNumber()) {
      throw new Error('CloudFetch link has expired');
    }

    const response = await this.fetch(link.fileLink, { headers: link.httpHeaders });
    if (!response.ok) {
      throw new Error(`CloudFetch HTTP error ${response.status} ${response.statusText}`);
    }

    const result = await response.arrayBuffer();
    return {
      batches: [Buffer.from(result)],
      rowCount: link.rowCount.toNumber(true),
    };
  }

  private async fetch(url: RequestInfo, init?: RequestInit) {
    const connectionProvider = await this.context.getConnectionProvider();
    const agent = await connectionProvider.getAgent();
    const retryPolicy = await connectionProvider.getRetryPolicy();

    const requestConfig: RequestInit = { agent, ...init };
    const result = await retryPolicy.invokeWithRetry(() => {
      const request = new Request(url, requestConfig);
      return fetch(request).then((response) => ({ request, response }));
    });
    return result.response;
  }
}
