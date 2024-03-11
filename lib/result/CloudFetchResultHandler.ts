import LZ4 from 'lz4';
import fetch, { RequestInfo, RequestInit } from 'node-fetch';
import { TGetResultSetMetadataResp, TRowSet, TSparkArrowResultLink } from '../../thrift/TCLIService_types';
import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';

export default class CloudFetchResultHandler implements IResultsProvider<Array<Buffer>> {
  protected readonly context: IClientContext;

  private readonly source: IResultsProvider<TRowSet | undefined>;

  private readonly isLZ4Compressed: boolean;

  private pendingLinks: Array<TSparkArrowResultLink> = [];

  private downloadTasks: Array<Promise<Buffer>> = [];

  constructor(
    context: IClientContext,
    source: IResultsProvider<TRowSet | undefined>,
    { lz4Compressed }: TGetResultSetMetadataResp,
  ) {
    this.context = context;
    this.source = source;
    this.isLZ4Compressed = lz4Compressed ?? false;
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

    const batch = await this.downloadTasks.shift();
    const batches = batch ? [batch] : [];

    if (this.isLZ4Compressed) {
      return batches.map((buffer) => LZ4.decode(buffer));
    }
    return batches;
  }

  private async downloadLink(link: TSparkArrowResultLink): Promise<Buffer> {
    if (Date.now() >= link.expiryTime.toNumber()) {
      throw new Error('CloudFetch link has expired');
    }

    const response = await this.fetch(link.fileLink);
    if (!response.ok) {
      throw new Error(`CloudFetch HTTP error ${response.status} ${response.statusText}`);
    }

    const result = await response.arrayBuffer();
    return Buffer.from(result);
  }

  private async fetch(url: RequestInfo, init?: RequestInit) {
    const connectionProvider = await this.context.getConnectionProvider();
    const agent = await connectionProvider.getAgent();
    const retryPolicy = await connectionProvider.getRetryPolicy();

    const requestConfig: RequestInit = { agent, ...init };
    return retryPolicy.invokeWithRetry(() => fetch(url, requestConfig));
  }
}
