import { Buffer } from 'buffer';
import fetch, { RequestInfo, RequestInit } from 'node-fetch';
import { TRowSet, TSparkArrowResultLink } from '../../thrift/TCLIService_types';
import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';

export default class CloudFetchResultHandler implements IResultsProvider<Array<Buffer>> {
  protected readonly context: IClientContext;

  private readonly source: IResultsProvider<TRowSet | undefined>;

  private pendingLinks: Array<TSparkArrowResultLink> = [];

  private downloadedBatches: Array<Buffer> = [];

  constructor(context: IClientContext, source: IResultsProvider<TRowSet | undefined>) {
    this.context = context;
    this.source = source;
  }

  public async hasMore() {
    if (this.pendingLinks.length > 0 || this.downloadedBatches.length > 0) {
      return true;
    }
    return this.source.hasMore();
  }

  public async fetchNext(options: ResultsProviderFetchNextOptions) {
    const data = await this.source.fetchNext(options);

    data?.resultLinks?.forEach((link) => {
      this.pendingLinks.push(link);
    });

    if (this.downloadedBatches.length === 0) {
      const clientConfig = this.context.getConfig();
      const links = this.pendingLinks.splice(0, clientConfig.cloudFetchConcurrentDownloads);
      const tasks = links.map((link) => this.downloadLink(link));
      const batches = await Promise.all(tasks);
      this.downloadedBatches.push(...batches);
    }

    return this.downloadedBatches.splice(0, 1);
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

    return fetch(url, {
      agent,
      ...init,
    });
  }
}
