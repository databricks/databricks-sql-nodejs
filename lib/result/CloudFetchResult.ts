import { Buffer } from 'buffer';
import fetch from 'node-fetch';
import { TRowSet, TSparkArrowResultLink, TTableSchema } from '../../thrift/TCLIService_types';
import ArrowResult from './ArrowResult';
import globalConfig from '../globalConfig';

export default class CloudFetchResult extends ArrowResult {
  private pendingLinks: Array<TSparkArrowResultLink> = [];

  private downloadedBatches: Array<Buffer> = [];

  constructor(schema?: TTableSchema, arrowSchema?: Buffer) {
    // Arrow schema returned in metadata is not needed for CloudFetch results:
    // each batch already contains schema and could be decoded as is
    super(schema, Buffer.alloc(0));
  }

  async hasPendingData() {
    return this.pendingLinks.length > 0 || this.downloadedBatches.length > 0;
  }

  protected async getBatches(data: Array<TRowSet>): Promise<Array<Buffer>> {
    data?.forEach((item) => {
      item.resultLinks?.forEach((link) => {
        this.pendingLinks.push(link);
      });
    });

    if (this.downloadedBatches.length === 0) {
      const links = this.pendingLinks.splice(0, globalConfig.cloudFetchConcurrentDownloads);
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

    const response = await fetch(link.fileLink);
    if (!response.ok) {
      throw new Error(`CloudFetch HTTP error ${response.status} ${response.statusText}`);
    }

    const result = await response.arrayBuffer();
    return Buffer.from(result);
  }
}
