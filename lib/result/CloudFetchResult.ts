import { Buffer } from 'buffer';
import fetch from 'node-fetch';
import { TRowSet, TSparkArrowResultLink } from '../../thrift/TCLIService_types';
import ArrowResult from './ArrowResult';

export default class CloudFetchResult extends ArrowResult {
  protected batchesToRows(batches: Array<Buffer>) {
    if (batches.length === 1) {
      return super.batchesToRows(batches);
    }

    const results: Array<Array<any>> = [];

    for (const batch of batches) {
      results.push(super.batchesToRows([batch]));
    }

    return results.flat(1);
  }

  protected async getBatches(data: Array<TRowSet>): Promise<Array<Buffer>> {
    const tasks: Array<Promise<Buffer>> = [];

    data?.forEach((item) => {
      item.resultLinks?.forEach((link) => {
        tasks.push(this.downloadLink(link));
      });
    });

    return Promise.all(tasks);
  }

  private async downloadLink(link: TSparkArrowResultLink): Promise<Buffer> {
    // TODO: Process expired links
    const response = await fetch(link.fileLink);
    if (!response.ok) {
      throw new Error(`CloudFetch HTTP error ${response.status} ${response.statusText}`);
    }

    const result = await response.arrayBuffer();
    return Buffer.from(result);
  }
}
