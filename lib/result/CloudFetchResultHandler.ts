import fetch, { RequestInfo, RequestInit, Request } from 'node-fetch';
import { TGetResultSetMetadataResp, TRowSet, TSparkArrowResultLink } from '../../thrift/TCLIService_types';
import HiveDriverError from '../errors/HiveDriverError';
import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';
import { ArrowBatch } from './utils';
import { LZ4 } from '../utils';
import { LogLevel } from '../contracts/IDBSQLLogger';

export default class CloudFetchResultHandler implements IResultsProvider<ArrowBatch> {
  private readonly context: IClientContext;

  private readonly source: IResultsProvider<TRowSet | undefined>;

  private readonly isLZ4Compressed: boolean;

  private readonly statementId?: string;

  private pendingLinks: Array<TSparkArrowResultLink> = [];

  private downloadTasks: Array<Promise<ArrowBatch>> = [];

  private chunkIndex: number = 0;

  constructor(
    context: IClientContext,
    source: IResultsProvider<TRowSet | undefined>,
    metadata: TGetResultSetMetadataResp,
    statementId?: string,
  ) {
    this.context = context;
    this.source = source;
    this.isLZ4Compressed = metadata.lz4Compressed ?? false;
    this.statementId = statementId;

    if (this.isLZ4Compressed && !LZ4()) {
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

    const batch = await this.downloadTasks.shift();
    if (!batch) {
      return {
        batches: [],
        rowCount: 0,
      };
    }

    if (this.isLZ4Compressed) {
      batch.batches = batch.batches.map((buffer) => LZ4()!.decode(buffer));
    }
    return batch;
  }

  private logDownloadMetrics(url: string, fileSizeBytes: number, downloadTimeMs: number): void {
    const speedMBps = fileSizeBytes / (1024 * 1024) / (downloadTimeMs / 1000);
    const cleanUrl = url.split('?')[0];

    this.context
      .getLogger()
      .log(LogLevel.info, `Result File Download speed from cloud storage ${cleanUrl}: ${speedMBps.toFixed(4)} MB/s`);

    const speedThresholdMBps = this.context.getConfig().cloudFetchSpeedThresholdMBps;
    if (speedMBps < speedThresholdMBps) {
      this.context
        .getLogger()
        .log(
          LogLevel.warn,
          `Results download is slower than threshold speed of ${speedThresholdMBps.toFixed(
            4,
          )} MB/s: ${speedMBps.toFixed(4)} MB/s`,
        );
    }
  }

  private async downloadLink(link: TSparkArrowResultLink): Promise<ArrowBatch> {
    if (Date.now() >= link.expiryTime.toNumber()) {
      throw new Error('CloudFetch link has expired');
    }

    const startTime = Date.now();
    const response = await this.fetch(link.fileLink, { headers: link.httpHeaders });
    if (!response.ok) {
      throw new Error(`CloudFetch HTTP error ${response.status} ${response.statusText}`);
    }

    const result = await response.arrayBuffer();
    const downloadTimeMs = Date.now() - startTime;

    this.logDownloadMetrics(link.fileLink, result.byteLength, downloadTimeMs);

    // Emit cloudfetch.chunk telemetry event
    this.emitCloudFetchChunk(this.chunkIndex, downloadTimeMs, result.byteLength);
    this.chunkIndex += 1;

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

  /**
   * Emit cloudfetch.chunk telemetry event.
   * CRITICAL: All exceptions swallowed and logged at LogLevel.debug ONLY.
   */
  private emitCloudFetchChunk(chunkIndex: number, latencyMs: number, bytes: number): void {
    try {
      if (!this.statementId) {
        return;
      }

      const {telemetryEmitter} = (this.context as any);
      if (!telemetryEmitter) {
        return;
      }

      telemetryEmitter.emitCloudFetchChunk({
        statementId: this.statementId,
        chunkIndex,
        latencyMs,
        bytes,
        compressed: this.isLZ4Compressed,
      });
    } catch (error: any) {
      this.context.getLogger().log(LogLevel.debug, `Error emitting cloudfetch.chunk event: ${error.message}`);
    }
  }
}
