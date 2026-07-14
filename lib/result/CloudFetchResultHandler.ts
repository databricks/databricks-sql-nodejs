import fetch, { RequestInfo, RequestInit, Request, Response } from 'node-fetch';
import { TGetResultSetMetadataResp, TRowSet, TSparkArrowResultLink } from '../../thrift/TCLIService_types';
import HiveDriverError from '../errors/HiveDriverError';
import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';
import { ArrowBatch } from './utils';
import { LZ4 } from '../utils';
import { LogLevel } from '../contracts/IDBSQLLogger';
import { safeEmit } from '../telemetry/telemetryUtils';

// A download task is prefetched up to `cloudFetchConcurrentDownloads` ahead of consumption,
// but only one task is awaited per `fetchNext` call. To keep every in-flight prefetch promise
// "handled" the instant it is created — even if the consumer abandons iteration after an
// earlier task rejects — we reflect each download's outcome into a value that always resolves.
// The error is re-thrown only when the task is actually consumed, so observable behavior is
// unchanged while the unhandled-rejection leak is eliminated.
type SettledDownload = { ok: true; batch: ArrowBatch } | { ok: false; error: unknown };

export default class CloudFetchResultHandler implements IResultsProvider<ArrowBatch> {
  private readonly context: IClientContext;

  private readonly source: IResultsProvider<TRowSet | undefined>;

  private readonly isLZ4Compressed: boolean;

  private readonly statementId?: string;

  private pendingLinks: Array<TSparkArrowResultLink> = [];

  private downloadTasks: Array<Promise<SettledDownload>> = [];

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
      throw new HiveDriverError('Cannot handle LZ4 compressed result: module `lz4-napi` not installed');
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
      // Attach success/failure handlers synchronously at creation time. This guarantees a
      // prefetched download promise can never become an unhandled rejection if the consumer
      // stops iterating before it is awaited (e.g. because an earlier task threw).
      const tasks = links.map((link) =>
        this.downloadLink(link).then(
          (batch): SettledDownload => ({ ok: true, batch }),
          (error): SettledDownload => ({ ok: false, error }),
        ),
      );
      this.downloadTasks.push(...tasks);
    }

    const settled = await this.downloadTasks.shift();
    if (!settled) {
      return {
        batches: [],
        rowCount: 0,
      };
    }

    if (!settled.ok) {
      throw settled.error;
    }

    const { batch } = settled;
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
      .log(LogLevel.debug, `Result File Download speed from cloud storage ${cleanUrl}: ${speedMBps.toFixed(4)} MB/s`);

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
    const { response, body } = await this.fetch(link.fileLink, { headers: link.httpHeaders });
    if (!response.ok) {
      throw new Error(`CloudFetch HTTP error ${response.status} ${response.statusText}`);
    }
    // `body` is always set when `response.ok` is true — `fetch` reads it inside
    // the retried block on success.
    const result = body!;
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

  private async fetch(url: RequestInfo, init?: RequestInit): Promise<{ response: Response; body?: ArrayBuffer }> {
    const connectionProvider = await this.context.getConnectionProvider();
    const agent = await connectionProvider.getAgent();
    const retryPolicy = await connectionProvider.getRetryPolicy();

    const requestConfig: RequestInit = { agent, ...init };
    // Read the body inside the retried block. CloudFetch downloads are large
    // GETs against pre-signed cloud-storage URLs that frequently surface
    // `socket hang up` / "Premature close" once the stream is mid-transfer.
    // Pulling `arrayBuffer()` in here lets the retry policy treat those
    // body-stream failures the same as connect-time failures.
    let downloaded: ArrayBuffer | undefined;
    const result = await retryPolicy.invokeWithRetry(async () => {
      downloaded = undefined;
      const request = new Request(url, requestConfig);
      const response = await fetch(request);
      if (response.ok) {
        downloaded = await response.arrayBuffer();
      }
      return { request, response };
    });
    // Fall back to reading the body here if the retry policy returned a
    // response without consuming it via our operation (e.g. unit-test stubs
    // that hand back a pre-baked Response without invoking the operation
    // callback). In production the body is always read inside the retried
    // block above, so this path is a no-op.
    if (downloaded === undefined && result.response.ok) {
      downloaded = await result.response.arrayBuffer();
    }
    return { response: result.response, body: downloaded };
  }

  /**
   * Emit cloudfetch.chunk telemetry event.
   * CRITICAL: All exceptions swallowed and logged at LogLevel.debug ONLY.
   */
  private emitCloudFetchChunk(chunkIndex: number, latencyMs: number, bytes: number): void {
    const { statementId } = this;
    if (!statementId) {
      return;
    }
    safeEmit(this.context, (emitter) => {
      emitter.emitCloudFetchChunk({
        statementId,
        chunkIndex,
        latencyMs,
        bytes,
        compressed: this.isLZ4Compressed,
      });
    });
  }
}
