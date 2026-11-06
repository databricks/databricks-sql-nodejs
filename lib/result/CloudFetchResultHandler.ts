import fetch, { RequestInfo, RequestInit, Request, Response } from 'node-fetch';
import { TGetResultSetMetadataResp, TRowSet, TSparkArrowResultLink } from '../../thrift/TCLIService_types';
import HiveDriverError from '../errors/HiveDriverError';
import RetryError from '../errors/RetryError';
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
    let result: { request: Request; response: Response };
    try {
      result = await retryPolicy.invokeWithRetry(async () => {
        downloaded = undefined;
        const request = new Request(url, requestConfig);
        const response = await fetch(request);
        if (response.ok) {
          downloaded = await response.arrayBuffer();
        }
        return { request, response };
      });
    } catch (error) {
      throw CloudFetchResultHandler.describeDownloadError(error);
    }
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

  private static redactUrls(text: string): string {
    return text.replace(/\b[a-z][a-z0-9+.-]*:\/\/\S+/gi, '<redacted-url>');
  }

  /**
   * Network errors (e.g. node-fetch `FetchError`) embed the request URL in their message and stack.
   * For CloudFetch that is a presigned cloud-storage link whose query string carries credentials,
   * so scrub any URL in place. Best effort: non-writable properties on exotic error objects must
   * not mask the original failure.
   */
  private static scrubErrorUrls(error: Error): void {
    try {
      if (typeof error.message === 'string') {
        error.message = CloudFetchResultHandler.redactUrls(error.message);
      }
      if (typeof error.stack === 'string') {
        // Only the header (message) lines can carry the request URL; leave `    at ...` frames intact so
        // `file://` locations of ESM modules stay readable.
        error.stack = error.stack
          .split('\n')
          .map((line) => (/^\s+at /.test(line) ? line : CloudFetchResultHandler.redactUrls(line)))
          .join('\n');
      }
    } catch {
      // ignore
    }
  }

  /**
   * Make a download failure safe to log and easier to diagnose:
   * - Any error escaping the retry policy (wrapped in `RetryError` or re-thrown unchanged, e.g. a
   *   non-retryable TLS `FetchError`) has URLs scrubbed from its message / stack, since the
   *   presigned CloudFetch link carries credentials in its query string.
   * - For `RetryError`, the generic "Max retry count exceeded" / "Retry timeout exceeded" message
   *   is enriched with the CloudFetch context and the last cause (HTTP status or network error).
   *   When the payload is an HTTP `{ request, response }` pair, the node-fetch objects (which hold
   *   the full presigned URL) are replaced with a plain snapshot carrying the status, status text,
   *   headers and the URL without its query string, so `util.inspect` / structured-logger
   *   serializers that walk `error.payload` don't print credentials. Note this changes the observable
   *   payload shape for CloudFetch HTTP failures: `payload.response` is no longer a node-fetch
   *   `Response` (so `instanceof Response`, `.text()` / `.json()` do not apply); it is a plain
   *   `{ status, statusText, ok, headers, url }` object. This is intentional — keeping the original
   *   `Response` reachable would keep the credentialed URL reachable too.
   * The same error instance, class and error code are kept so `instanceof` / `errorCode` checks
   * still work.
   */
  private static describeDownloadError(error: unknown): unknown {
    if (!(error instanceof RetryError)) {
      if (error instanceof Error) {
        CloudFetchResultHandler.scrubErrorUrls(error);
      }
      return error;
    }

    let cause: string | undefined;
    const { payload } = error;
    if (payload instanceof Error) {
      const { code } = payload as Error & { code?: unknown };
      CloudFetchResultHandler.scrubErrorUrls(payload);
      let safeMessage: string;
      try {
        // `message` may be non-string on exotic error subclasses; never let that mask the `RetryError`.
        const rawMessage: unknown = payload.message;
        safeMessage = CloudFetchResultHandler.redactUrls(
          typeof rawMessage === 'string' ? rawMessage : String(rawMessage),
        );
      } catch {
        safeMessage = '<unavailable>';
      }
      cause = code ? `${safeMessage} (${code})` : safeMessage;
    } else if (payload && typeof payload === 'object' && 'response' in payload) {
      const { request, response } = payload as {
        request?: { method?: string; url?: string };
        response?: { status?: number; statusText?: string; ok?: boolean; headers?: unknown; url?: string };
      };
      if (response && typeof response.status === 'number') {
        cause = `HTTP ${response.status}${response.statusText ? `: ${response.statusText}` : ''}`;
      }
      const stripQuery = (url?: string) => (typeof url === 'string' ? url.split('?')[0] : undefined);
      const safePayload = {
        request: request ? { method: request.method, url: stripQuery(request.url) } : undefined,
        response: response
          ? {
              status: response.status,
              statusText: response.statusText,
              ok: response.ok,
              headers: response.headers,
              url: stripQuery(response.url),
            }
          : undefined,
      };
      // The original `Response` becomes unreachable once replaced below, so nobody could drain its body
      // afterwards. Drain it now (best effort) so an unread stream doesn't pin a keep-alive socket.
      try {
        const body = (response as { body?: unknown } | undefined)?.body as
          | { on?: (event: string, listener: () => void) => unknown; resume?: () => unknown }
          | undefined;
        if (body && typeof body.resume === 'function') {
          if (typeof body.on === 'function') {
            body.on('error', () => {});
          }
          body.resume();
        }
      } catch {
        // Best effort: never mask the original failure.
      }
      try {
        Object.defineProperty(error, 'payload', { value: safePayload });
      } catch {
        // Best effort: never mask the original failure.
      }
    }

    // Mutate the original error rather than creating a new one, so the stack trace still points at
    // where the retry policy gave up. V8 bakes the message into the `stack` header at construction
    // time, so rewrite that header too — otherwise loggers that print `err.stack` miss the context.
    const originalHeader = `${error.name}: ${error.message}`;
    const message = `CloudFetch result download failed: ${error.message}${cause ? ` (last error: ${cause})` : ''}`;
    error.message = message;
    if (typeof error.stack === 'string' && error.stack.startsWith(originalHeader)) {
      error.stack = `${error.name}: ${message}${error.stack.slice(originalHeader.length)}`;
    }
    return error;
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
