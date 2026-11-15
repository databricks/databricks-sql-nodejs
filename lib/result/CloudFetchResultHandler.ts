import fetch, { RequestInfo, RequestInit, Request, Response } from 'node-fetch';
import Int64 from 'node-int64';
import { TGetResultSetMetadataResp, TRowSet, TSparkArrowResultLink } from '../../thrift/TCLIService_types';
import HiveDriverError from '../errors/HiveDriverError';
import RetryError, { RetryErrorCode } from '../errors/RetryError';
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

// Largest delay `setTimeout` accepts; longer values fire immediately, so they are clamped.
const MAX_TIMER_DELAY_MS = 2 ** 31 - 1;

// Statuses that mean the presigned URL itself was rejected (expired or invalid signature; GCS
// answers 404 for an expired one). Retrying the same URL can't succeed, so the link is refreshed.
// Other 4xx responses are retried with the same URL.
const LINK_REFRESH_STATUSES: ReadonlySet<number> = new Set([401, 403, 404]);

interface LinkRefresher {
  // An aborted `signal` means the caller gave up; a refresh still queued may then be skipped
  refreshLinks(startRowOffset: Int64, maxRows: Int64, signal?: AbortSignal): Promise<Array<TSparkArrowResultLink>>;
}

function canRefreshLinks(source: unknown): source is LinkRefresher {
  return typeof (source as Partial<LinkRefresher> | undefined)?.refreshLinks === 'function';
}

// The `retriesTimeout` deadline of one download: its GET attempts, the backoff between them and
// its link refreshes. On expiry `signal` is aborted with a `TimeoutExceeded` error.
class DownloadDeadline {
  public readonly signal?: AbortSignal;

  // Reported as the cause of the timeout error
  public lastFailure: unknown;

  private readonly timer?: NodeJS.Timeout;

  constructor(timeoutMs: number) {
    if (timeoutMs > 0) {
      const controller = new AbortController();
      this.signal = controller.signal;
      this.timer = setTimeout(() => {
        controller.abort(new RetryError(RetryErrorCode.TimeoutExceeded, this.lastFailure));
      }, Math.min(timeoutMs, MAX_TIMER_DELAY_MS));
    }
  }

  public throwIfExpired(): void {
    if (this.signal?.aborted) {
      throw this.signal.reason;
    }
  }

  // Settles like `promise`, or rejects with the timeout error as soon as the deadline expires
  public race<T>(promise: Promise<T>): Promise<T> {
    const { signal } = this;
    if (!signal) {
      return promise;
    }
    return new Promise<T>((resolve, reject) => {
      const onExpired = () => reject(signal.reason);
      if (signal.aborted) {
        onExpired();
      } else {
        signal.addEventListener('abort', onExpired, { once: true });
      }
      promise.then(
        (value) => {
          signal.removeEventListener('abort', onExpired);
          resolve(value);
        },
        (error) => {
          signal.removeEventListener('abort', onExpired);
          reject(error);
        },
      );
    });
  }

  public clear(): void {
    clearTimeout(this.timer);
  }
}

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

    // The retry policy checks `retriesTimeout` only between attempts, so it is enforced as one
    // deadline over the whole download, including stalled attempts and link refreshes (#520)
    const deadline = new DownloadDeadline(this.context.getConfig().retriesTimeout);
    try {
      return await deadline.race(this.download(link, deadline));
    } catch (error) {
      throw CloudFetchResultHandler.describeDownloadError(error);
    } finally {
      deadline.clear();
    }
  }

  private async download(link: TSparkArrowResultLink, deadline: DownloadDeadline): Promise<ArrowBatch> {
    const startTime = Date.now();
    const { retryMaxAttempts } = this.context.getConfig();
    let currentLink = link;
    let { response, body } = await this.fetch(currentLink.fileLink, { headers: currentLink.httpHeaders }, deadline);
    // At most `retryMaxAttempts` URLs per download; each `fetch` retries its URL under its own budget
    for (let attempt = 1; LINK_REFRESH_STATUSES.has(response.status) && attempt < retryMaxAttempts; attempt += 1) {
      // eslint-disable-next-line no-await-in-loop
      const freshLink = await this.refreshLink(currentLink, response.status, deadline.signal);
      if (!freshLink) {
        break;
      }
      // The download already failed if the refresh outlasted the deadline; don't GET the fresh URL
      deadline.throwIfExpired();
      currentLink = freshLink;
      // eslint-disable-next-line no-await-in-loop
      ({ response, body } = await this.fetch(currentLink.fileLink, { headers: currentLink.httpHeaders }, deadline));
    }
    if (!response.ok) {
      throw new Error(`CloudFetch HTTP error ${response.status} ${response.statusText}`);
    }
    // A download that outlasted the deadline was already reported as failed
    deadline.throwIfExpired();
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

  /** Asks the server for a fresh link to the rows of `link`; `undefined` if there is none. */
  private async refreshLink(
    link: TSparkArrowResultLink,
    status: number,
    signal: AbortSignal | undefined,
  ): Promise<TSparkArrowResultLink | undefined> {
    if (!canRefreshLinks(this.source)) {
      return undefined;
    }
    const offset = link.startRowOffset.toNumber();
    this.context
      .getLogger()
      .log(LogLevel.debug, `CloudFetch download got HTTP ${status}; refreshing the link at row offset ${offset}`);
    const links = await this.source.refreshLinks(link.startRowOffset, link.rowCount, signal);
    return links.find((candidate) => candidate.startRowOffset.toNumber() === offset);
  }

  private async fetch(
    url: RequestInfo,
    init: RequestInit,
    deadline: DownloadDeadline = new DownloadDeadline(0),
  ): Promise<{ response: Response; body?: ArrayBuffer }> {
    const connectionProvider = await this.context.getConnectionProvider();
    let requestUrl = url;
    if (typeof requestUrl !== 'string') {
      requestUrl = 'href' in requestUrl ? requestUrl.href : requestUrl.url;
    }
    const agent = await connectionProvider.getAgent(requestUrl);
    const retryPolicy = await connectionProvider.getRetryPolicy({ retryClientErrorsExcept: LINK_REFRESH_STATUSES });
    const { socketTimeout } = this.context.getConfig();
    // `socketTimeout` is an inactivity timeout for each attempt: an attempt that receives
    // nothing for that long is aborted and retried like a network timeout.
    const idleTimeoutMs = socketTimeout > 0 ? Math.min(socketTimeout, MAX_TIMER_DELAY_MS) : undefined;

    const requestConfig: RequestInit = { agent, ...init };
    const { signal } = deadline;

    // Read the body inside the retried block. CloudFetch downloads are large
    // GETs against pre-signed cloud-storage URLs that frequently surface
    // `socket hang up` / "Premature close" once the stream is mid-transfer.
    // Pulling `arrayBuffer()` in here lets the retry policy treat those
    // body-stream failures the same as connect-time failures.
    let downloaded: ArrayBuffer | undefined;
    // An expired deadline aborts the in-flight request and rejects a pending backoff sleep
    const result = await retryPolicy.invokeWithRetry(async () => {
      // The timeout error is not retryable, so this also ends the retry loop when the
      // deadline expired during a backoff sleep or aborted the attempt.
      deadline.throwIfExpired();
      downloaded = undefined;
      // Aborted by the deadline or when the attempt goes idle
      const attempt = new AbortController();
      const abortAttempt = () => attempt.abort();
      signal?.addEventListener('abort', abortAttempt, { once: true });
      let attemptTimedOut = false;
      let idleTimer: NodeJS.Timeout | undefined;
      const restartIdleTimer = () => {
        if (idleTimeoutMs !== undefined) {
          clearTimeout(idleTimer);
          idleTimer = setTimeout(() => {
            attemptTimedOut = true;
            attempt.abort();
          }, idleTimeoutMs);
        }
      };
      restartIdleTimer();
      const request = new Request(url, {
        ...requestConfig,
        signal: attempt.signal as unknown as RequestInit['signal'],
      });
      try {
        const response = await fetch(request);
        restartIdleTimer();
        if (response.ok) {
          const bodyRead = response.arrayBuffer();
          response.body.on('data', restartIdleTimer);
          downloaded = await bodyRead;
        } else {
          deadline.lastFailure = { request, response };
        }
        return { request, response };
      } catch (error) {
        deadline.throwIfExpired();
        deadline.lastFailure = attemptTimedOut
          ? Object.assign(new Error('CloudFetch download attempt timed out'), { code: 'ETIMEDOUT' })
          : error;
        throw deadline.lastFailure;
      } finally {
        clearTimeout(idleTimer);
        signal?.removeEventListener('abort', abortAttempt);
      }
    }, signal);
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
   * Make a CloudFetch retry-exhaustion failure easier to diagnose (#471): the generic
   * "Max retry count exceeded" / "Retry timeout exceeded" message of a `RetryError` is enriched with
   * the CloudFetch context and the last cause — the HTTP status / status text, or for network errors
   * only the error code / type. Raw network-error messages are deliberately NOT copied, because
   * node-fetch embeds the presigned (credential-bearing) request URL in them.
   * The same error instance, class, error code and payload (including the node-fetch
   * `Request` / `Response` objects) are kept. Errors other than `RetryError` are returned unchanged.
   */
  private static describeDownloadError(error: unknown): unknown {
    if (!(error instanceof RetryError)) {
      return error;
    }

    const prefix = 'CloudFetch result download failed: ';
    // Idempotent: the same instance may come through again (e.g. a custom retry policy rethrowing a
    // cached error) — don't stack the prefix / cause a second time.
    if (error.message.startsWith(prefix)) {
      return error;
    }

    let cause: string | undefined;
    const { payload } = error;
    if (payload instanceof Error) {
      const { code, type } = payload as Error & { code?: unknown; type?: unknown };
      if (typeof code === 'string' && code) {
        cause = `network error ${code}`;
      } else if (typeof type === 'string' && type) {
        cause = `network error (${type})`;
      } else {
        cause = 'network error';
      }
    } else if (payload && typeof payload === 'object' && 'response' in payload) {
      const { response } = payload as { response?: { status?: unknown; statusText?: unknown } };
      if (response && typeof response.status === 'number') {
        const { statusText } = response;
        cause = `HTTP ${response.status}${typeof statusText === 'string' && statusText ? `: ${statusText}` : ''}`;
      }
    }

    // Mutate the original error rather than creating a new one, so the stack trace still points at
    // where the retry policy gave up. V8 bakes the message into the `stack` header at construction
    // time, so rewrite that header too — otherwise loggers that print `err.stack` miss the context.
    const originalHeader = `${error.name}: ${error.message}`;
    const message = `${prefix}${error.message}${cause ? ` (last error: ${cause})` : ''}`;
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
