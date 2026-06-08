import IRetryPolicy, { ShouldRetryResult, RetryableOperation } from '../contracts/IRetryPolicy';
import { HttpTransactionDetails } from '../contracts/IConnectionProvider';
import IClientContext from '../../contracts/IClientContext';
import RetryError, { RetryErrorCode } from '../../errors/RetryError';

function delay(milliseconds: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(() => resolve(), milliseconds);
  });
}

// Transient network error codes worth retrying. Aligned with the OS-level errno set
// surfaced by Node's `http`/`https` (and `node-fetch` via `system` FetchError type)
// when an in-flight request fails before/while delivering a response. Matches the
// classes of errors that the Python (urllib3) and JDBC (Apache HttpClient) drivers
// retry by default at the connection layer.
const RETRYABLE_NETWORK_ERROR_CODES = new Set([
  'ECONNRESET',
  'ECONNREFUSED',
  'ETIMEDOUT',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'EPIPE',
  'ENOTFOUND',
  'EAI_AGAIN',
]);

// Fallback message patterns for errors that don't carry an errno. node-fetch surfaces
// "socket hang up" as a generic FetchError, and "Premature close" when the response
// body stream closes before all data is received — both occur regularly when a
// keep-alive TCP connection is silently dropped by an intermediate load balancer.
const RETRYABLE_NETWORK_ERROR_MESSAGE_RE = /socket hang up|premature close|aborted/i;

export default class HttpRetryPolicy implements IRetryPolicy<HttpTransactionDetails> {
  private context: IClientContext;

  private startTime: number; // in milliseconds

  private attempt: number;

  constructor(context: IClientContext) {
    this.context = context;
    this.startTime = Date.now();
    this.attempt = 0;
  }

  public async shouldRetry(details: HttpTransactionDetails): Promise<ShouldRetryResult> {
    if (this.isRetryable(details)) {
      return this.computeRetry(details);
    }

    return { shouldRetry: false };
  }

  public async invokeWithRetry(operation: RetryableOperation<HttpTransactionDetails>): Promise<HttpTransactionDetails> {
    for (;;) {
      // Capture either the resolved response or the thrown error so the
      // retry-decision logic below can flow without an early `continue` and
      // share one backoff site between both paths.
      let outcome: { ok: true; details: HttpTransactionDetails } | { ok: false; error: unknown };
      try {
        // eslint-disable-next-line no-await-in-loop
        const details = await operation();
        outcome = { ok: true, details };
      } catch (error) {
        outcome = { ok: false, error };
      }

      if (outcome.ok) {
        // eslint-disable-next-line no-await-in-loop
        const status = await this.shouldRetry(outcome.details);
        if (!status.shouldRetry) {
          return outcome.details;
        }
        // eslint-disable-next-line no-await-in-loop
        await delay(status.retryAfter);
      } else {
        // The operation threw before producing a response. This is typically a
        // transient network failure (stale keep-alive socket reset by a load
        // balancer, DNS hiccup, truncated response body, etc.). The status-code-
        // driven `shouldRetry` path can't see these because there's no `Response`
        // to inspect, so we have a separate decision point here. Non-network
        // errors (programmer errors, config errors, RetryError raised by our
        // own attempts/timeout budget) are re-thrown unchanged.
        if (!this.isRetryableNetworkError(outcome.error)) {
          throw outcome.error;
        }
        // eslint-disable-next-line no-await-in-loop
        const status = await this.computeNetworkErrorRetry(outcome.error);
        if (!status.shouldRetry) {
          throw outcome.error;
        }
        // eslint-disable-next-line no-await-in-loop
        await delay(status.retryAfter);
      }
    }
  }

  // Shared budgeting logic — bumps the attempt counter, enforces overall retries
  // timeout/max attempts, and computes the next backoff. Used by both the HTTP
  // status-code path (`shouldRetry`) and the network-error path
  // (`computeNetworkErrorRetry`) so they share a single attempt budget.
  private computeRetry(details: HttpTransactionDetails): ShouldRetryResult {
    const clientConfig = this.context.getConfig();

    // Don't retry if overall retry timeout exceeded
    const timeoutExceeded = Date.now() - this.startTime >= clientConfig.retriesTimeout;
    if (timeoutExceeded) {
      throw new RetryError(RetryErrorCode.TimeoutExceeded, details);
    }

    this.attempt += 1;

    // Don't retry if max attempts count reached
    const attemptsExceeded = this.attempt >= clientConfig.retryMaxAttempts;
    if (attemptsExceeded) {
      throw new RetryError(RetryErrorCode.AttemptsExceeded, details);
    }

    // If possible, use `Retry-After` header as a floor for a backoff algorithm
    const retryAfterHeader = this.getRetryAfterHeader(details, clientConfig.retryDelayMin);
    const retryAfter = this.getBackoffDelay(
      this.attempt,
      retryAfterHeader ?? clientConfig.retryDelayMin,
      clientConfig.retryDelayMax,
    );

    return { shouldRetry: true, retryAfter };
  }

  private async computeNetworkErrorRetry(error: unknown): Promise<ShouldRetryResult> {
    const clientConfig = this.context.getConfig();

    const timeoutExceeded = Date.now() - this.startTime >= clientConfig.retriesTimeout;
    if (timeoutExceeded) {
      throw new RetryError(RetryErrorCode.TimeoutExceeded, error);
    }

    this.attempt += 1;

    const attemptsExceeded = this.attempt >= clientConfig.retryMaxAttempts;
    if (attemptsExceeded) {
      throw new RetryError(RetryErrorCode.AttemptsExceeded, error);
    }

    const retryAfter = this.getBackoffDelay(this.attempt, clientConfig.retryDelayMin, clientConfig.retryDelayMax);

    return { shouldRetry: true, retryAfter };
  }

  protected isRetryableNetworkError(error: unknown): boolean {
    if (!error || typeof error !== 'object') {
      return false;
    }
    const candidate = error as { code?: string; type?: string; message?: string };

    // node-fetch FetchError surfaces low-level network failures with `type: 'system'`
    // and a body-stream timeout with `type: 'body-timeout'`. Both should be retried;
    // `request-timeout` is converted to a Thrift TApplicationException upstream so
    // we don't need to retry it here.
    if (candidate.type === 'system' || candidate.type === 'body-timeout') {
      return true;
    }

    if (typeof candidate.code === 'string' && RETRYABLE_NETWORK_ERROR_CODES.has(candidate.code)) {
      return true;
    }

    if (typeof candidate.message === 'string' && RETRYABLE_NETWORK_ERROR_MESSAGE_RE.test(candidate.message)) {
      return true;
    }

    return false;
  }

  protected isRetryable({ response }: HttpTransactionDetails): boolean {
    const statusCode = response.status;

    const result =
      // Retry on all codes below 100
      statusCode < 100 ||
      // ...and on `429 Too Many Requests`
      statusCode === 429 ||
      // ...and on all `5xx` codes except for `501 Not Implemented`
      (statusCode >= 500 && statusCode !== 501);

    return result;
  }

  protected getRetryAfterHeader({ response }: HttpTransactionDetails, delayMin: number): number | undefined {
    // `Retry-After` header may contain a date after which to retry, or delay seconds. We support only delay seconds.
    // Value from `Retry-After` header is used when:
    // 1. it's available and is non-empty
    // 2. it could be parsed as a number, and is greater than zero
    // 3. additionally, we clamp it to not be smaller than minimal retry delay
    const header = response.headers.get('Retry-After') || '';
    if (header !== '') {
      const value = Number(header);
      if (Number.isFinite(value) && value > 0) {
        return Math.max(delayMin, value);
      }
    }
    return undefined;
  }

  protected getBackoffDelay(attempt: number, delayMin: number, delayMax: number): number {
    const value = 2 ** attempt * delayMin;
    return Math.min(value, delayMax);
  }
}
