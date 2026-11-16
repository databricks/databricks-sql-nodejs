import IRetryPolicy, { ShouldRetryResult, RetryableOperation, RetryRequestClass } from '../contracts/IRetryPolicy';
import { HttpTransactionDetails } from '../contracts/IConnectionProvider';
import IClientContext from '../../contracts/IClientContext';
import RetryError, { RetryErrorCode } from '../../errors/RetryError';

// Sleeps for `milliseconds`; aborting `signal` clears the timer and rejects with
// `signal.reason`, so a cancelled backoff does not keep the process alive.
function delay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    let timer: NodeJS.Timeout | undefined;
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, milliseconds);
    signal?.addEventListener('abort', onAbort, { once: true });
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
  'EAI_AGAIN',
]);

// The client's certificate verification (OpenSSL's X509 codes as Node names them, and its hostname check)
const CERTIFICATE_ERROR_CODES = [
  'CERT_CHAIN_TOO_LONG',
  'CERT_HAS_EXPIRED',
  'CERT_NOT_YET_VALID',
  'CERT_REJECTED',
  'CERT_REVOKED',
  'CERT_SIGNATURE_FAILURE',
  'CERT_UNTRUSTED',
  'DEPTH_ZERO_SELF_SIGNED_CERT',
  'ERROR_IN_CERT_NOT_AFTER_FIELD',
  'ERROR_IN_CERT_NOT_BEFORE_FIELD',
  'ERR_TLS_CERT_ALTNAME_FORMAT',
  'ERR_TLS_CERT_ALTNAME_INVALID',
  'HOSTNAME_MISMATCH',
  'INVALID_CA',
  'INVALID_PURPOSE',
  'PATH_LENGTH_EXCEEDED',
  'SELF_SIGNED_CERT_IN_CHAIN',
  'UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY',
  'UNABLE_TO_DECRYPT_CERT_SIGNATURE',
  'UNABLE_TO_GET_ISSUER_CERT',
  'UNABLE_TO_GET_ISSUER_CERT_LOCALLY',
  'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
];

// Which statuses and network errors are retried matches the kernel (databricks-sql-kernel src/client/retry.rs
// and the CloudFetch chunk retry); backoff and budget stay Node's.
// Network errors retrying can't fix, thrown at once: certificate failures, and for RPCs a host name that doesn't
// resolve (`ENOTFOUND`; a temporary DNS failure is `EAI_AGAIN`). The kernel retries CloudFetch downloads on it.
const PERMANENT_NETWORK_ERROR_CODES: Record<RetryRequestClass, Set<string>> = {
  rpc: new Set([...CERTIFICATE_ERROR_CODES, 'ENOTFOUND']),
  cloudFetch: new Set(CERTIFICATE_ERROR_CODES),
};

// Idempotent requests retry every status except these (the kernel's IDEMPOTENT_NON_RETRYABLE); non-idempotent
// ones only a 429, or a 503 with `Retry-After`.
const IDEMPOTENT_NON_RETRYABLE_STATUSES = new Set([400, 401, 403, 404, 405, 409, 410, 411, 412, 413, 414, 415, 416]);

export function isRetryableStatus({ response }: HttpTransactionDetails, idempotent: boolean): boolean {
  const { status } = response;
  if (status >= 200 && status < 400) {
    return false;
  }
  if (idempotent) {
    return !IDEMPOTENT_NON_RETRYABLE_STATUSES.has(status);
  }
  return status === 429 || (status === 503 && response.headers.has('Retry-After'));
}

// A failure to connect (the kernel's `is_connect`), so the request never left the client and even a
// non-idempotent one can be retried. Node reports these as `connect <CODE> <address>`, as a socket closed
// before the TLS handshake finished, or, for a host with several addresses, as an AggregateError of the
// attempts, which has a code but no message (node-fetch's reason is then empty).
export function isConnectError(error: unknown): boolean {
  const { code, message } = (error ?? {}) as { code?: unknown; message?: unknown };
  if (typeof code !== 'string' || PERMANENT_NETWORK_ERROR_CODES.rpc.has(code)) {
    return false;
  }
  return (
    code === 'ECONNREFUSED' ||
    code === 'EAI_AGAIN' ||
    (typeof message === 'string' &&
      (message.includes(`connect ${code}`) ||
        message.includes('before secure TLS connection was established') ||
        message.endsWith('reason: ')))
  );
}

// Fallback message patterns for errors that don't carry an errno. node-fetch surfaces
// "socket hang up" as a generic FetchError, and "Premature close" when the response
// body stream closes before all data is received — both occur regularly when a
// keep-alive TCP connection is silently dropped by an intermediate load balancer.
const RETRYABLE_NETWORK_ERROR_MESSAGE_RE = /socket hang up|premature close|aborted/i;

export default class HttpRetryPolicy implements IRetryPolicy<HttpTransactionDetails> {
  private context: IClientContext;

  private startTime: number; // in milliseconds

  private attempt: number;

  private readonly requestClass: RetryRequestClass;

  constructor(context: IClientContext, requestClass: RetryRequestClass = 'rpc') {
    this.context = context;
    this.startTime = Date.now();
    this.attempt = 0;
    this.requestClass = requestClass;
  }

  public async shouldRetry(details: HttpTransactionDetails): Promise<ShouldRetryResult> {
    if (this.isRetryable(details)) {
      return this.computeRetry(details);
    }

    return { shouldRetry: false };
  }

  public async invokeWithRetry(
    operation: RetryableOperation<HttpTransactionDetails>,
    signal?: AbortSignal,
  ): Promise<HttpTransactionDetails> {
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
        await delay(status.retryAfter, signal);
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
        await delay(status.retryAfter, signal);
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

    // Wait as long as `Retry-After` asks (up to retryDelayMax), but don't start a wait that would end after
    // retriesTimeout, as the kernel does. Without the header, back off exponentially.
    const retryAfterHeader = this.getRetryAfterHeader(details, clientConfig.retryDelayMin);
    if (retryAfterHeader !== undefined) {
      const retryAfter = Math.min(retryAfterHeader, clientConfig.retryDelayMax);
      if (Date.now() - this.startTime + retryAfter > clientConfig.retriesTimeout) {
        throw new RetryError(RetryErrorCode.TimeoutExceeded, details);
      }
      return { shouldRetry: true, retryAfter };
    }
    const retryAfter = this.getBackoffDelay(this.attempt, clientConfig.retryDelayMin, clientConfig.retryDelayMax);

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

    if (typeof candidate.code === 'string' && PERMANENT_NETWORK_ERROR_CODES[this.requestClass].has(candidate.code)) {
      return false;
    }

    // node-fetch FetchError surfaces low-level network failures with `type: 'system'`, no response
    // within the request timeout as `request-timeout` (retried like the kernel's `is_timeout`), and a
    // body-stream timeout as `body-timeout`.
    if (candidate.type === 'system' || candidate.type === 'request-timeout' || candidate.type === 'body-timeout') {
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

  protected isRetryable(details: HttpTransactionDetails): boolean {
    return isRetryableStatus(details, true);
  }

  protected getRetryAfterHeader({ response }: HttpTransactionDetails, delayMin: number): number | undefined {
    // `Retry-After` header may contain a date after which to retry, or delay seconds. We support only delay seconds.
    // Value from `Retry-After` header is used when:
    // 1. it's available and is non-empty
    // 2. it could be parsed as a number, and is greater than zero
    // 3. additionally, we clamp it to not be smaller than minimal retry delay (in milliseconds)
    const header = response.headers.get('Retry-After') || '';
    if (header !== '') {
      const value = Number(header);
      if (Number.isFinite(value) && value > 0) {
        return Math.max(delayMin, value * 1000);
      }
    }
    return undefined;
  }

  protected getBackoffDelay(attempt: number, delayMin: number, delayMax: number): number {
    const value = 2 ** attempt * delayMin;
    return Math.min(value, delayMax);
  }
}
