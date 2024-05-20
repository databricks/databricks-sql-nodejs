import IRetryPolicy, { ShouldRetryResult, RetryableOperation } from '../contracts/IRetryPolicy';
import { HttpTransactionDetails } from '../contracts/IConnectionProvider';
import IClientContext from '../../contracts/IClientContext';
import RetryError, { RetryErrorCode } from '../../errors/RetryError';

function delay(milliseconds: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(() => resolve(), milliseconds);
  });
}

export default class HttpRetryPolicy implements IRetryPolicy<HttpTransactionDetails> {
  protected context: IClientContext;

  protected startTime: number; // in milliseconds

  protected attempt: number;

  constructor(context: IClientContext) {
    this.context = context;
    this.startTime = Date.now();
    this.attempt = 0;
  }

  public async shouldRetry(details: HttpTransactionDetails): Promise<ShouldRetryResult> {
    if (this.isRetryable(details)) {
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

    return { shouldRetry: false };
  }

  public async invokeWithRetry(operation: RetryableOperation<HttpTransactionDetails>): Promise<HttpTransactionDetails> {
    for (;;) {
      const details = await operation(); // eslint-disable-line no-await-in-loop
      const status = await this.shouldRetry(details); // eslint-disable-line no-await-in-loop
      if (!status.shouldRetry) {
        return details;
      }
      await delay(status.retryAfter); // eslint-disable-line no-await-in-loop
    }
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
