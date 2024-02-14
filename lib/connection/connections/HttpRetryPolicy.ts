import { Response } from 'node-fetch';
import IRetryPolicy, { ShouldRetryResult, RetryableOperation } from '../contracts/IRetryPolicy';
import IClientContext, { ClientConfig } from '../../contracts/IClientContext';
import RetryError, { RetryErrorCode } from '../../errors/RetryError';

function delay(milliseconds: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(() => resolve(), milliseconds);
  });
}

export default class HttpRetryPolicy implements IRetryPolicy<Response> {
  private context: IClientContext;

  private readonly startTime: number; // in milliseconds

  private attempt: number;

  constructor(context: IClientContext) {
    this.context = context;
    this.startTime = Date.now();
    this.attempt = 0;
  }

  public async shouldRetry(response: Response): Promise<ShouldRetryResult> {
    if (!response.ok) {
      if (this.canRetry(response)) {
        const clientConfig = this.context.getConfig();

        // Don't retry if overall retry timeout exceeded
        const timeoutExceeded = Date.now() - this.startTime >= clientConfig.retriesTimeout;
        if (timeoutExceeded) {
          throw new RetryError(RetryErrorCode.TimeoutExceeded, response);
        }

        this.attempt += 1;

        // Don't retry if max attempts count reached
        const attemptsExceeded = this.attempt >= clientConfig.retryMaxAttempts;
        if (attemptsExceeded) {
          throw new RetryError(RetryErrorCode.AttemptsExceeded, response);
        }

        // Try to use retry delay from `Retry-After` header if available and valid, otherwise fall back to backoff
        const retryAfter =
          this.getRetryAfterHeader(response, clientConfig) ?? this.getBackoffDelay(this.attempt, clientConfig);

        return { shouldRetry: true, retryAfter };
      }
    }

    return { shouldRetry: false };
  }

  public async invokeWithRetry(operation: RetryableOperation<Response>): Promise<Response> {
    for (;;) {
      const response = await operation(); // eslint-disable-line no-await-in-loop
      const status = await this.shouldRetry(response); // eslint-disable-line no-await-in-loop
      if (!status.shouldRetry) {
        return response;
      }
      await delay(status.retryAfter); // eslint-disable-line no-await-in-loop
    }
  }

  protected canRetry(response: Response): boolean {
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

  protected getRetryAfterHeader(response: Response, config: ClientConfig): number | undefined {
    // `Retry-After` header may contain a date after which to retry, or delay seconds. We support only delay seconds.
    // Value from `Retry-After` header is used when:
    // 1. it's available and is non-empty
    // 2. it could be parsed as a number, and is greater than zero
    // 3. additionally, we clamp it to not be smaller than minimal retry delay
    const header = response.headers.get('Retry-After') || '';
    if (header !== '') {
      const value = Number(header);
      if (Number.isFinite(value) && value > 0) {
        return Math.max(config.retryDelayMin, value);
      }
    }
    return undefined;
  }

  protected getBackoffDelay(attempt: number, config: ClientConfig): number {
    const value = 2 ** attempt * config.retryDelayMin;
    return Math.min(value, config.retryDelayMax);
  }
}
