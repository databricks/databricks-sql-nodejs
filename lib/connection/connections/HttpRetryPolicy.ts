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
      switch (response.status) {
        // On these status codes it's safe to retry the request. However,
        // both error codes mean that server is overwhelmed or even down.
        // Therefore, we need to add some delay between attempts so
        // server can recover and more likely handle next request
        case 429: // Too Many Requests
        case 503: // Service Unavailable
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

        // TODO: Here we should handle other error types (see PECO-730)

        // no default
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
