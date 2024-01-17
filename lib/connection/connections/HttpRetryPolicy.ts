import { Response } from 'node-fetch';
import IRetryPolicy, { ShouldRetryResult, RetryableOperation } from '../contracts/IRetryPolicy';
import IClientContext, { ClientConfig } from '../../contracts/IClientContext';
import RetryError, { RetryErrorCode } from '../../errors/RetryError';

function getRetryDelay(attempt: number, config: ClientConfig): number {
  const scale = Math.max(1, 1.5 ** (attempt - 1)); // ensure scale >= 1
  return Math.min(config.retryDelayMin * scale, config.retryDelayMax);
}

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
          this.attempt += 1;

          const clientConfig = this.context.getConfig();

          // Delay interval depends on current attempt - the more attempts we do
          // the longer the interval will be
          // TODO: Respect `Retry-After` header (PECO-729)
          const retryDelay = getRetryDelay(this.attempt, clientConfig);

          const attemptsExceeded = this.attempt >= clientConfig.retryMaxAttempts;
          if (attemptsExceeded) {
            throw new RetryError(RetryErrorCode.AttemptsExceeded, response);
          }

          const timeoutExceeded = Date.now() - this.startTime + retryDelay >= clientConfig.retriesTimeout;
          if (timeoutExceeded) {
            throw new RetryError(RetryErrorCode.TimeoutExceeded, response);
          }

          return { shouldRetry: true, retryAfter: retryDelay };

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
}
