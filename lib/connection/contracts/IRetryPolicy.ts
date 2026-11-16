export type ShouldRetryResult =
  | {
      shouldRetry: false;
    }
  | {
      shouldRetry: true;
      retryAfter: number; // in milliseconds
    };

export type RetryableOperation<R> = () => Promise<R>;

// Request classes whose retried network errors differ (see HttpRetryPolicy)
export type RetryRequestClass = 'rpc' | 'cloudFetch';

export default interface IRetryPolicy<R> {
  shouldRetry(details: R): Promise<ShouldRetryResult>;

  // `signal` cancels a pending backoff sleep: aborting it rejects with `signal.reason`
  // instead of waiting for the sleep to finish (the operation itself must honour the signal).
  invokeWithRetry(operation: RetryableOperation<R>, signal?: AbortSignal): Promise<R>;
}
