export type ShouldRetryResult =
  | {
      shouldRetry: false;
    }
  | {
      shouldRetry: true;
      retryAfter: number; // in milliseconds
    };

export type RetryableOperation<R> = () => Promise<R>;

export default interface IRetryPolicy<R> {
  shouldRetry(details: R): Promise<ShouldRetryResult>;

  invokeWithRetry(operation: RetryableOperation<R>): Promise<R>;
}
