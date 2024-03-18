import IRetryPolicy, { ShouldRetryResult, RetryableOperation } from '../contracts/IRetryPolicy';

export default class NullRetryPolicy<R> implements IRetryPolicy<R> {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async shouldRetry(details: R): Promise<ShouldRetryResult> {
    return { shouldRetry: false };
  }

  public async invokeWithRetry(operation: RetryableOperation<R>): Promise<R> {
    // Just invoke the operation, don't attempt to retry it
    return operation();
  }
}
