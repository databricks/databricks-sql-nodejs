import HttpRetryPolicy, { isConnectError, isRetryableStatus } from './HttpRetryPolicy';
import { HttpTransactionDetails } from '../contracts/IConnectionProvider';
import { RetryableOperation } from '../contracts/IRetryPolicy';
import RetryError from '../../errors/RetryError';

// For operations that are not safe to repeat: retries only requests the server didn't run, as the kernel's
// non-idempotent strategy does: a failure to connect, a 429, or a 503 with `Retry-After`. Failures on a
// connected socket (ECONNRESET, EPIPE, timeouts, ...) are not retried: the server may already have the request.
export default class UnsentRequestRetryPolicy extends HttpRetryPolicy {
  public async invokeWithRetry(operation: RetryableOperation<HttpTransactionDetails>): Promise<HttpTransactionDetails> {
    try {
      return await super.invokeWithRetry(operation);
    } catch (error) {
      // When retries run out, surface the connection error itself rather than a RetryError
      if (error instanceof RetryError && this.isRetryableNetworkError(error.payload)) {
        throw error.payload;
      }
      throw error;
    }
  }

  protected isRetryable(details: HttpTransactionDetails): boolean {
    return isRetryableStatus(details, false);
  }

  protected isRetryableNetworkError(error: unknown): boolean {
    return isConnectError(error);
  }
}
