import HttpRetryPolicy from './HttpRetryPolicy';
import { HttpTransactionDetails } from '../contracts/IConnectionProvider';
import { RetryableOperation } from '../contracts/IRetryPolicy';
import RetryError from '../../errors/RetryError';

// Raised before a connection exists, so no request bytes were written. Failures on a connected
// socket (ECONNRESET, EPIPE, timeouts, ...) are excluded: the server may already have the request.
const UNSENT_REQUEST_ERROR_CODES = new Set(['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN']);

// For operations that are not safe to repeat: retries only requests that never left the client.
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

  protected isRetryable(): boolean {
    return false;
  }

  protected isRetryableNetworkError(error: unknown): boolean {
    const code = (error as { code?: unknown } | undefined)?.code;
    return typeof code === 'string' && UNSENT_REQUEST_ERROR_CODES.has(code);
  }
}
