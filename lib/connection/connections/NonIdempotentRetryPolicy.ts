import HttpRetryPolicy from './HttpRetryPolicy';
import { HttpTransactionDetails } from '../contracts/IConnectionProvider';
import { RetryableOperation } from '../contracts/IRetryPolicy';
import RetryError from '../../errors/RetryError';

// Raised before a connection exists, so no request bytes were written. Failures on a connected
// socket (ECONNRESET, EPIPE, timeouts, ...) are excluded: the server may already have the request.
const UNSENT_REQUEST_ERROR_CODES = new Set(['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN']);

// For operations that are not safe to repeat: retries only requests the server did not run, i.e.
// requests that never left the client, a 429, or a 503 with `Retry-After` (the server asks for a
// retry). Other failures are ambiguous and are not retried. Same rules as the kernel backend.
export default class NonIdempotentRetryPolicy extends HttpRetryPolicy {
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

  protected isRetryable({ response }: HttpTransactionDetails): boolean {
    return response.status === 429 || (response.status === 503 && response.headers.has('Retry-After'));
  }

  protected isRetryableNetworkError(error: unknown): boolean {
    const code = (error as { code?: unknown } | undefined)?.code;
    return typeof code === 'string' && UNSENT_REQUEST_ERROR_CODES.has(code);
  }
}
