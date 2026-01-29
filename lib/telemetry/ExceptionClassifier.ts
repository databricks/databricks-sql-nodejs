/**
 * Copyright (c) 2025 Databricks Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import AuthenticationError from '../errors/AuthenticationError';
import RetryError from '../errors/RetryError';

/**
 * Classifies exceptions as terminal (unrecoverable) vs retryable.
 *
 * Terminal exceptions should be flushed immediately to telemetry,
 * while retryable exceptions are buffered until statement completion.
 *
 * This follows the JDBC driver pattern of smart exception flushing
 * to optimize telemetry export efficiency while ensuring critical
 * errors are reported immediately.
 */
export default class ExceptionClassifier {
  /**
   * Determines if an exception is terminal (non-retryable).
   *
   * Terminal exceptions indicate unrecoverable failures that should
   * be reported immediately, such as authentication failures, invalid
   * requests, or resource not found errors.
   *
   * @param error - The error to classify
   * @returns true if the error is terminal, false otherwise
   */
  static isTerminal(error: Error): boolean {
    // Check for AuthenticationError (terminal)
    if (error instanceof AuthenticationError) {
      return true;
    }

    // Check for HTTP status codes in error properties
    // Supporting both 'statusCode' and 'status' property names for flexibility
    const statusCode = (error as any).statusCode ?? (error as any).status;

    if (typeof statusCode === 'number') {
      // Terminal HTTP status codes:
      // 400 - Bad Request (invalid request format)
      // 401 - Unauthorized (authentication required)
      // 403 - Forbidden (permission denied)
      // 404 - Not Found (resource does not exist)
      return statusCode === 400 || statusCode === 401 || statusCode === 403 || statusCode === 404;
    }

    // Default to false for unknown error types
    return false;
  }

  /**
   * Determines if an exception is retryable.
   *
   * Retryable exceptions indicate transient failures that may succeed
   * on retry, such as rate limiting, server errors, or network timeouts.
   *
   * @param error - The error to classify
   * @returns true if the error is retryable, false otherwise
   */
  static isRetryable(error: Error): boolean {
    // Check for RetryError (explicitly retryable)
    if (error instanceof RetryError) {
      return true;
    }

    // Check for network timeout errors
    if (error.name === 'TimeoutError' || error.message.includes('timeout')) {
      return true;
    }

    // Check for HTTP status codes in error properties
    // Supporting both 'statusCode' and 'status' property names for flexibility
    const statusCode = (error as any).statusCode ?? (error as any).status;

    if (typeof statusCode === 'number') {
      // Retryable HTTP status codes:
      // 429 - Too Many Requests (rate limiting)
      // 500 - Internal Server Error
      // 502 - Bad Gateway
      // 503 - Service Unavailable
      // 504 - Gateway Timeout
      return statusCode === 429 || statusCode === 500 || statusCode === 502 || statusCode === 503 || statusCode === 504;
    }

    // Default to false for unknown error types
    return false;
  }
}
