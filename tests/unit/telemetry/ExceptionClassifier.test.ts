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

import { expect } from 'chai';
import ExceptionClassifier from '../../../lib/telemetry/ExceptionClassifier';
import AuthenticationError from '../../../lib/errors/AuthenticationError';
import RetryError, { RetryErrorCode } from '../../../lib/errors/RetryError';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

describe('ExceptionClassifier', () => {
  describe('isTerminal()', () => {
    describe('AuthenticationError', () => {
      it('should identify AuthenticationError as terminal', () => {
        const error = new AuthenticationError('Authentication failed');
        expect(ExceptionClassifier.isTerminal(error)).to.be.true;
      });
    });

    describe('HTTP 401 Unauthorized', () => {
      it('should identify 401 status code as terminal', () => {
        const error = new Error('Unauthorized');
        (error as any).statusCode = 401;
        expect(ExceptionClassifier.isTerminal(error)).to.be.true;
      });

      it('should identify 401 status property as terminal', () => {
        const error = new Error('Unauthorized');
        (error as any).status = 401;
        expect(ExceptionClassifier.isTerminal(error)).to.be.true;
      });
    });

    describe('HTTP 403 Forbidden', () => {
      it('should identify 403 status code as terminal', () => {
        const error = new Error('Forbidden');
        (error as any).statusCode = 403;
        expect(ExceptionClassifier.isTerminal(error)).to.be.true;
      });

      it('should identify 403 status property as terminal', () => {
        const error = new Error('Forbidden');
        (error as any).status = 403;
        expect(ExceptionClassifier.isTerminal(error)).to.be.true;
      });
    });

    describe('HTTP 404 Not Found', () => {
      it('should identify 404 status code as terminal', () => {
        const error = new Error('Not Found');
        (error as any).statusCode = 404;
        expect(ExceptionClassifier.isTerminal(error)).to.be.true;
      });

      it('should identify 404 status property as terminal', () => {
        const error = new Error('Not Found');
        (error as any).status = 404;
        expect(ExceptionClassifier.isTerminal(error)).to.be.true;
      });
    });

    describe('HTTP 400 Bad Request', () => {
      it('should identify 400 status code as terminal', () => {
        const error = new Error('Bad Request');
        (error as any).statusCode = 400;
        expect(ExceptionClassifier.isTerminal(error)).to.be.true;
      });

      it('should identify 400 status property as terminal', () => {
        const error = new Error('Bad Request');
        (error as any).status = 400;
        expect(ExceptionClassifier.isTerminal(error)).to.be.true;
      });
    });

    describe('Non-terminal errors', () => {
      it('should return false for 429 status code', () => {
        const error = new Error('Too Many Requests');
        (error as any).statusCode = 429;
        expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      });

      it('should return false for 500 status code', () => {
        const error = new Error('Internal Server Error');
        (error as any).statusCode = 500;
        expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      });

      it('should return false for 502 status code', () => {
        const error = new Error('Bad Gateway');
        (error as any).statusCode = 502;
        expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      });

      it('should return false for 503 status code', () => {
        const error = new Error('Service Unavailable');
        (error as any).statusCode = 503;
        expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      });

      it('should return false for 504 status code', () => {
        const error = new Error('Gateway Timeout');
        (error as any).statusCode = 504;
        expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      });

      it('should return false for RetryError', () => {
        const error = new RetryError(RetryErrorCode.AttemptsExceeded);
        expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      });

      it('should return false for generic Error', () => {
        const error = new Error('Generic error');
        expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      });

      it('should return false for HiveDriverError', () => {
        const error = new HiveDriverError('Driver error');
        expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      });

      it('should return false for error without status code', () => {
        const error = new Error('No status code');
        expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      });
    });
  });

  describe('isRetryable()', () => {
    describe('RetryError', () => {
      it('should identify RetryError with AttemptsExceeded as retryable', () => {
        const error = new RetryError(RetryErrorCode.AttemptsExceeded);
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });

      it('should identify RetryError with TimeoutExceeded as retryable', () => {
        const error = new RetryError(RetryErrorCode.TimeoutExceeded);
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });
    });

    describe('Network timeout errors', () => {
      it('should identify TimeoutError by name as retryable', () => {
        const error = new Error('Operation timed out');
        error.name = 'TimeoutError';
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });

      it('should identify timeout by message as retryable', () => {
        const error = new Error('Connection timeout occurred');
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });

      it('should identify timeout in lowercase message as retryable', () => {
        const error = new Error('Request timeout');
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });
    });

    describe('HTTP 429 Too Many Requests', () => {
      it('should identify 429 status code as retryable', () => {
        const error = new Error('Too Many Requests');
        (error as any).statusCode = 429;
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });

      it('should identify 429 status property as retryable', () => {
        const error = new Error('Too Many Requests');
        (error as any).status = 429;
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });
    });

    describe('HTTP 500 Internal Server Error', () => {
      it('should identify 500 status code as retryable', () => {
        const error = new Error('Internal Server Error');
        (error as any).statusCode = 500;
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });

      it('should identify 500 status property as retryable', () => {
        const error = new Error('Internal Server Error');
        (error as any).status = 500;
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });
    });

    describe('HTTP 502 Bad Gateway', () => {
      it('should identify 502 status code as retryable', () => {
        const error = new Error('Bad Gateway');
        (error as any).statusCode = 502;
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });

      it('should identify 502 status property as retryable', () => {
        const error = new Error('Bad Gateway');
        (error as any).status = 502;
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });
    });

    describe('HTTP 503 Service Unavailable', () => {
      it('should identify 503 status code as retryable', () => {
        const error = new Error('Service Unavailable');
        (error as any).statusCode = 503;
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });

      it('should identify 503 status property as retryable', () => {
        const error = new Error('Service Unavailable');
        (error as any).status = 503;
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });
    });

    describe('HTTP 504 Gateway Timeout', () => {
      it('should identify 504 status code as retryable', () => {
        const error = new Error('Gateway Timeout');
        (error as any).statusCode = 504;
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });

      it('should identify 504 status property as retryable', () => {
        const error = new Error('Gateway Timeout');
        (error as any).status = 504;
        expect(ExceptionClassifier.isRetryable(error)).to.be.true;
      });
    });

    describe('Non-retryable errors', () => {
      it('should return false for 400 status code', () => {
        const error = new Error('Bad Request');
        (error as any).statusCode = 400;
        expect(ExceptionClassifier.isRetryable(error)).to.be.false;
      });

      it('should return false for 401 status code', () => {
        const error = new Error('Unauthorized');
        (error as any).statusCode = 401;
        expect(ExceptionClassifier.isRetryable(error)).to.be.false;
      });

      it('should return false for 403 status code', () => {
        const error = new Error('Forbidden');
        (error as any).statusCode = 403;
        expect(ExceptionClassifier.isRetryable(error)).to.be.false;
      });

      it('should return false for 404 status code', () => {
        const error = new Error('Not Found');
        (error as any).statusCode = 404;
        expect(ExceptionClassifier.isRetryable(error)).to.be.false;
      });

      it('should return false for AuthenticationError', () => {
        const error = new AuthenticationError('Authentication failed');
        expect(ExceptionClassifier.isRetryable(error)).to.be.false;
      });

      it('should return false for generic Error', () => {
        const error = new Error('Generic error');
        expect(ExceptionClassifier.isRetryable(error)).to.be.false;
      });

      it('should return false for HiveDriverError', () => {
        const error = new HiveDriverError('Driver error');
        expect(ExceptionClassifier.isRetryable(error)).to.be.false;
      });

      it('should return false for error without status code', () => {
        const error = new Error('No status code');
        expect(ExceptionClassifier.isRetryable(error)).to.be.false;
      });
    });
  });

  describe('Unknown error types', () => {
    it('should handle unknown error types gracefully in isTerminal', () => {
      const error = new Error('Unknown error');
      (error as any).someOtherProperty = 'value';
      expect(ExceptionClassifier.isTerminal(error)).to.be.false;
    });

    it('should handle unknown error types gracefully in isRetryable', () => {
      const error = new Error('Unknown error');
      (error as any).someOtherProperty = 'value';
      expect(ExceptionClassifier.isRetryable(error)).to.be.false;
    });

    it('should return false for both isTerminal and isRetryable when uncertain', () => {
      const error = new Error('Ambiguous error');
      expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      expect(ExceptionClassifier.isRetryable(error)).to.be.false;
    });
  });

  describe('Edge cases', () => {
    it('should handle error with non-numeric status code', () => {
      const error = new Error('Invalid status');
      (error as any).statusCode = 'not-a-number';
      expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      expect(ExceptionClassifier.isRetryable(error)).to.be.false;
    });

    it('should handle error with undefined status code', () => {
      const error = new Error('No status');
      (error as any).statusCode = undefined;
      expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      expect(ExceptionClassifier.isRetryable(error)).to.be.false;
    });

    it('should handle error with null status code', () => {
      const error = new Error('Null status');
      (error as any).statusCode = null;
      expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      expect(ExceptionClassifier.isRetryable(error)).to.be.false;
    });

    it('should prioritize statusCode over status property', () => {
      const error = new Error('Multiple status properties');
      (error as any).statusCode = 401; // terminal
      (error as any).status = 500; // retryable
      expect(ExceptionClassifier.isTerminal(error)).to.be.true;
      expect(ExceptionClassifier.isRetryable(error)).to.be.false;
    });

    it('should handle error with both statusCode and status set to same value', () => {
      const error = new Error('Duplicate status');
      (error as any).statusCode = 503;
      (error as any).status = 503;
      expect(ExceptionClassifier.isTerminal(error)).to.be.false;
      expect(ExceptionClassifier.isRetryable(error)).to.be.true;
    });
  });

  describe('No dependencies on other telemetry components', () => {
    it('should be a standalone static class', () => {
      // ExceptionClassifier should not require instantiation
      expect(typeof ExceptionClassifier.isTerminal).to.equal('function');
      expect(typeof ExceptionClassifier.isRetryable).to.equal('function');
    });

    it('should not require any context or configuration', () => {
      // Should work without any setup
      const error = new Error('Test');
      expect(() => ExceptionClassifier.isTerminal(error)).to.not.throw();
      expect(() => ExceptionClassifier.isRetryable(error)).to.not.throw();
    });
  });
});
