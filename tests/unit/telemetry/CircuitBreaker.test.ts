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
import sinon from 'sinon';
import {
  CircuitBreaker,
  CircuitBreakerRegistry,
  CircuitBreakerState,
  DEFAULT_CIRCUIT_BREAKER_CONFIG,
} from '../../../lib/telemetry/CircuitBreaker';
import ClientContextStub from '../.stubs/ClientContextStub';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

describe('CircuitBreaker', () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
    clock = sinon.useFakeTimers();
  });

  afterEach(() => {
    clock.restore();
  });

  describe('Initial state', () => {
    it('should start in CLOSED state', () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context);

      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
      expect(breaker.getFailureCount()).to.equal(0);
      expect(breaker.getSuccessCount()).to.equal(0);
    });

    it('should use default configuration', () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context);

      // Verify by checking behavior with default values
      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
    });

    it('should accept custom configuration', () => {
      const context = new ClientContextStub();
      const customConfig = {
        failureThreshold: 3,
        timeout: 30000,
        successThreshold: 1,
      };
      const breaker = new CircuitBreaker(context, customConfig);

      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
    });
  });

  describe('execute() in CLOSED state', () => {
    it('should execute operation successfully', async () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context);
      const operation = sinon.stub().resolves('success');

      const result = await breaker.execute(operation);

      expect(result).to.equal('success');
      expect(operation.calledOnce).to.be.true;
      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
      expect(breaker.getFailureCount()).to.equal(0);
    });

    it('should increment failure count on operation failure', async () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context);
      const operation = sinon.stub().rejects(new Error('Operation failed'));

      try {
        await breaker.execute(operation);
        expect.fail('Should have thrown error');
      } catch (error: any) {
        expect(error.message).to.equal('Operation failed');
      }

      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
      expect(breaker.getFailureCount()).to.equal(1);
    });

    it('should reset failure count on success after failures', async () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context);

      // Fail twice
      const failOp = sinon.stub().rejects(new Error('Failed'));
      try {
        await breaker.execute(failOp);
      } catch {}
      try {
        await breaker.execute(failOp);
      } catch {}

      expect(breaker.getFailureCount()).to.equal(2);

      // Then succeed
      const successOp = sinon.stub().resolves('success');
      await breaker.execute(successOp);

      expect(breaker.getFailureCount()).to.equal(0);
      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
    });
  });

  describe('Transition to OPEN state', () => {
    it('should open after configured failure threshold (default 5)', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const breaker = new CircuitBreaker(context);
      const operation = sinon.stub().rejects(new Error('Failed'));

      // Fail 5 times (default threshold)
      for (let i = 0; i < 5; i++) {
        try {
          await breaker.execute(operation);
        } catch {}
      }

      expect(breaker.getState()).to.equal(CircuitBreakerState.OPEN);
      expect(breaker.getFailureCount()).to.equal(5);
      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/Circuit breaker transitioned to OPEN/))).to.be.true;

      logSpy.restore();
    });

    it('should open after custom failure threshold', async () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context, { failureThreshold: 3 });
      const operation = sinon.stub().rejects(new Error('Failed'));

      // Fail 3 times
      for (let i = 0; i < 3; i++) {
        try {
          await breaker.execute(operation);
        } catch {}
      }

      expect(breaker.getState()).to.equal(CircuitBreakerState.OPEN);
      expect(breaker.getFailureCount()).to.equal(3);
    });

    it('should log state transition at debug level', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const breaker = new CircuitBreaker(context);
      const operation = sinon.stub().rejects(new Error('Failed'));

      // Fail 5 times to open circuit
      for (let i = 0; i < 5; i++) {
        try {
          await breaker.execute(operation);
        } catch {}
      }

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/Circuit breaker transitioned to OPEN/))).to.be.true;

      logSpy.restore();
    });
  });

  describe('execute() in OPEN state', () => {
    it('should reject operations immediately when OPEN', async () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context);
      const operation = sinon.stub().rejects(new Error('Failed'));

      // Open the circuit
      for (let i = 0; i < 5; i++) {
        try {
          await breaker.execute(operation);
        } catch {}
      }

      expect(breaker.getState()).to.equal(CircuitBreakerState.OPEN);

      // Try to execute another operation
      const newOperation = sinon.stub().resolves('success');
      try {
        await breaker.execute(newOperation);
        expect.fail('Should have thrown error');
      } catch (error: any) {
        expect(error.message).to.equal('Circuit breaker OPEN');
      }

      // Operation should not have been called
      expect(newOperation.called).to.be.false;
    });

    it('should stay OPEN for configured timeout (default 60s)', async () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context);
      const operation = sinon.stub().rejects(new Error('Failed'));

      // Open the circuit
      for (let i = 0; i < 5; i++) {
        try {
          await breaker.execute(operation);
        } catch {}
      }

      expect(breaker.getState()).to.equal(CircuitBreakerState.OPEN);

      // Advance time by 59 seconds (less than timeout)
      clock.tick(59000);

      // Should still be OPEN
      const newOperation = sinon.stub().resolves('success');
      try {
        await breaker.execute(newOperation);
        expect.fail('Should have thrown error');
      } catch (error: any) {
        expect(error.message).to.equal('Circuit breaker OPEN');
      }

      expect(breaker.getState()).to.equal(CircuitBreakerState.OPEN);
    });
  });

  describe('Transition to HALF_OPEN state', () => {
    it('should transition to HALF_OPEN after timeout', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const breaker = new CircuitBreaker(context);
      const operation = sinon.stub().rejects(new Error('Failed'));

      // Open the circuit
      for (let i = 0; i < 5; i++) {
        try {
          await breaker.execute(operation);
        } catch {}
      }

      expect(breaker.getState()).to.equal(CircuitBreakerState.OPEN);

      // Advance time past timeout (60 seconds)
      clock.tick(60001);

      // Next operation should transition to HALF_OPEN
      const successOperation = sinon.stub().resolves('success');
      await breaker.execute(successOperation);

      expect(logSpy.calledWith(LogLevel.debug, 'Circuit breaker transitioned to HALF_OPEN')).to.be.true;

      logSpy.restore();
    });

    it('should use custom timeout', async () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context, { timeout: 30000 }); // 30 seconds
      const operation = sinon.stub().rejects(new Error('Failed'));

      // Open the circuit
      for (let i = 0; i < 5; i++) {
        try {
          await breaker.execute(operation);
        } catch {}
      }

      // Advance time by 25 seconds (less than custom timeout)
      clock.tick(25000);

      const newOperation = sinon.stub().resolves('success');
      try {
        await breaker.execute(newOperation);
        expect.fail('Should have thrown error');
      } catch (error: any) {
        expect(error.message).to.equal('Circuit breaker OPEN');
      }

      // Advance past custom timeout
      clock.tick(5001);

      // Should now transition to HALF_OPEN
      const successOperation = sinon.stub().resolves('success');
      const result = await breaker.execute(successOperation);
      expect(result).to.equal('success');
      expect(breaker.getState()).to.equal(CircuitBreakerState.HALF_OPEN);
    });
  });

  describe('execute() in HALF_OPEN state', () => {
    async function openAndWaitForHalfOpen(breaker: CircuitBreaker): Promise<void> {
      const operation = sinon.stub().rejects(new Error('Failed'));
      // Open the circuit
      for (let i = 0; i < 5; i++) {
        try {
          await breaker.execute(operation);
        } catch {}
      }
      // Wait for timeout
      clock.tick(60001);
    }

    it('should allow test requests in HALF_OPEN state', async () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context);

      await openAndWaitForHalfOpen(breaker);

      // Execute first test request
      const operation = sinon.stub().resolves('success');
      const result = await breaker.execute(operation);

      expect(result).to.equal('success');
      expect(operation.calledOnce).to.be.true;
      expect(breaker.getState()).to.equal(CircuitBreakerState.HALF_OPEN);
    });

    it('should close after configured successes (default 2)', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const breaker = new CircuitBreaker(context);

      await openAndWaitForHalfOpen(breaker);

      // First success
      const operation1 = sinon.stub().resolves('success1');
      await breaker.execute(operation1);
      expect(breaker.getState()).to.equal(CircuitBreakerState.HALF_OPEN);
      expect(breaker.getSuccessCount()).to.equal(1);

      // Second success should close the circuit
      const operation2 = sinon.stub().resolves('success2');
      await breaker.execute(operation2);
      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
      expect(breaker.getSuccessCount()).to.equal(0); // Reset after closing
      expect(logSpy.calledWith(LogLevel.debug, 'Circuit breaker transitioned to CLOSED')).to.be.true;

      logSpy.restore();
    });

    it('should close after custom success threshold', async () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context, { successThreshold: 3 });

      await openAndWaitForHalfOpen(breaker);

      // Need 3 successes
      for (let i = 0; i < 2; i++) {
        const operation = sinon.stub().resolves(`success${i}`);
        await breaker.execute(operation);
        expect(breaker.getState()).to.equal(CircuitBreakerState.HALF_OPEN);
      }

      // Third success should close
      const operation3 = sinon.stub().resolves('success3');
      await breaker.execute(operation3);
      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
    });

    it('should reopen if operation fails in HALF_OPEN state', async () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context);

      await openAndWaitForHalfOpen(breaker);

      // First success
      const successOp = sinon.stub().resolves('success');
      await breaker.execute(successOp);
      expect(breaker.getState()).to.equal(CircuitBreakerState.HALF_OPEN);
      expect(breaker.getSuccessCount()).to.equal(1);

      // Failure should reset success count but not immediately open
      const failOp = sinon.stub().rejects(new Error('Failed'));
      try {
        await breaker.execute(failOp);
      } catch {}

      expect(breaker.getSuccessCount()).to.equal(0); // Reset
      expect(breaker.getFailureCount()).to.equal(1);
      expect(breaker.getState()).to.equal(CircuitBreakerState.HALF_OPEN);
    });

    it('should track failures and eventually reopen circuit', async () => {
      const context = new ClientContextStub();
      const breaker = new CircuitBreaker(context);

      await openAndWaitForHalfOpen(breaker);

      // Now in HALF_OPEN, fail 5 times to reopen
      const failOp = sinon.stub().rejects(new Error('Failed'));
      for (let i = 0; i < 5; i++) {
        try {
          await breaker.execute(failOp);
        } catch {}
      }

      expect(breaker.getState()).to.equal(CircuitBreakerState.OPEN);
    });
  });

  describe('State transitions logging', () => {
    it('should log all state transitions at debug level', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const breaker = new CircuitBreaker(context);

      // Open circuit
      const failOp = sinon.stub().rejects(new Error('Failed'));
      for (let i = 0; i < 5; i++) {
        try {
          await breaker.execute(failOp);
        } catch {}
      }

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/Circuit breaker transitioned to OPEN/))).to.be.true;

      // Wait for timeout
      clock.tick(60001);

      // Transition to HALF_OPEN
      const successOp = sinon.stub().resolves('success');
      await breaker.execute(successOp);

      expect(logSpy.calledWith(LogLevel.debug, 'Circuit breaker transitioned to HALF_OPEN')).to.be.true;

      // Close circuit
      await breaker.execute(successOp);

      expect(logSpy.calledWith(LogLevel.debug, 'Circuit breaker transitioned to CLOSED')).to.be.true;

      // Verify no console logging
      expect(logSpy.neverCalledWith(LogLevel.error, sinon.match.any)).to.be.true;
      expect(logSpy.neverCalledWith(LogLevel.warn, sinon.match.any)).to.be.true;
      expect(logSpy.neverCalledWith(LogLevel.info, sinon.match.any)).to.be.true;

      logSpy.restore();
    });
  });
});

describe('CircuitBreakerRegistry', () => {
  describe('getCircuitBreaker', () => {
    it('should create a new circuit breaker for a host', () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const host = 'test-host.databricks.com';

      const breaker = registry.getCircuitBreaker(host);

      expect(breaker).to.not.be.undefined;
      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
    });

    it('should return the same circuit breaker for the same host', () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const host = 'test-host.databricks.com';

      const breaker1 = registry.getCircuitBreaker(host);
      const breaker2 = registry.getCircuitBreaker(host);

      expect(breaker1).to.equal(breaker2); // Same instance
    });

    it('should create separate circuit breakers for different hosts', () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const host1 = 'host1.databricks.com';
      const host2 = 'host2.databricks.com';

      const breaker1 = registry.getCircuitBreaker(host1);
      const breaker2 = registry.getCircuitBreaker(host2);

      expect(breaker1).to.not.equal(breaker2);
    });

    it('should accept custom configuration', () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const host = 'test-host.databricks.com';
      const customConfig = { failureThreshold: 3 };

      const breaker = registry.getCircuitBreaker(host, customConfig);

      expect(breaker).to.not.be.undefined;
      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
    });

    it('should log circuit breaker creation at debug level', () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const registry = new CircuitBreakerRegistry(context);
      const host = 'test-host.databricks.com';

      registry.getCircuitBreaker(host);

      expect(logSpy.calledWith(LogLevel.debug, `Created circuit breaker for host: ${host}`)).to.be.true;

      logSpy.restore();
    });
  });

  describe('Per-host isolation', () => {
    it('should isolate failures between hosts', async () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const host1 = 'host1.databricks.com';
      const host2 = 'host2.databricks.com';

      const breaker1 = registry.getCircuitBreaker(host1);
      const breaker2 = registry.getCircuitBreaker(host2);

      // Fail breaker1 5 times to open it
      const failOp = sinon.stub().rejects(new Error('Failed'));
      for (let i = 0; i < 5; i++) {
        try {
          await breaker1.execute(failOp);
        } catch {}
      }

      expect(breaker1.getState()).to.equal(CircuitBreakerState.OPEN);
      expect(breaker2.getState()).to.equal(CircuitBreakerState.CLOSED);

      // breaker2 should still work
      const successOp = sinon.stub().resolves('success');
      const result = await breaker2.execute(successOp);
      expect(result).to.equal('success');
      expect(breaker2.getState()).to.equal(CircuitBreakerState.CLOSED);
    });

    it('should track separate failure counts per host', async () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const host1 = 'host1.databricks.com';
      const host2 = 'host2.databricks.com';

      const breaker1 = registry.getCircuitBreaker(host1);
      const breaker2 = registry.getCircuitBreaker(host2);

      // Fail breaker1 twice
      const failOp = sinon.stub().rejects(new Error('Failed'));
      for (let i = 0; i < 2; i++) {
        try {
          await breaker1.execute(failOp);
        } catch {}
      }

      // Fail breaker2 three times
      for (let i = 0; i < 3; i++) {
        try {
          await breaker2.execute(failOp);
        } catch {}
      }

      expect(breaker1.getFailureCount()).to.equal(2);
      expect(breaker2.getFailureCount()).to.equal(3);
    });
  });

  describe('getAllBreakers', () => {
    it('should return all registered circuit breakers', () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const host1 = 'host1.databricks.com';
      const host2 = 'host2.databricks.com';

      const breaker1 = registry.getCircuitBreaker(host1);
      const breaker2 = registry.getCircuitBreaker(host2);

      const allBreakers = registry.getAllBreakers();

      expect(allBreakers.size).to.equal(2);
      expect(allBreakers.get(host1)).to.equal(breaker1);
      expect(allBreakers.get(host2)).to.equal(breaker2);
    });

    it('should return empty map if no breakers registered', () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);

      const allBreakers = registry.getAllBreakers();

      expect(allBreakers.size).to.equal(0);
    });
  });

  describe('removeCircuitBreaker', () => {
    it('should remove circuit breaker for host', () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const host = 'test-host.databricks.com';

      registry.getCircuitBreaker(host);
      expect(registry.getAllBreakers().size).to.equal(1);

      registry.removeCircuitBreaker(host);
      expect(registry.getAllBreakers().size).to.equal(0);
    });

    it('should log circuit breaker removal at debug level', () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const registry = new CircuitBreakerRegistry(context);
      const host = 'test-host.databricks.com';

      registry.getCircuitBreaker(host);
      registry.removeCircuitBreaker(host);

      expect(logSpy.calledWith(LogLevel.debug, `Removed circuit breaker for host: ${host}`)).to.be.true;

      logSpy.restore();
    });

    it('should handle removing non-existent host gracefully', () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);

      expect(() => registry.removeCircuitBreaker('non-existent.com')).to.not.throw();
    });
  });

  describe('clear', () => {
    it('should remove all circuit breakers', () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);

      registry.getCircuitBreaker('host1.databricks.com');
      registry.getCircuitBreaker('host2.databricks.com');
      registry.getCircuitBreaker('host3.databricks.com');

      expect(registry.getAllBreakers().size).to.equal(3);

      registry.clear();

      expect(registry.getAllBreakers().size).to.equal(0);
    });
  });
});
