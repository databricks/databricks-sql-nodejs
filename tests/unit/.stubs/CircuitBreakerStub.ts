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

import { CircuitBreakerState } from '../../../lib/telemetry/CircuitBreaker';

/**
 * Stub implementation of CircuitBreaker for testing.
 * Provides a simplified implementation that can be controlled in tests.
 */
export default class CircuitBreakerStub {
  private state: CircuitBreakerState = CircuitBreakerState.CLOSED;
  private failureCount = 0;
  private successCount = 0;
  public executeCallCount = 0;

  /**
   * Executes an operation with circuit breaker protection.
   * In stub mode, always executes the operation unless state is OPEN.
   */
  async execute<T>(operation: () => Promise<T>): Promise<T> {
    this.executeCallCount++;

    if (this.state === CircuitBreakerState.OPEN) {
      throw new Error('Circuit breaker OPEN');
    }

    try {
      const result = await operation();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  /**
   * Gets the current state of the circuit breaker.
   */
  getState(): CircuitBreakerState {
    return this.state;
  }

  /**
   * Sets the state (for testing purposes).
   */
  setState(state: CircuitBreakerState): void {
    this.state = state;
  }

  /**
   * Gets the current failure count.
   */
  getFailureCount(): number {
    return this.failureCount;
  }

  /**
   * Sets the failure count (for testing purposes).
   */
  setFailureCount(count: number): void {
    this.failureCount = count;
  }

  /**
   * Gets the current success count.
   */
  getSuccessCount(): number {
    return this.successCount;
  }

  /**
   * Resets all state (for testing purposes).
   */
  reset(): void {
    this.state = CircuitBreakerState.CLOSED;
    this.failureCount = 0;
    this.successCount = 0;
    this.executeCallCount = 0;
  }

  /**
   * Handles successful operation execution.
   */
  private onSuccess(): void {
    this.failureCount = 0;
    if (this.state === CircuitBreakerState.HALF_OPEN) {
      this.successCount++;
      if (this.successCount >= 2) {
        this.state = CircuitBreakerState.CLOSED;
        this.successCount = 0;
      }
    }
  }

  /**
   * Handles failed operation execution.
   */
  private onFailure(): void {
    this.failureCount++;
    this.successCount = 0;
    if (this.failureCount >= 5) {
      this.state = CircuitBreakerState.OPEN;
    }
  }
}

/**
 * Stub implementation of CircuitBreakerRegistry for testing.
 */
export class CircuitBreakerRegistryStub {
  private breakers: Map<string, CircuitBreakerStub>;

  constructor() {
    this.breakers = new Map();
  }

  /**
   * Gets or creates a circuit breaker for the specified host.
   */
  getCircuitBreaker(host: string): CircuitBreakerStub {
    let breaker = this.breakers.get(host);
    if (!breaker) {
      breaker = new CircuitBreakerStub();
      this.breakers.set(host, breaker);
    }
    return breaker;
  }

  /**
   * Gets all registered circuit breakers.
   */
  getAllBreakers(): Map<string, CircuitBreakerStub> {
    return new Map(this.breakers);
  }

  /**
   * Removes a circuit breaker for the specified host.
   */
  removeCircuitBreaker(host: string): void {
    this.breakers.delete(host);
  }

  /**
   * Clears all circuit breakers.
   */
  clear(): void {
    this.breakers.clear();
  }
}
