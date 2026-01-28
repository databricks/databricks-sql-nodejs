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

import IClientContext from '../contracts/IClientContext';
import { LogLevel } from '../contracts/IDBSQLLogger';

/**
 * States of the circuit breaker.
 */
export enum CircuitBreakerState {
  /** Normal operation, requests pass through */
  CLOSED = 'CLOSED',
  /** After threshold failures, all requests rejected immediately */
  OPEN = 'OPEN',
  /** After timeout, allows test requests to check if endpoint recovered */
  HALF_OPEN = 'HALF_OPEN',
}

/**
 * Configuration for circuit breaker behavior.
 */
export interface CircuitBreakerConfig {
  /** Number of consecutive failures before opening the circuit */
  failureThreshold: number;
  /** Time in milliseconds to wait before attempting recovery */
  timeout: number;
  /** Number of consecutive successes in HALF_OPEN state to close the circuit */
  successThreshold: number;
}

/**
 * Default circuit breaker configuration.
 */
export const DEFAULT_CIRCUIT_BREAKER_CONFIG: CircuitBreakerConfig = {
  failureThreshold: 5,
  timeout: 60000, // 1 minute
  successThreshold: 2,
};

/**
 * Circuit breaker for telemetry exporter.
 * Protects against failing telemetry endpoint with automatic recovery.
 *
 * States:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: After threshold failures, all requests rejected immediately
 * - HALF_OPEN: After timeout, allows test requests to check if endpoint recovered
 */
export class CircuitBreaker {
  private state: CircuitBreakerState = CircuitBreakerState.CLOSED;
  private failureCount = 0;
  private successCount = 0;
  private nextAttempt?: Date;
  private readonly config: CircuitBreakerConfig;

  constructor(
    private context: IClientContext,
    config?: Partial<CircuitBreakerConfig>
  ) {
    this.config = {
      ...DEFAULT_CIRCUIT_BREAKER_CONFIG,
      ...config,
    };
  }

  /**
   * Executes an operation with circuit breaker protection.
   *
   * @param operation The operation to execute
   * @returns Promise resolving to the operation result
   * @throws Error if circuit is OPEN or operation fails
   */
  async execute<T>(operation: () => Promise<T>): Promise<T> {
    const logger = this.context.getLogger();

    // Check if circuit is open
    if (this.state === CircuitBreakerState.OPEN) {
      if (this.nextAttempt && Date.now() < this.nextAttempt.getTime()) {
        throw new Error('Circuit breaker OPEN');
      }
      // Timeout expired, transition to HALF_OPEN
      this.state = CircuitBreakerState.HALF_OPEN;
      this.successCount = 0;
      logger.log(LogLevel.debug, 'Circuit breaker transitioned to HALF_OPEN');
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
   * Gets the current failure count.
   */
  getFailureCount(): number {
    return this.failureCount;
  }

  /**
   * Gets the current success count (relevant in HALF_OPEN state).
   */
  getSuccessCount(): number {
    return this.successCount;
  }

  /**
   * Handles successful operation execution.
   */
  private onSuccess(): void {
    const logger = this.context.getLogger();

    // Reset failure count on any success
    this.failureCount = 0;

    if (this.state === CircuitBreakerState.HALF_OPEN) {
      this.successCount++;
      logger.log(
        LogLevel.debug,
        `Circuit breaker success in HALF_OPEN (${this.successCount}/${this.config.successThreshold})`
      );

      if (this.successCount >= this.config.successThreshold) {
        // Transition to CLOSED
        this.state = CircuitBreakerState.CLOSED;
        this.successCount = 0;
        this.nextAttempt = undefined;
        logger.log(LogLevel.debug, 'Circuit breaker transitioned to CLOSED');
      }
    }
  }

  /**
   * Handles failed operation execution.
   */
  private onFailure(): void {
    const logger = this.context.getLogger();

    this.failureCount++;
    this.successCount = 0; // Reset success count on failure

    logger.log(
      LogLevel.debug,
      `Circuit breaker failure (${this.failureCount}/${this.config.failureThreshold})`
    );

    if (this.failureCount >= this.config.failureThreshold) {
      // Transition to OPEN
      this.state = CircuitBreakerState.OPEN;
      this.nextAttempt = new Date(Date.now() + this.config.timeout);
      logger.log(
        LogLevel.debug,
        `Circuit breaker transitioned to OPEN (will retry after ${this.config.timeout}ms)`
      );
    }
  }
}

/**
 * Manages circuit breakers per host.
 * Ensures each host has its own isolated circuit breaker to prevent
 * failures on one host from affecting telemetry to other hosts.
 */
export class CircuitBreakerRegistry {
  private breakers: Map<string, CircuitBreaker>;

  constructor(private context: IClientContext) {
    this.breakers = new Map();
  }

  /**
   * Gets or creates a circuit breaker for the specified host.
   *
   * @param host The host identifier (e.g., "workspace.cloud.databricks.com")
   * @param config Optional configuration overrides
   * @returns Circuit breaker for the host
   */
  getCircuitBreaker(host: string, config?: Partial<CircuitBreakerConfig>): CircuitBreaker {
    let breaker = this.breakers.get(host);
    if (!breaker) {
      breaker = new CircuitBreaker(this.context, config);
      this.breakers.set(host, breaker);
      const logger = this.context.getLogger();
      logger.log(LogLevel.debug, `Created circuit breaker for host: ${host}`);
    }
    return breaker;
  }

  /**
   * Gets all registered circuit breakers.
   * Useful for testing and diagnostics.
   */
  getAllBreakers(): Map<string, CircuitBreaker> {
    return new Map(this.breakers);
  }

  /**
   * Removes a circuit breaker for the specified host.
   * Useful for cleanup when a host is no longer in use.
   *
   * @param host The host identifier
   */
  removeCircuitBreaker(host: string): void {
    this.breakers.delete(host);
    const logger = this.context.getLogger();
    logger.log(LogLevel.debug, `Removed circuit breaker for host: ${host}`);
  }

  /**
   * Clears all circuit breakers.
   * Useful for testing.
   */
  clear(): void {
    this.breakers.clear();
  }
}
