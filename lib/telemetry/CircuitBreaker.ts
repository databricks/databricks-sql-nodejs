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

export enum CircuitBreakerState {
  CLOSED = 'CLOSED',
  OPEN = 'OPEN',
  HALF_OPEN = 'HALF_OPEN',
}

export interface CircuitBreakerConfig {
  failureThreshold: number;
  timeout: number;
  successThreshold: number;
}

export const DEFAULT_CIRCUIT_BREAKER_CONFIG: Readonly<CircuitBreakerConfig> = Object.freeze({
  failureThreshold: 5,
  timeout: 60000,
  successThreshold: 2,
});

export const CIRCUIT_BREAKER_OPEN_CODE = 'CIRCUIT_BREAKER_OPEN' as const;

/**
 * Thrown when execute() is called while the breaker is OPEN or a HALF_OPEN
 * probe is already in flight. Callers identify the condition via
 * `instanceof CircuitBreakerOpenError` or `err.code === CIRCUIT_BREAKER_OPEN_CODE`
 * rather than string-matching the message.
 */
export class CircuitBreakerOpenError extends Error {
  readonly code = CIRCUIT_BREAKER_OPEN_CODE;

  constructor(message = 'Circuit breaker OPEN') {
    super(message);
    this.name = 'CircuitBreakerOpenError';
  }
}

export class CircuitBreaker {
  private state: CircuitBreakerState = CircuitBreakerState.CLOSED;

  private failureCount = 0;

  private successCount = 0;

  private nextAttempt?: Date;

  private halfOpenInflight = 0;

  private readonly config: CircuitBreakerConfig;

  constructor(private context: IClientContext, config?: Partial<CircuitBreakerConfig>) {
    this.config = {
      ...DEFAULT_CIRCUIT_BREAKER_CONFIG,
      ...config,
    };
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    const admitted = this.tryAdmit();
    if (!admitted) {
      throw new CircuitBreakerOpenError();
    }

    const { wasHalfOpenProbe } = admitted;

    try {
      const result = await operation();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    } finally {
      if (wasHalfOpenProbe && this.halfOpenInflight > 0) {
        this.halfOpenInflight -= 1;
      }
    }
  }

  /**
   * Synchronous admission check. Returning `null` means "reject". Returning
   * an object means the caller is admitted; `wasHalfOpenProbe` indicates
   * whether this admission consumed the single HALF_OPEN probe slot so the
   * caller can decrement it in `finally`.
   *
   * Running this as a single synchronous block is what prevents the
   * concurrent-probe race that existed in the previous implementation.
   */
  private tryAdmit(): { wasHalfOpenProbe: boolean } | null {
    const logger = this.context.getLogger();

    if (this.state === CircuitBreakerState.OPEN) {
      if (this.nextAttempt && Date.now() < this.nextAttempt.getTime()) {
        return null;
      }
      this.state = CircuitBreakerState.HALF_OPEN;
      this.successCount = 0;
      this.halfOpenInflight = 0;
      logger.log(LogLevel.debug, 'Circuit breaker transitioned to HALF_OPEN');
    }

    if (this.state === CircuitBreakerState.HALF_OPEN) {
      if (this.halfOpenInflight > 0) {
        return null;
      }
      this.halfOpenInflight += 1;
      return { wasHalfOpenProbe: true };
    }

    return { wasHalfOpenProbe: false };
  }

  getState(): CircuitBreakerState {
    return this.state;
  }

  getFailureCount(): number {
    return this.failureCount;
  }

  getSuccessCount(): number {
    return this.successCount;
  }

  private onSuccess(): void {
    const logger = this.context.getLogger();

    this.failureCount = 0;

    if (this.state === CircuitBreakerState.HALF_OPEN) {
      this.successCount += 1;
      logger.log(
        LogLevel.debug,
        `Circuit breaker success in HALF_OPEN (${this.successCount}/${this.config.successThreshold})`,
      );

      if (this.successCount >= this.config.successThreshold) {
        this.state = CircuitBreakerState.CLOSED;
        this.successCount = 0;
        this.nextAttempt = undefined;
        logger.log(LogLevel.debug, 'Circuit breaker transitioned to CLOSED');
      }
    }
  }

  private onFailure(): void {
    const logger = this.context.getLogger();

    this.failureCount += 1;
    this.successCount = 0;

    logger.log(LogLevel.debug, `Circuit breaker failure (${this.failureCount}/${this.config.failureThreshold})`);

    if (this.state === CircuitBreakerState.HALF_OPEN || this.failureCount >= this.config.failureThreshold) {
      this.state = CircuitBreakerState.OPEN;
      this.nextAttempt = new Date(Date.now() + this.config.timeout);
      logger.log(
        LogLevel.warn,
        `Telemetry circuit breaker OPEN after ${this.failureCount} failures (will retry after ${this.config.timeout}ms)`,
      );
    }
  }
}

export class CircuitBreakerRegistry {
  private breakers: Map<string, CircuitBreaker>;

  constructor(private context: IClientContext) {
    this.breakers = new Map();
  }

  getCircuitBreaker(host: string, config?: Partial<CircuitBreakerConfig>): CircuitBreaker {
    let breaker = this.breakers.get(host);
    if (!breaker) {
      breaker = new CircuitBreaker(this.context, config);
      this.breakers.set(host, breaker);
      const logger = this.context.getLogger();
      logger.log(LogLevel.debug, `Created circuit breaker for host: ${host}`);
    } else if (config) {
      const logger = this.context.getLogger();
      logger.log(LogLevel.debug, `Circuit breaker for host ${host} already exists; provided config will be ignored`);
    }
    return breaker;
  }

  getAllBreakers(): Map<string, CircuitBreaker> {
    return new Map(this.breakers);
  }

  removeCircuitBreaker(host: string): void {
    this.breakers.delete(host);
    const logger = this.context.getLogger();
    logger.log(LogLevel.debug, `Removed circuit breaker for host: ${host}`);
  }

  clear(): void {
    this.breakers.clear();
  }
}
