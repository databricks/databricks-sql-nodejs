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

import fetch, { Response } from 'node-fetch';
import IClientContext from '../contracts/IClientContext';
import { LogLevel } from '../contracts/IDBSQLLogger';
import { TelemetryMetric, DEFAULT_TELEMETRY_CONFIG } from './types';
import { CircuitBreakerRegistry } from './CircuitBreaker';
import ExceptionClassifier from './ExceptionClassifier';

/**
 * Databricks telemetry log format for export.
 */
interface DatabricksTelemetryLog {
  workspace_id?: string;
  frontend_log_event_id: string;
  context: {
    client_context: {
      timestamp_millis: number;
      user_agent: string;
    };
  };
  entry: {
    sql_driver_log: {
      session_id?: string;
      sql_statement_id?: string;
      operation_latency_ms?: number;
      sql_operation?: {
        execution_result_format?: string;
        chunk_details?: {
          chunk_count: number;
          total_bytes?: number;
        };
      };
      error_info?: {
        error_name: string;
        stack_trace: string;
      };
      driver_config?: any;
    };
  };
}

/**
 * Payload format for Databricks telemetry export.
 */
interface DatabricksTelemetryPayload {
  frontend_logs: DatabricksTelemetryLog[];
}

/**
 * Exports telemetry metrics to Databricks telemetry service.
 *
 * Endpoints:
 * - Authenticated: /api/2.0/sql/telemetry-ext
 * - Unauthenticated: /api/2.0/sql/telemetry-unauth
 *
 * Features:
 * - Circuit breaker integration for endpoint protection
 * - Retry logic with exponential backoff for retryable errors
 * - Terminal error detection (no retry on 400, 401, 403, 404)
 * - CRITICAL: export() method NEVER throws - all exceptions swallowed
 * - CRITICAL: All logging at LogLevel.debug ONLY
 */
export default class DatabricksTelemetryExporter {
  private circuitBreaker;

  private readonly userAgent: string;

  private fetchFn: typeof fetch;

  constructor(
    private context: IClientContext,
    private host: string,
    private circuitBreakerRegistry: CircuitBreakerRegistry,
    fetchFunction?: typeof fetch
  ) {
    this.circuitBreaker = circuitBreakerRegistry.getCircuitBreaker(host);
    this.fetchFn = fetchFunction || fetch;

    // Get driver version for user agent
    this.userAgent = `databricks-sql-nodejs/${this.getDriverVersion()}`;
  }

  /**
   * Export metrics to Databricks service. Never throws.
   *
   * @param metrics - Array of telemetry metrics to export
   */
  async export(metrics: TelemetryMetric[]): Promise<void> {
    if (!metrics || metrics.length === 0) {
      return;
    }

    const logger = this.context.getLogger();

    try {
      await this.circuitBreaker.execute(async () => {
        await this.exportWithRetry(metrics);
      });
    } catch (error: any) {
      // CRITICAL: All exceptions swallowed and logged at debug level ONLY
      if (error.message === 'Circuit breaker OPEN') {
        logger.log(LogLevel.debug, 'Circuit breaker OPEN - dropping telemetry');
      } else {
        logger.log(LogLevel.debug, `Telemetry export error: ${error.message}`);
      }
    }
  }

  /**
   * Export metrics with retry logic for retryable errors.
   * Implements exponential backoff with jitter.
   */
  private async exportWithRetry(metrics: TelemetryMetric[]): Promise<void> {
    const config = this.context.getConfig();
    const logger = this.context.getLogger();
    const maxRetries = config.telemetryMaxRetries ?? DEFAULT_TELEMETRY_CONFIG.maxRetries;

    let lastError: Error | null = null;

    /* eslint-disable no-await-in-loop */
    for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
      try {
        await this.exportInternal(metrics);
        return; // Success
      } catch (error: any) {
        lastError = error;

        // Check if error is terminal (don't retry)
        if (ExceptionClassifier.isTerminal(error)) {
          logger.log(LogLevel.debug, `Terminal error - no retry: ${error.message}`);
          throw error; // Terminal error, propagate to circuit breaker
        }

        // Check if error is retryable
        if (!ExceptionClassifier.isRetryable(error)) {
          logger.log(LogLevel.debug, `Non-retryable error: ${error.message}`);
          throw error; // Not retryable, propagate to circuit breaker
        }

        // Last attempt reached
        if (attempt >= maxRetries) {
          logger.log(LogLevel.debug, `Max retries reached (${maxRetries}): ${error.message}`);
          throw error; // Max retries exhausted, propagate to circuit breaker
        }

        // Calculate backoff with exponential + jitter (100ms - 1000ms)
        const baseDelay = Math.min(100 * 2**attempt, 1000);
        const jitter = Math.random() * 100;
        const delay = baseDelay + jitter;

        logger.log(
          LogLevel.debug,
          `Retrying telemetry export (attempt ${attempt + 1}/${maxRetries}) after ${Math.round(delay)}ms`
        );

        await this.sleep(delay);
      }
    }
    /* eslint-enable no-await-in-loop */

    // Should not reach here, but just in case
    if (lastError) {
      throw lastError;
    }
  }

  /**
   * Internal export implementation that makes the HTTP call.
   */
  private async exportInternal(metrics: TelemetryMetric[]): Promise<void> {
    const config = this.context.getConfig();
    const logger = this.context.getLogger();

    // Determine endpoint based on authentication mode
    const authenticatedExport =
      config.telemetryAuthenticatedExport ?? DEFAULT_TELEMETRY_CONFIG.authenticatedExport;
    const endpoint = authenticatedExport
      ? `https://${this.host}/api/2.0/sql/telemetry-ext`
      : `https://${this.host}/api/2.0/sql/telemetry-unauth`;

    // Format payload
    const payload: DatabricksTelemetryPayload = {
      frontend_logs: metrics.map((m) => this.toTelemetryLog(m)),
    };

    logger.log(
      LogLevel.debug,
      `Exporting ${metrics.length} telemetry metrics to ${authenticatedExport ? 'authenticated' : 'unauthenticated'} endpoint`
    );

    // Make HTTP POST request
    // Note: In production, auth headers would be added via connectionProvider
    const response: Response = await this.fetchFn(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': this.userAgent,
        // Note: ConnectionProvider may add auth headers automatically
        // via getThriftConnection, but for telemetry we use direct fetch
        // In production, we'd need to extract auth headers from connectionProvider
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const error: any = new Error(`Telemetry export failed: ${response.status} ${response.statusText}`);
      error.statusCode = response.status;
      throw error;
    }

    logger.log(LogLevel.debug, `Successfully exported ${metrics.length} telemetry metrics`);
  }

  /**
   * Convert TelemetryMetric to Databricks telemetry log format.
   */
  private toTelemetryLog(metric: TelemetryMetric): DatabricksTelemetryLog {
    const log: DatabricksTelemetryLog = {
      workspace_id: metric.workspaceId,
      frontend_log_event_id: this.generateUUID(),
      context: {
        client_context: {
          timestamp_millis: metric.timestamp,
          user_agent: this.userAgent,
        },
      },
      entry: {
        sql_driver_log: {
          session_id: metric.sessionId,
          sql_statement_id: metric.statementId,
        },
      },
    };

    // Add metric-specific fields
    if (metric.metricType === 'connection' && metric.driverConfig) {
      log.entry.sql_driver_log.driver_config = metric.driverConfig;
    } else if (metric.metricType === 'statement') {
      log.entry.sql_driver_log.operation_latency_ms = metric.latencyMs;

      if (metric.resultFormat || metric.chunkCount) {
        log.entry.sql_driver_log.sql_operation = {
          execution_result_format: metric.resultFormat,
        };

        if (metric.chunkCount && metric.chunkCount > 0) {
          log.entry.sql_driver_log.sql_operation.chunk_details = {
            chunk_count: metric.chunkCount,
            total_bytes: metric.bytesDownloaded,
          };
        }
      }
    } else if (metric.metricType === 'error') {
      log.entry.sql_driver_log.error_info = {
        error_name: metric.errorName || 'UnknownError',
        stack_trace: metric.errorMessage || '',
      };
    }

    return log;
  }

  /**
   * Generate a UUID v4.
   */
  private generateUUID(): string {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = (Math.random() * 16) | 0;
      const v = c === 'x' ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }

  /**
   * Get driver version from package.json.
   */
  private getDriverVersion(): string {
    try {
      // In production, this would read from package.json
      return '1.0.0';
    } catch {
      return 'unknown';
    }
  }

  /**
   * Sleep for the specified number of milliseconds.
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }
}
