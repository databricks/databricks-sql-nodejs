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
import { buildUrl } from './urlUtils';

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
      system_configuration?: {
        driver_version?: string;
        runtime_name?: string;
        runtime_version?: string;
        runtime_vendor?: string;
        os_name?: string;
        os_version?: string;
        os_arch?: string;
        driver_name?: string;
        client_app_name?: string;
        locale_name?: string;
        char_set_encoding?: string;
        process_name?: string;
      };
      driver_connection_params?: {
        http_path?: string;
        socket_timeout?: number;
        enable_arrow?: boolean;
        enable_direct_results?: boolean;
        enable_metric_view_metadata?: boolean;
      };
      auth_type?: string;
      operation_latency_ms?: number;
      sql_operation?: {
        statement_type?: string;
        is_compressed?: boolean;
        execution_result?: string;
        chunk_details?: {
          total_chunks_present?: number;
          total_chunks_iterated?: number;
          initial_chunk_latency_millis?: number;
          slowest_chunk_latency_millis?: number;
          sum_chunks_download_time_millis?: number;
        };
      };
      error_info?: {
        error_name: string;
        stack_trace: string;
      };
    };
  };
}

/**
 * Payload format for Databricks telemetry export.
 * Matches JDBC TelemetryRequest format with protoLogs.
 */
interface DatabricksTelemetryPayload {
  uploadTime: number;
  items: string[]; // Always empty - required field
  protoLogs: string[]; // JSON-stringified TelemetryFrontendLog objects
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
    fetchFunction?: typeof fetch,
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
        const baseDelay = Math.min(100 * 2 ** attempt, 1000);
        const jitter = Math.random() * 100;
        const delay = baseDelay + jitter;

        logger.log(
          LogLevel.debug,
          `Retrying telemetry export (attempt ${attempt + 1}/${maxRetries}) after ${Math.round(delay)}ms`,
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
    const authenticatedExport = config.telemetryAuthenticatedExport ?? DEFAULT_TELEMETRY_CONFIG.authenticatedExport;
    const endpoint = authenticatedExport
      ? buildUrl(this.host, '/telemetry-ext')
      : buildUrl(this.host, '/telemetry-unauth');

    // Format payload - each log is JSON-stringified to match JDBC format
    const telemetryLogs = metrics.map((m) => this.toTelemetryLog(m));
    const protoLogs = telemetryLogs.map((log) => JSON.stringify(log));

    const payload: DatabricksTelemetryPayload = {
      uploadTime: Date.now(),
      items: [], // Required but unused
      protoLogs,
    };

    logger.log(
      LogLevel.debug,
      `Exporting ${metrics.length} telemetry metrics to ${
        authenticatedExport ? 'authenticated' : 'unauthenticated'
      } endpoint`,
    );

    // Get authentication headers if using authenticated endpoint
    const authHeaders = authenticatedExport ? await this.context.getAuthHeaders() : {};

    // Make HTTP POST request with authentication
    const response: Response = await this.fetchFn(endpoint, {
      method: 'POST',
      headers: {
        ...authHeaders,
        'Content-Type': 'application/json',
        'User-Agent': this.userAgent,
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
    // Filter out NIL UUID for statement ID
    const statementId =
      metric.statementId && metric.statementId !== '00000000-0000-0000-0000-000000000000'
        ? metric.statementId
        : undefined;

    const log: DatabricksTelemetryLog = {
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
          sql_statement_id: statementId,
        },
      },
    };

    // Add metric-specific fields based on proto definition
    if (metric.metricType === 'connection' && metric.driverConfig) {
      // Map driverConfig to system_configuration (snake_case as per proto)
      log.entry.sql_driver_log.system_configuration = {
        driver_version: metric.driverConfig.driverVersion,
        driver_name: metric.driverConfig.driverName,
        runtime_name: 'Node.js',
        runtime_version: metric.driverConfig.nodeVersion,
        runtime_vendor: metric.driverConfig.runtimeVendor,
        os_name: metric.driverConfig.platform,
        os_version: metric.driverConfig.osVersion,
        os_arch: metric.driverConfig.osArch,
        locale_name: metric.driverConfig.localeName,
        char_set_encoding: metric.driverConfig.charSetEncoding,
        process_name: metric.driverConfig.processName,
      };
      // Include connection open latency
      if (metric.latencyMs !== undefined) {
        log.entry.sql_driver_log.operation_latency_ms = metric.latencyMs;
      }
      // Include driver connection params (only if we have fields to include)
      if (
        metric.driverConfig.httpPath ||
        metric.driverConfig.socketTimeout ||
        metric.driverConfig.enableMetricViewMetadata !== undefined
      ) {
        log.entry.sql_driver_log.driver_connection_params = {
          ...(metric.driverConfig.httpPath && { http_path: metric.driverConfig.httpPath }),
          ...(metric.driverConfig.socketTimeout && { socket_timeout: metric.driverConfig.socketTimeout }),
          ...(metric.driverConfig.arrowEnabled !== undefined && { enable_arrow: metric.driverConfig.arrowEnabled }),
          ...(metric.driverConfig.directResultsEnabled !== undefined && {
            enable_direct_results: metric.driverConfig.directResultsEnabled,
          }),
          ...(metric.driverConfig.enableMetricViewMetadata !== undefined && {
            enable_metric_view_metadata: metric.driverConfig.enableMetricViewMetadata,
          }),
        };
      }
      // Include auth type at top level
      log.entry.sql_driver_log.auth_type = metric.driverConfig.authType;
    } else if (metric.metricType === 'statement') {
      log.entry.sql_driver_log.operation_latency_ms = metric.latencyMs;

      // Only create sql_operation if we have any fields to include
      if (metric.operationType || metric.compressed !== undefined || metric.resultFormat || metric.chunkCount) {
        log.entry.sql_driver_log.sql_operation = {
          ...(metric.operationType && { statement_type: metric.operationType }),
          ...(metric.compressed !== undefined && { is_compressed: metric.compressed }),
          ...(metric.resultFormat && { execution_result: metric.resultFormat }),
        };

        if (metric.chunkCount && metric.chunkCount > 0) {
          log.entry.sql_driver_log.sql_operation.chunk_details = {
            total_chunks_present: metric.chunkCount,
            total_chunks_iterated: metric.chunkCount,
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
