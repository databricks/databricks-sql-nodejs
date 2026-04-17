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

import { v4 as uuidv4 } from 'uuid';
import fetch, { RequestInit, Response } from 'node-fetch';
import IClientContext from '../contracts/IClientContext';
import { LogLevel } from '../contracts/IDBSQLLogger';
import IAuthentication from '../connection/contracts/IAuthentication';
import AuthenticationError from '../errors/AuthenticationError';
import HiveDriverError from '../errors/HiveDriverError';
import { TelemetryMetric, DEFAULT_TELEMETRY_CONFIG } from './types';
import { CircuitBreaker, CircuitBreakerOpenError, CircuitBreakerRegistry } from './CircuitBreaker';
import ExceptionClassifier from './ExceptionClassifier';
import {
  buildTelemetryUrl,
  hasAuthorization,
  normalizeHeaders,
  redactSensitive,
  sanitizeProcessName,
} from './telemetryUtils';
import buildUserAgentString from '../utils/buildUserAgentString';

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
      operation_latency_ms?: number;
      sql_operation?: {
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
 * Thrown for non-credential terminal telemetry failures (e.g. refusal to
 * export to an invalid host). Separate from `AuthenticationError` so the
 * classifier can keep the "short-circuit, don't retry, count as breaker
 * failure" contract without muddying the auth taxonomy used by the rest of
 * the driver.
 */
export class TelemetryTerminalError extends HiveDriverError {
  readonly terminal = true as const;
}

/**
 * Exports telemetry metrics to the Databricks telemetry service.
 *
 * CRITICAL: export() never throws — all errors are swallowed and logged at
 * LogLevel.debug (the one exception is a single warn on the first observed
 * auth-missing, re-armed on recovery).
 */
export default class DatabricksTelemetryExporter {
  private readonly circuitBreaker: CircuitBreaker;

  private readonly authenticatedUserAgent: string;

  /** User-Agent used for the unauthenticated endpoint; strips any
   *  caller-supplied `userAgentEntry` that could identify the customer. */
  private readonly unauthenticatedUserAgent: string;

  private authMissingWarned = false;

  constructor(
    private context: IClientContext,
    private host: string,
    private circuitBreakerRegistry: CircuitBreakerRegistry,
    private authProvider?: IAuthentication,
  ) {
    this.circuitBreaker = circuitBreakerRegistry.getCircuitBreaker(host);
    const config = this.context.getConfig();
    this.authenticatedUserAgent = buildUserAgentString(config.userAgentEntry);
    this.unauthenticatedUserAgent = buildUserAgentString(undefined);
  }

  /**
   * Release the per-host circuit breaker. Intended for the owning client's
   * close() path.
   *
   * NOTE: `CircuitBreakerRegistry` currently shares one breaker per host
   * across consumers; calling this while another consumer is active will
   * reset their failure-count memory. The owning-client is expected to be
   * the last consumer on its host; multi-consumer refcounting on the
   * registry will land in the consumer-wiring PR.
   */
  dispose(): void {
    this.circuitBreakerRegistry.removeCircuitBreaker(this.host);
  }

  async export(metrics: TelemetryMetric[]): Promise<void> {
    if (!metrics || metrics.length === 0) {
      return;
    }

    const logger = this.context.getLogger();

    try {
      await this.circuitBreaker.execute(() => this.exportWithRetry(metrics));
    } catch (error: any) {
      if (error instanceof CircuitBreakerOpenError) {
        logger.log(LogLevel.debug, 'Circuit breaker OPEN - dropping telemetry');
      } else if (error instanceof AuthenticationError) {
        logger.log(LogLevel.debug, `Telemetry export auth failure: ${error.message}`);
      } else if (error instanceof TelemetryTerminalError) {
        logger.log(LogLevel.debug, `Telemetry export refused: ${error.message}`);
      } else {
        logger.log(LogLevel.debug, `Telemetry export error: ${error?.message ?? error}`);
      }
    }
  }

  /**
   * Retry wrapper shaped after HttpRetryPolicy: retries only on errors
   * classified as retryable by ExceptionClassifier, stops on terminal ones,
   * surfaces the last error to the circuit breaker.
   *
   * `maxRetries` is the number of retries *after* the first attempt (i.e.
   * attempts = maxRetries + 1), matching HttpRetryPolicy's semantics.
   */
  private async exportWithRetry(metrics: TelemetryMetric[]): Promise<void> {
    const config = this.context.getConfig();
    const logger = this.context.getLogger();

    const rawMaxRetries = config.telemetryMaxRetries ?? DEFAULT_TELEMETRY_CONFIG.maxRetries;
    const maxRetries =
      Number.isFinite(rawMaxRetries) && rawMaxRetries >= 0 ? rawMaxRetries : DEFAULT_TELEMETRY_CONFIG.maxRetries;
    const baseMs = config.telemetryBackoffBaseMs ?? DEFAULT_TELEMETRY_CONFIG.backoffBaseMs;
    const maxMs = config.telemetryBackoffMaxMs ?? DEFAULT_TELEMETRY_CONFIG.backoffMaxMs;
    const jitterMs = config.telemetryBackoffJitterMs ?? DEFAULT_TELEMETRY_CONFIG.backoffJitterMs;

    const totalAttempts = maxRetries + 1;

    let lastError: Error | null = null;

    /* eslint-disable no-await-in-loop */
    for (let attempt = 0; attempt < totalAttempts; attempt += 1) {
      try {
        await this.exportInternal(metrics);
        return;
      } catch (error: any) {
        lastError = error;

        if (
          error instanceof AuthenticationError ||
          error instanceof TelemetryTerminalError ||
          ExceptionClassifier.isTerminal(error)
        ) {
          throw error;
        }
        if (!ExceptionClassifier.isRetryable(error)) {
          throw error;
        }
        if (attempt >= totalAttempts - 1) {
          throw error;
        }

        const base = Math.min(baseMs * 2 ** attempt, maxMs);
        const jitter = Math.random() * jitterMs;
        const delay = Math.min(base + jitter, maxMs);

        // Include the failing error so ops can see what's being retried,
        // not just the cadence.
        logger.log(
          LogLevel.debug,
          `Retrying telemetry export (attempt ${attempt + 1}/${totalAttempts}) after ${Math.round(delay)}ms: ${
            error?.statusCode ?? ''
          } ${redactSensitive(error?.message ?? '')}`,
        );

        await this.sleep(delay);
      }
    }
    /* eslint-enable no-await-in-loop */

    if (lastError) {
      throw lastError;
    }
  }

  private async exportInternal(metrics: TelemetryMetric[]): Promise<void> {
    const config = this.context.getConfig();
    const logger = this.context.getLogger();

    const authenticatedExport = config.telemetryAuthenticatedExport ?? DEFAULT_TELEMETRY_CONFIG.authenticatedExport;
    const endpoint = buildTelemetryUrl(this.host, authenticatedExport ? '/telemetry-ext' : '/telemetry-unauth');
    if (!endpoint) {
      // Malformed / deny-listed host — drop the batch rather than letting
      // it target an attacker-controlled destination.
      throw new TelemetryTerminalError('Refusing telemetry export: host failed validation');
    }

    const userAgent = authenticatedExport ? this.authenticatedUserAgent : this.unauthenticatedUserAgent;
    let headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'User-Agent': userAgent,
    };

    if (authenticatedExport) {
      headers = { ...headers, ...(await this.getAuthHeaders()) };
      if (!hasAuthorization(headers)) {
        if (!this.authMissingWarned) {
          this.authMissingWarned = true;
          logger.log(LogLevel.warn, 'Telemetry: Authorization header missing — metrics will be dropped');
        }
        throw new AuthenticationError('Telemetry export: missing Authorization header');
      }
    }

    const protoLogs = metrics.map((m) => this.toTelemetryLog(m, authenticatedExport, userAgent));
    const body = JSON.stringify({
      uploadTime: Date.now(),
      items: [],
      protoLogs: protoLogs.map((log) => JSON.stringify(log)),
    });

    logger.log(
      LogLevel.debug,
      `Exporting ${metrics.length} telemetry metrics to ${
        authenticatedExport ? 'authenticated' : 'unauthenticated'
      } endpoint`,
    );

    const response = await this.sendRequest(endpoint, {
      method: 'POST',
      headers,
      body,
      timeout: 10000,
    });

    if (!response.ok) {
      await response.text().catch(() => {});
      const error: any = new Error(`Telemetry export failed: ${response.status} ${response.statusText}`);
      error.statusCode = response.status;
      throw error;
    }

    await response.text().catch(() => {});
    // Successful round-trip re-arms the "auth missing" warn so operators see
    // a fresh signal the next time auth breaks.
    this.authMissingWarned = false;
    logger.log(LogLevel.debug, `Successfully exported ${metrics.length} telemetry metrics`);
  }

  private async getAuthHeaders(): Promise<Record<string, string>> {
    if (!this.authProvider) {
      return {};
    }
    const logger = this.context.getLogger();
    try {
      return normalizeHeaders(await this.authProvider.authenticate());
    } catch (error: any) {
      logger.log(LogLevel.debug, `Telemetry: auth provider threw: ${error?.message ?? error}`);
      return {};
    }
  }

  private async sendRequest(url: string, init: RequestInit): Promise<Response> {
    const connectionProvider = await this.context.getConnectionProvider();
    const agent = await connectionProvider.getAgent();
    return fetch(url, { ...init, agent });
  }

  private toTelemetryLog(
    metric: TelemetryMetric,
    authenticatedExport: boolean,
    userAgent: string,
  ): DatabricksTelemetryLog {
    // Unauthenticated export must not ship correlation IDs, fingerprint
    // data, or raw error detail — an on-path observer could otherwise link
    // sessions → workspaces → user activity without any auth.
    const includeCorrelation = authenticatedExport;

    const log: DatabricksTelemetryLog = {
      workspace_id: includeCorrelation ? metric.workspaceId : undefined,
      frontend_log_event_id: uuidv4(),
      context: {
        client_context: {
          timestamp_millis: metric.timestamp,
          user_agent: userAgent,
        },
      },
      entry: {
        sql_driver_log: {
          session_id: includeCorrelation ? metric.sessionId : undefined,
          sql_statement_id: includeCorrelation ? metric.statementId : undefined,
        },
      },
    };

    if (metric.metricType === 'connection' && metric.driverConfig && includeCorrelation) {
      // system_configuration is a high-entropy client fingerprint (OS, arch,
      // locale, process, runtime). Only ship on the authenticated path.
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
        process_name: sanitizeProcessName(metric.driverConfig.processName) || undefined,
      };
    } else if (metric.metricType === 'statement') {
      log.entry.sql_driver_log.operation_latency_ms = metric.latencyMs;

      if (metric.resultFormat || metric.chunkCount) {
        log.entry.sql_driver_log.sql_operation = {
          execution_result: metric.resultFormat,
        };

        if (metric.chunkCount && metric.chunkCount > 0) {
          log.entry.sql_driver_log.sql_operation.chunk_details = {
            total_chunks_present: metric.chunkCount,
            total_chunks_iterated: metric.chunkCount,
          };
        }
      }
    } else if (metric.metricType === 'error') {
      const stackOrMessage = metric.errorStack ?? metric.errorMessage ?? '';
      log.entry.sql_driver_log.error_info = {
        error_name: metric.errorName || 'UnknownError',
        // Redact common secret shapes and cap length. On the unauth path we
        // keep only the error class — no message body.
        stack_trace: includeCorrelation ? redactSensitive(stackOrMessage) : '',
      };
    }

    return log;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }
}
