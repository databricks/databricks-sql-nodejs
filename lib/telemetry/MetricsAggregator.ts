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
import {
  TelemetryEvent,
  TelemetryEventType,
  TelemetryMetric,
  DEFAULT_TELEMETRY_CONFIG,
} from './types';
import DatabricksTelemetryExporter from './DatabricksTelemetryExporter';
import ExceptionClassifier from './ExceptionClassifier';

/**
 * Per-statement telemetry details for aggregation
 */
interface StatementTelemetryDetails {
  statementId: string;
  sessionId: string;
  workspaceId?: string;
  operationType?: string;
  startTime: number;
  executionLatencyMs?: number;
  resultFormat?: string;
  chunkCount: number;
  bytesDownloaded: number;
  pollCount: number;
  compressionEnabled?: boolean;
  errors: TelemetryEvent[];
}

/**
 * Aggregates telemetry events by statement_id and manages batching/flushing.
 *
 * Features:
 * - Aggregates events by statement_id
 * - Connection events emitted immediately (no aggregation)
 * - Statement events buffered until completeStatement() called
 * - Terminal exceptions flushed immediately
 * - Retryable exceptions buffered until statement complete
 * - Batch size and periodic timer trigger flushes
 * - CRITICAL: All exceptions swallowed and logged at LogLevel.debug ONLY
 * - CRITICAL: NO console logging
 *
 * Follows JDBC TelemetryCollector.java:29-30 pattern.
 */
export default class MetricsAggregator {
  private statementMetrics: Map<string, StatementTelemetryDetails> = new Map();

  private pendingMetrics: TelemetryMetric[] = [];

  private flushTimer: NodeJS.Timeout | null = null;

  private batchSize: number;

  private flushIntervalMs: number;

  constructor(
    private context: IClientContext,
    private exporter: DatabricksTelemetryExporter
  ) {
    try {
      const config = context.getConfig();
      this.batchSize = config.telemetryBatchSize ?? DEFAULT_TELEMETRY_CONFIG.batchSize;
      this.flushIntervalMs = config.telemetryFlushIntervalMs ?? DEFAULT_TELEMETRY_CONFIG.flushIntervalMs;

      // Start periodic flush timer
      this.startFlushTimer();
    } catch (error: any) {
      // CRITICAL: All exceptions swallowed and logged at debug level ONLY
      const logger = this.context.getLogger();
      logger.log(LogLevel.debug, `MetricsAggregator constructor error: ${error.message}`);

      // Initialize with default values
      this.batchSize = DEFAULT_TELEMETRY_CONFIG.batchSize;
      this.flushIntervalMs = DEFAULT_TELEMETRY_CONFIG.flushIntervalMs;
    }
  }

  /**
   * Process a telemetry event. Never throws.
   *
   * @param event - The telemetry event to process
   */
  processEvent(event: TelemetryEvent): void {
    const logger = this.context.getLogger();

    try {
      // Connection events are emitted immediately (no aggregation)
      if (event.eventType === TelemetryEventType.CONNECTION_OPEN) {
        this.processConnectionEvent(event);
        return;
      }

      // Error events - check if terminal or retryable
      if (event.eventType === TelemetryEventType.ERROR) {
        this.processErrorEvent(event);
        return;
      }

      // Statement events - buffer until complete
      if (event.statementId) {
        this.processStatementEvent(event);
      }
    } catch (error: any) {
      // CRITICAL: All exceptions swallowed and logged at debug level ONLY
      logger.log(LogLevel.debug, `MetricsAggregator.processEvent error: ${error.message}`);
    }
  }

  /**
   * Process connection event (emit immediately)
   */
  private processConnectionEvent(event: TelemetryEvent): void {
    const metric: TelemetryMetric = {
      metricType: 'connection',
      timestamp: event.timestamp,
      sessionId: event.sessionId,
      workspaceId: event.workspaceId,
      driverConfig: event.driverConfig,
    };

    this.addPendingMetric(metric);
  }

  /**
   * Process error event (terminal errors flushed immediately, retryable buffered)
   */
  private processErrorEvent(event: TelemetryEvent): void {
    const logger = this.context.getLogger();

    // Create error object for classification
    const error: any = new Error(event.errorMessage || 'Unknown error');
    error.name = event.errorName || 'UnknownError';

    // Check if terminal using isTerminal field or ExceptionClassifier
    const isTerminal = event.isTerminal ?? ExceptionClassifier.isTerminal(error);

    if (isTerminal) {
      // Terminal error - flush immediately
      logger.log(LogLevel.debug, `Terminal error detected - flushing immediately`);

      // If associated with a statement, complete and flush it
      if (event.statementId && this.statementMetrics.has(event.statementId)) {
        const details = this.statementMetrics.get(event.statementId)!;
        details.errors.push(event);
        this.completeStatement(event.statementId);
      } else {
        // Standalone error - emit immediately
        const metric: TelemetryMetric = {
          metricType: 'error',
          timestamp: event.timestamp,
          sessionId: event.sessionId,
          statementId: event.statementId,
          workspaceId: event.workspaceId,
          errorName: event.errorName,
          errorMessage: event.errorMessage,
        };
        this.addPendingMetric(metric);
      }

      // Flush immediately for terminal errors
      this.flush();
    } else if (event.statementId) {
      // Retryable error - buffer until statement complete
      const details = this.getOrCreateStatementDetails(event);
      details.errors.push(event);
    }
  }

  /**
   * Process statement event (buffer until complete)
   */
  private processStatementEvent(event: TelemetryEvent): void {
    const details = this.getOrCreateStatementDetails(event);

    switch (event.eventType) {
      case TelemetryEventType.STATEMENT_START:
        details.operationType = event.operationType;
        details.startTime = event.timestamp;
        break;

      case TelemetryEventType.STATEMENT_COMPLETE:
        details.executionLatencyMs = event.latencyMs;
        details.resultFormat = event.resultFormat;
        details.chunkCount = event.chunkCount ?? 0;
        details.bytesDownloaded = event.bytesDownloaded ?? 0;
        details.pollCount = event.pollCount ?? 0;
        break;

      case TelemetryEventType.CLOUDFETCH_CHUNK:
        details.chunkCount += 1;
        details.bytesDownloaded += event.bytes ?? 0;
        if (event.compressed !== undefined) {
          details.compressionEnabled = event.compressed;
        }
        break;

      default:
        // Unknown event type - ignore
        break;
    }
  }

  /**
   * Get or create statement details for the given event
   */
  private getOrCreateStatementDetails(event: TelemetryEvent): StatementTelemetryDetails {
    const statementId = event.statementId!;

    if (!this.statementMetrics.has(statementId)) {
      this.statementMetrics.set(statementId, {
        statementId,
        sessionId: event.sessionId!,
        workspaceId: event.workspaceId,
        startTime: event.timestamp,
        chunkCount: 0,
        bytesDownloaded: 0,
        pollCount: 0,
        errors: [],
      });
    }

    return this.statementMetrics.get(statementId)!;
  }

  /**
   * Complete a statement and prepare it for flushing. Never throws.
   *
   * @param statementId - The statement ID to complete
   */
  completeStatement(statementId: string): void {
    const logger = this.context.getLogger();

    try {
      const details = this.statementMetrics.get(statementId);
      if (!details) {
        return;
      }

      // Create statement metric
      const metric: TelemetryMetric = {
        metricType: 'statement',
        timestamp: details.startTime,
        sessionId: details.sessionId,
        statementId: details.statementId,
        workspaceId: details.workspaceId,
        latencyMs: details.executionLatencyMs,
        resultFormat: details.resultFormat,
        chunkCount: details.chunkCount,
        bytesDownloaded: details.bytesDownloaded,
        pollCount: details.pollCount,
      };

      this.addPendingMetric(metric);

      // Add buffered error metrics
      for (const errorEvent of details.errors) {
        const errorMetric: TelemetryMetric = {
          metricType: 'error',
          timestamp: errorEvent.timestamp,
          sessionId: details.sessionId,
          statementId: details.statementId,
          workspaceId: details.workspaceId,
          errorName: errorEvent.errorName,
          errorMessage: errorEvent.errorMessage,
        };
        this.addPendingMetric(errorMetric);
      }

      // Remove from map
      this.statementMetrics.delete(statementId);
    } catch (error: any) {
      // CRITICAL: All exceptions swallowed and logged at debug level ONLY
      logger.log(LogLevel.debug, `MetricsAggregator.completeStatement error: ${error.message}`);
    }
  }

  /**
   * Add a metric to pending batch and flush if batch size reached
   */
  private addPendingMetric(metric: TelemetryMetric): void {
    this.pendingMetrics.push(metric);

    // Check if batch size reached
    if (this.pendingMetrics.length >= this.batchSize) {
      this.flush();
    }
  }

  /**
   * Flush all pending metrics to exporter. Never throws.
   */
  flush(): void {
    const logger = this.context.getLogger();

    try {
      if (this.pendingMetrics.length === 0) {
        return;
      }

      const metricsToExport = [...this.pendingMetrics];
      this.pendingMetrics = [];

      logger.log(LogLevel.debug, `Flushing ${metricsToExport.length} telemetry metrics`);

      // Export metrics (exporter.export never throws)
      this.exporter.export(metricsToExport);
    } catch (error: any) {
      // CRITICAL: All exceptions swallowed and logged at debug level ONLY
      logger.log(LogLevel.debug, `MetricsAggregator.flush error: ${error.message}`);
    }
  }

  /**
   * Start the periodic flush timer
   */
  private startFlushTimer(): void {
    const logger = this.context.getLogger();

    try {
      if (this.flushTimer) {
        clearInterval(this.flushTimer);
      }

      this.flushTimer = setInterval(() => {
        this.flush();
      }, this.flushIntervalMs);

      // Prevent timer from keeping Node.js process alive
      this.flushTimer.unref();
    } catch (error: any) {
      // CRITICAL: All exceptions swallowed and logged at debug level ONLY
      logger.log(LogLevel.debug, `MetricsAggregator.startFlushTimer error: ${error.message}`);
    }
  }

  /**
   * Close the aggregator and flush remaining metrics. Never throws.
   */
  close(): void {
    const logger = this.context.getLogger();

    try {
      // Stop flush timer
      if (this.flushTimer) {
        clearInterval(this.flushTimer);
        this.flushTimer = null;
      }

      // Complete any remaining statements
      for (const statementId of this.statementMetrics.keys()) {
        this.completeStatement(statementId);
      }

      // Final flush
      this.flush();
    } catch (error: any) {
      // CRITICAL: All exceptions swallowed and logged at debug level ONLY
      logger.log(LogLevel.debug, `MetricsAggregator.close error: ${error.message}`);
    }
  }
}
