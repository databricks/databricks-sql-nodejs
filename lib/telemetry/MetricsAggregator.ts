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
import { TelemetryEvent, TelemetryEventType, TelemetryMetric, DEFAULT_TELEMETRY_CONFIG } from './types';
import DatabricksTelemetryExporter from './DatabricksTelemetryExporter';

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
 * Overflow policy — when the pending buffer hits `maxPendingMetrics`, error
 * metrics are preserved preferentially over connection/statement metrics.
 * The first-failure error is usually the most valuable signal in post-mortem
 * debugging; dropping it FIFO would defeat the purpose of capture.
 */
export default class MetricsAggregator {
  private statementMetrics: Map<string, StatementTelemetryDetails> = new Map();

  private pendingMetrics: TelemetryMetric[] = [];

  private flushTimer: NodeJS.Timeout | null = null;

  private closed = false;

  private closing = false;

  private batchSize: number;

  private flushIntervalMs: number;

  private maxPendingMetrics: number;

  private maxErrorsPerStatement: number;

  private statementTtlMs: number;

  constructor(private context: IClientContext, private exporter: DatabricksTelemetryExporter) {
    try {
      const config = context.getConfig();
      this.batchSize = config.telemetryBatchSize ?? DEFAULT_TELEMETRY_CONFIG.batchSize;
      this.flushIntervalMs = config.telemetryFlushIntervalMs ?? DEFAULT_TELEMETRY_CONFIG.flushIntervalMs;
      this.maxPendingMetrics = config.telemetryMaxPendingMetrics ?? DEFAULT_TELEMETRY_CONFIG.maxPendingMetrics;
      this.maxErrorsPerStatement =
        config.telemetryMaxErrorsPerStatement ?? DEFAULT_TELEMETRY_CONFIG.maxErrorsPerStatement;
      this.statementTtlMs = config.telemetryStatementTtlMs ?? DEFAULT_TELEMETRY_CONFIG.statementTtlMs;

      this.startFlushTimer();
    } catch (error: any) {
      const logger = this.context.getLogger();
      logger.log(LogLevel.debug, `MetricsAggregator constructor error: ${error.message}`);

      this.batchSize = DEFAULT_TELEMETRY_CONFIG.batchSize;
      this.flushIntervalMs = DEFAULT_TELEMETRY_CONFIG.flushIntervalMs;
      this.maxPendingMetrics = DEFAULT_TELEMETRY_CONFIG.maxPendingMetrics;
      this.maxErrorsPerStatement = DEFAULT_TELEMETRY_CONFIG.maxErrorsPerStatement;
      this.statementTtlMs = DEFAULT_TELEMETRY_CONFIG.statementTtlMs;
    }
  }

  processEvent(event: TelemetryEvent): void {
    if (this.closed) return;
    const logger = this.context.getLogger();

    try {
      if (event.eventType === TelemetryEventType.CONNECTION_OPEN) {
        this.processConnectionEvent(event);
        return;
      }

      if (event.eventType === TelemetryEventType.ERROR) {
        this.processErrorEvent(event);
        return;
      }

      if (event.statementId) {
        this.processStatementEvent(event);
      }
    } catch (error: any) {
      logger.log(LogLevel.debug, `MetricsAggregator.processEvent error: ${error.message}`);
    }
  }

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

  private processErrorEvent(event: TelemetryEvent): void {
    const logger = this.context.getLogger();

    // `isTerminal` is carried on the event by the emitter (it knows the
    // call site's taxonomy). If callers ever drop it we default to
    // retryable — buffering by statement is the safer choice.
    const isTerminal = event.isTerminal === true;

    if (isTerminal) {
      logger.log(LogLevel.debug, 'Terminal error detected - flushing immediately');

      if (event.statementId && this.statementMetrics.has(event.statementId)) {
        const details = this.statementMetrics.get(event.statementId)!;
        this.pushBoundedError(details, event);
        this.completeStatement(event.statementId);
      } else {
        const metric: TelemetryMetric = {
          metricType: 'error',
          timestamp: event.timestamp,
          sessionId: event.sessionId,
          statementId: event.statementId,
          workspaceId: event.workspaceId,
          errorName: event.errorName,
          errorMessage: event.errorMessage,
          errorStack: event.errorStack,
        };
        this.addPendingMetric(metric);
      }

      // Fire-and-forget on the terminal-error path so customer code doesn't
      // stall on telemetry HTTP. Do NOT reset the periodic flush timer:
      // under burst failures that would keep the tail-drain timer from
      // ever firing.
      Promise.resolve(this.flush(false)).catch((err: any) => {
        logger.log(LogLevel.debug, `Terminal-error flush failed: ${err?.message ?? err}`);
      });
    } else if (event.statementId) {
      const details = this.getOrCreateStatementDetails(event);
      this.pushBoundedError(details, event);
    }
  }

  private pushBoundedError(details: StatementTelemetryDetails, event: TelemetryEvent): void {
    if (details.errors.length >= this.maxErrorsPerStatement) {
      details.errors.shift();
    }
    details.errors.push(event);
  }

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
        break;
    }
  }

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
   * Drop entries older than `statementTtlMs`, emitting their buffered error
   * events as standalone metrics first so the first-failure signal survives
   * the eviction. Called from the periodic flush timer so idle clients
   * don't leak orphan entries.
   */
  private evictExpiredStatements(): void {
    const cutoff = Date.now() - this.statementTtlMs;
    let evicted = 0;
    for (const [id, details] of this.statementMetrics) {
      if (details.startTime < cutoff) {
        for (const errorEvent of details.errors) {
          this.addPendingMetric({
            metricType: 'error',
            timestamp: errorEvent.timestamp,
            sessionId: details.sessionId,
            statementId: details.statementId,
            workspaceId: details.workspaceId,
            errorName: errorEvent.errorName,
            errorMessage: errorEvent.errorMessage,
            errorStack: errorEvent.errorStack,
          });
        }
        this.statementMetrics.delete(id);
        evicted += 1;
      }
    }
    if (evicted > 0) {
      this.context
        .getLogger()
        .log(LogLevel.debug, `Evicted ${evicted} abandoned statement(s) past ${this.statementTtlMs}ms TTL`);
    }
  }

  completeStatement(statementId: string): void {
    if (this.closed) return;
    const logger = this.context.getLogger();

    try {
      const details = this.statementMetrics.get(statementId);
      if (!details) {
        return;
      }

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

      for (const errorEvent of details.errors) {
        const errorMetric: TelemetryMetric = {
          metricType: 'error',
          timestamp: errorEvent.timestamp,
          sessionId: details.sessionId,
          statementId: details.statementId,
          workspaceId: details.workspaceId,
          errorName: errorEvent.errorName,
          errorMessage: errorEvent.errorMessage,
          errorStack: errorEvent.errorStack,
        };
        this.addPendingMetric(errorMetric);
      }

      this.statementMetrics.delete(statementId);
    } catch (error: any) {
      logger.log(LogLevel.debug, `MetricsAggregator.completeStatement error: ${error.message}`);
    }
  }

  /**
   * Append `metric` to the pending buffer, enforcing `maxPendingMetrics`.
   *
   * Overflow drops the oldest non-error entry (single `splice` — no new
   * allocation). Under an all-error buffer it falls back to dropping the
   * oldest entry at index 0.
   */
  private addPendingMetric(metric: TelemetryMetric): void {
    if (this.closed) return;
    this.pendingMetrics.push(metric);

    if (this.pendingMetrics.length > this.maxPendingMetrics) {
      const dropIndex = this.findDropIndex();
      this.pendingMetrics.splice(dropIndex, 1);
      const logger = this.context.getLogger();
      logger.log(
        LogLevel.debug,
        `Dropped 1 oldest non-error telemetry metric (buffer full at ${this.maxPendingMetrics})`,
      );
    }

    if (this.pendingMetrics.length >= this.batchSize && !this.closing) {
      // resetTimer=false so the periodic tail-drain keeps its cadence even
      // under sustained batch-size bursts.
      const logger = this.context.getLogger();
      Promise.resolve(this.flush(false)).catch((err: any) => {
        logger.log(LogLevel.debug, `Batch-trigger flush failed: ${err?.message ?? err}`);
      });
    }
  }

  private findDropIndex(): number {
    for (let i = 0; i < this.pendingMetrics.length; i += 1) {
      if (this.pendingMetrics[i].metricType !== 'error') {
        return i;
      }
    }
    return 0;
  }

  /**
   * Drain the pending buffer and return a promise that resolves when the
   * exporter finishes with the drained batch. `close()` awaits this so
   * `process.exit()` after `client.close()` doesn't truncate the POST.
   */
  async flush(resetTimer: boolean = true): Promise<void> {
    const logger = this.context.getLogger();

    let exportPromise: Promise<void> | null = null;
    try {
      if (this.pendingMetrics.length === 0) {
        if (resetTimer && !this.closed) {
          this.startFlushTimer();
        }
        return;
      }

      const metricsToExport = this.pendingMetrics;
      this.pendingMetrics = [];

      logger.log(LogLevel.debug, `Flushing ${metricsToExport.length} telemetry metrics`);

      exportPromise = this.exporter.export(metricsToExport);

      if (resetTimer && !this.closed) {
        this.startFlushTimer();
      }
    } catch (error: any) {
      logger.log(LogLevel.debug, `MetricsAggregator.flush error: ${error.message}`);
    }

    if (exportPromise) {
      try {
        await exportPromise;
      } catch (err: any) {
        logger.log(LogLevel.debug, `Unexpected export error: ${err?.message ?? err}`);
      }
    }
  }

  private startFlushTimer(): void {
    if (this.closed) return;
    const logger = this.context.getLogger();

    try {
      if (this.flushTimer) {
        clearInterval(this.flushTimer);
      }

      this.flushTimer = setInterval(() => {
        // Idle eviction: run before the flush so orphan-error metrics have
        // a chance to batch into this drain rather than wait for the next.
        try {
          this.evictExpiredStatements();
        } catch (err: any) {
          logger.log(LogLevel.debug, `evictExpiredStatements error: ${err?.message ?? err}`);
        }
        Promise.resolve(this.flush(false)).catch((err: any) => {
          logger.log(LogLevel.debug, `Periodic flush failed: ${err?.message ?? err}`);
        });
      }, this.flushIntervalMs);

      this.flushTimer.unref();
    } catch (error: any) {
      logger.log(LogLevel.debug, `MetricsAggregator.startFlushTimer error: ${error.message}`);
    }
  }

  async close(): Promise<void> {
    const logger = this.context.getLogger();

    try {
      // Suppress batch-triggered fire-and-forget flushes from addPendingMetric
      // so no promises escape past the single awaited flush below.
      this.closing = true;

      if (this.flushTimer) {
        clearInterval(this.flushTimer);
        this.flushTimer = null;
      }

      // closed is still false here so completeStatement → addPendingMetric works normally.
      const remainingStatements = [...this.statementMetrics.keys()];
      for (const statementId of remainingStatements) {
        this.completeStatement(statementId);
      }

      this.closed = true;
      await this.flush(false);
    } catch (error: any) {
      logger.log(LogLevel.debug, `MetricsAggregator.close error: ${error.message}`);
    }
  }
}
