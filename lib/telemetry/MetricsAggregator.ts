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
  // sessionId is optional — emit sites pass `undefined` when no session is
  // associated yet (e.g. an operation created before its session id wired up).
  // The aggregator treats `undefined` as a single bucket rather than ghost
  // sessions per emit site.
  sessionId?: string;
  workspaceId?: string;
  operationType?: string;
  startTime: number;
  executionLatencyMs?: number;
  resultFormat?: string;
  chunkCount: number;
  bytesDownloaded: number;
  pollCount: number;
  compressionEnabled?: boolean;
  chunkInitialLatencyMs?: number;
  chunkSlowestLatencyMs?: number;
  chunkSumLatencyMs: number;
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

  // Single in-flight flush serializer. Concurrent triggers (batch hit, periodic
  // tick, terminal-error, manual flush) all share one HTTP POST so the user's
  // socket pool can't be starved by a slow telemetry endpoint.
  private flushInFlight: Promise<void> | null = null;

  private closed = false;

  private closing = false;

  private batchSize: number;

  private flushIntervalMs: number;

  private maxPendingMetrics: number;

  private maxErrorsPerStatement: number;

  private statementTtlMs: number;

  private maxStatementMetrics: number;

  // `beforeExit` hook installed when `telemetryFlushOnExit` is true. Tracked
  // so close() can detach the listener — leaving it attached would otherwise
  // keep references alive past the client's lifetime in long-running hosts
  // that create and destroy DBSQLClients (test runners, serverless cold
  // re-uses).
  private beforeExitHandler: (() => void) | null = null;

  // Operator-visible counters. Bumped every time the aggregator drops or
  // evicts a metric for capacity reasons. Surfaced via `getStats()` and
  // logged at warn on each flush when non-zero so silent data loss is
  // visible without forcing operators to grep debug logs.
  private droppedMetrics = 0;

  private evictedStatements = 0;

  // Counters of dropped/evicted entries reported in the most recent
  // warn-level summary. Compared against the running counter so each
  // summary reports the delta, not a cumulative value that grows forever.
  private lastReportedDrops = 0;

  private lastReportedEvictions = 0;

  constructor(private context: IClientContext, private exporter: DatabricksTelemetryExporter) {
    try {
      const config = context.getConfig();
      this.batchSize = config.telemetryBatchSize ?? DEFAULT_TELEMETRY_CONFIG.batchSize;
      this.flushIntervalMs = config.telemetryFlushIntervalMs ?? DEFAULT_TELEMETRY_CONFIG.flushIntervalMs;
      this.maxPendingMetrics = config.telemetryMaxPendingMetrics ?? DEFAULT_TELEMETRY_CONFIG.maxPendingMetrics;
      this.maxErrorsPerStatement =
        config.telemetryMaxErrorsPerStatement ?? DEFAULT_TELEMETRY_CONFIG.maxErrorsPerStatement;
      this.statementTtlMs = config.telemetryStatementTtlMs ?? DEFAULT_TELEMETRY_CONFIG.statementTtlMs;
      this.maxStatementMetrics = config.telemetryMaxStatementMetrics ?? DEFAULT_TELEMETRY_CONFIG.maxStatementMetrics;

      this.startFlushTimer();

      // Optional beforeExit hook for callers that can't easily reorder their
      // shutdown to `await client.close()` before `process.exit`. This is
      // best-effort — beforeExit doesn't fire on `process.exit()`, only on
      // a natural drain — and `process.exit(0)` skips it entirely. Disabled
      // by default; the test-runner override is the main reason callers opt out.
      if (config.telemetryFlushOnExit) {
        this.beforeExitHandler = () => {
          // beforeExit is synchronous; we kick off the flush but cannot wait
          // for the HTTP POST to complete. Synchronous Node.js APIs are not
          // available for fetch. Best-effort.
          this.flush(false).catch(() => {
            // swallow — telemetry must never break shutdown
          });
        };
        try {
          process.on('beforeExit', this.beforeExitHandler);
        } catch (err: any) {
          // Hosted environments where `process.on` is locked down
          this.context
            .getLogger()
            .log(LogLevel.debug, `MetricsAggregator beforeExit registration failed: ${err?.message ?? err}`);
          this.beforeExitHandler = null;
        }
      }
    } catch (error: any) {
      const logger = this.context.getLogger();
      logger.log(LogLevel.debug, `MetricsAggregator constructor error: ${error.message}`);

      this.batchSize = DEFAULT_TELEMETRY_CONFIG.batchSize;
      this.flushIntervalMs = DEFAULT_TELEMETRY_CONFIG.flushIntervalMs;
      this.maxPendingMetrics = DEFAULT_TELEMETRY_CONFIG.maxPendingMetrics;
      this.maxErrorsPerStatement = DEFAULT_TELEMETRY_CONFIG.maxErrorsPerStatement;
      this.statementTtlMs = DEFAULT_TELEMETRY_CONFIG.statementTtlMs;
      this.maxStatementMetrics = DEFAULT_TELEMETRY_CONFIG.maxStatementMetrics;
    }
  }

  processEvent(event: TelemetryEvent): void {
    if (this.closed) return;
    const logger = this.context.getLogger();

    try {
      if (event.eventType === TelemetryEventType.CONNECTION_OPEN) {
        this.processConnectionEvent(event, 'CREATE_SESSION');
        return;
      }

      if (event.eventType === TelemetryEventType.CONNECTION_CLOSE) {
        this.processConnectionEvent(event, 'DELETE_SESSION');
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

  private processConnectionEvent(event: TelemetryEvent, operationType: 'CREATE_SESSION' | 'DELETE_SESSION'): void {
    const metric: TelemetryMetric = {
      metricType: 'connection',
      timestamp: event.timestamp,
      sessionId: event.sessionId,
      workspaceId: event.workspaceId,
      driverConfig: event.driverConfig,
      operationType,
      latencyMs: event.latencyMs,
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
      this.flush(false).catch((err: any) => {
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
        // STATEMENT_COMPLETE may not carry chunk counts (the operation
        // doesn't always know them at close time); only override when the
        // emit explicitly supplied a value, otherwise the values accumulated
        // from CLOUDFETCH_CHUNK survive.
        if (event.chunkCount !== undefined) {
          details.chunkCount = event.chunkCount;
        }
        if (event.bytesDownloaded !== undefined) {
          details.bytesDownloaded = event.bytesDownloaded;
        }
        if (event.pollCount !== undefined) {
          details.pollCount = event.pollCount;
        }
        break;

      case TelemetryEventType.CLOUDFETCH_CHUNK:
        details.chunkCount += 1;
        details.bytesDownloaded += event.bytes ?? 0;
        // `compressionEnabled` is a sticky OR across all chunks in the
        // statement — any compressed chunk flips it true and it stays true.
        // Previously we copied the last chunk's value, which silently lied
        // for mixed-compression batches (compressed chunk 1, uncompressed
        // chunk 2 → compressionEnabled=false). True-on-any matches the
        // dashboard contract "did this statement benefit from compression".
        if (event.compressed === true) {
          details.compressionEnabled = true;
        } else if (event.compressed === false && details.compressionEnabled === undefined) {
          details.compressionEnabled = false;
        }
        // Per-chunk timing aggregation. Only record positive latencies — keeps
        // prefetched/cached pages out of the timing stats.
        if (event.latencyMs !== undefined && event.latencyMs > 0) {
          if (details.chunkInitialLatencyMs === undefined) {
            details.chunkInitialLatencyMs = event.latencyMs;
          }
          if (details.chunkSlowestLatencyMs === undefined || event.latencyMs > details.chunkSlowestLatencyMs) {
            details.chunkSlowestLatencyMs = event.latencyMs;
          }
          details.chunkSumLatencyMs += event.latencyMs;
        }
        break;

      default:
        break;
    }
  }

  private getOrCreateStatementDetails(event: TelemetryEvent): StatementTelemetryDetails {
    const statementId = event.statementId!;

    if (!this.statementMetrics.has(statementId)) {
      // Hard cap on map size — abandoned operations or buggy upstreams that
      // emit errors with random fresh statementIds would otherwise grow this
      // map unbounded for up to statementTtlMs.
      if (this.statementMetrics.size >= this.maxStatementMetrics) {
        this.evictOldestStatement();
      }
      this.statementMetrics.set(statementId, {
        statementId,
        sessionId: event.sessionId,
        workspaceId: event.workspaceId,
        startTime: event.timestamp,
        chunkCount: 0,
        bytesDownloaded: 0,
        pollCount: 0,
        chunkSumLatencyMs: 0,
        errors: [],
      });
    }

    return this.statementMetrics.get(statementId)!;
  }

  /**
   * Drop the oldest entry by insertion order to make room. Emits its buffered
   * errors as standalone metrics first so the first-failure signal survives.
   * Map iteration order is insertion order in JS.
   */
  private evictOldestStatement(): void {
    const oldest = this.statementMetrics.keys().next();
    if (oldest.done) return;
    const id = oldest.value;
    const details = this.statementMetrics.get(id)!;
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
    this.evictedStatements += 1;
    this.context
      .getLogger()
      .log(LogLevel.debug, `MetricsAggregator: evicted oldest statement ${id} (max=${this.maxStatementMetrics})`);
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
      this.evictedStatements += evicted;
      this.context
        .getLogger()
        .log(LogLevel.debug, `Evicted ${evicted} abandoned statement(s) past ${this.statementTtlMs}ms TTL`);
    }
  }

  /**
   * Operator-visible snapshot of aggregator state. Returned synchronously so
   * a health-check endpoint or shutdown summary can include it without
   * awaiting anything.
   *
   *   - `pendingMetricsCount`   : current buffer depth (0..maxPendingMetrics).
   *   - `inFlightStatements`    : open statement aggregations (0..maxStatementMetrics).
   *   - `droppedMetrics`        : cumulative count of metrics dropped due to
   *                                 buffer overflow since start.
   *   - `evictedStatements`     : cumulative count of statements evicted due
   *                                 to TTL or map-cap, since start.
   */
  getStats(): {
    pendingMetricsCount: number;
    inFlightStatements: number;
    droppedMetrics: number;
    evictedStatements: number;
  } {
    return {
      pendingMetricsCount: this.pendingMetrics.length,
      inFlightStatements: this.statementMetrics.size,
      droppedMetrics: this.droppedMetrics,
      evictedStatements: this.evictedStatements,
    };
  }

  /**
   * Emit a warn-level summary if drops/evictions occurred since the last
   * report. Operators running on `LogLevel.info` (the driver default) need
   * to see capacity events without enabling debug.
   */
  private maybeWarnOnCapacityEvents(): void {
    const dropsDelta = this.droppedMetrics - this.lastReportedDrops;
    const evictionsDelta = this.evictedStatements - this.lastReportedEvictions;
    if (dropsDelta === 0 && evictionsDelta === 0) {
      return;
    }
    this.lastReportedDrops = this.droppedMetrics;
    this.lastReportedEvictions = this.evictedStatements;
    this.context
      .getLogger()
      .log(
        LogLevel.warn,
        `Telemetry capacity events since last flush: ` +
          `dropped=${dropsDelta} (buffer cap=${this.maxPendingMetrics}); ` +
          `evicted=${evictionsDelta} statements (map cap=${this.maxStatementMetrics}, ttl=${this.statementTtlMs}ms). ` +
          `Raise telemetryMaxPendingMetrics / telemetryMaxStatementMetrics / telemetryStatementTtlMs if this is sustained.`,
      );
  }

  completeStatement(statementId: string): void {
    if (this.closed) return;
    const logger = this.context.getLogger();

    try {
      const details = this.statementMetrics.get(statementId);
      if (!details) {
        return;
      }

      // Emit chunkSumLatencyMs alongside chunkCount whenever there are
      // chunks. Dropping it when zero produced "5 chunks / 0ms total" rows in
      // dashboards because some sources (pre-fetched / cached pages) emit
      // chunks with latency=0. Aligning the omission rule with chunkCount
      // keeps the two fields consistent: present together or absent together.
      const hasChunks = details.chunkCount > 0;
      const metric: TelemetryMetric = {
        metricType: 'statement',
        timestamp: details.startTime,
        sessionId: details.sessionId,
        statementId: details.statementId,
        workspaceId: details.workspaceId,
        operationType: details.operationType,
        latencyMs: details.executionLatencyMs,
        resultFormat: details.resultFormat,
        chunkCount: details.chunkCount,
        chunkInitialLatencyMs: details.chunkInitialLatencyMs,
        chunkSlowestLatencyMs: details.chunkSlowestLatencyMs,
        chunkSumLatencyMs: hasChunks ? details.chunkSumLatencyMs : undefined,
        bytesDownloaded: details.bytesDownloaded,
        pollCount: details.pollCount,
        compressed: details.compressionEnabled,
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
      this.droppedMetrics += 1;
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
      this.flush(false).catch((err: any) => {
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
    // Coalesce concurrent flush callers onto the in-flight promise so we
    // never run two HTTP POSTs in parallel against the telemetry endpoint.
    // Pending metrics arriving while flushInFlight is set will be picked up
    // by the next caller.
    if (this.flushInFlight) {
      return this.flushInFlight;
    }
    this.flushInFlight = this.runFlush(resetTimer).finally(() => {
      this.flushInFlight = null;
    });
    return this.flushInFlight;
  }

  private async runFlush(resetTimer: boolean): Promise<void> {
    const logger = this.context.getLogger();

    // Surface capacity events (drops, evictions) once per flush at warn-level.
    // Runs before the empty-buffer short-circuit so an evict-only cycle still
    // emits the summary.
    this.maybeWarnOnCapacityEvents();

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
        this.flush(false).catch((err: any) => {
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

      // Detach the beforeExit hook before clearing the timer — otherwise a
      // long-running host that opens & closes many DBSQLClients accumulates
      // dead listeners on the process object.
      if (this.beforeExitHandler) {
        try {
          process.off('beforeExit', this.beforeExitHandler);
        } catch (err: any) {
          logger.log(LogLevel.debug, `MetricsAggregator beforeExit detach failed: ${err?.message ?? err}`);
        }
        this.beforeExitHandler = null;
      }

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
      // Cap the wait on the final flush so a flapping telemetry endpoint
      // can't hold up the user's process.exit(0). The in-flight POST is
      // abandoned past the deadline; data loss is preferable to a hung exit.
      const timeoutMs = this.context.getConfig().telemetryCloseTimeoutMs ?? 2000;
      let timeoutHandle: NodeJS.Timeout | null = null;
      const timeoutPromise = new Promise<void>((resolve) => {
        timeoutHandle = setTimeout(() => {
          logger.log(LogLevel.debug, `MetricsAggregator.close: flush timed out after ${timeoutMs}ms`);
          resolve();
        }, timeoutMs);
        timeoutHandle.unref?.();
      });
      // Drain pattern: if a batch-trigger flush was already in-flight when
      // close() ran, it captured a snapshot before completeStatement above
      // appended the close-time metrics. Wait for that to finish and then
      // run a fresh flush that picks up whatever's still in pendingMetrics.
      const drain = async (): Promise<void> => {
        if (this.flushInFlight) {
          await this.flushInFlight;
        }
        if (this.pendingMetrics.length > 0) {
          await this.flush(false);
        }
      };
      try {
        await Promise.race([drain(), timeoutPromise]);
      } finally {
        if (timeoutHandle) {
          clearTimeout(timeoutHandle);
        }
      }
    } catch (error: any) {
      logger.log(LogLevel.debug, `MetricsAggregator.close error: ${error.message}`);
    }
  }
}
