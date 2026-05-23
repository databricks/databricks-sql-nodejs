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

import { EventEmitter } from 'events';
import IClientContext from '../contracts/IClientContext';
import { LogLevel } from '../contracts/IDBSQLLogger';
import { TelemetryEvent, TelemetryEventType, DriverConfiguration } from './types';
import { redactSensitive } from './telemetryUtils';

/**
 * Typed map of event-type → listener payload shape. Keeps `on`/`off` calls
 * structurally typed: `emitter.on(TelemetryEventType.ERROR, (e) => …)` infers
 * `e: TelemetryEvent` instead of `any`. Avoids the EventEmitter-default
 * `(...args: any[]) => void` trap where a typo in the event name silently
 * registers a listener that never fires.
 */
export interface TelemetryEventMap {
  [TelemetryEventType.CONNECTION_OPEN]: (event: TelemetryEvent) => void;
  [TelemetryEventType.CONNECTION_CLOSE]: (event: TelemetryEvent) => void;
  [TelemetryEventType.STATEMENT_START]: (event: TelemetryEvent) => void;
  [TelemetryEventType.STATEMENT_COMPLETE]: (event: TelemetryEvent) => void;
  [TelemetryEventType.CLOUDFETCH_CHUNK]: (event: TelemetryEvent) => void;
  [TelemetryEventType.ERROR]: (event: TelemetryEvent) => void;
}

/**
 * EventEmitter for driver telemetry.
 * Emits events at key driver operations.
 *
 * CRITICAL REQUIREMENT: ALL exceptions must be caught and logged at LogLevel.debug ONLY
 * (never warn/error) to avoid customer anxiety. NO console logging allowed - only IDBSQLLogger.
 *
 * All emit methods funnel through `emitWrapped`, which holds the
 * try/catch/debug-log scaffold. The per-method bodies do nothing but build
 * the event shape — adding a new event type is a one-method change.
 */
export default class TelemetryEventEmitter extends EventEmitter {
  private enabled: boolean;

  constructor(private context: IClientContext) {
    super();
    // Check if telemetry is enabled from config
    // Default to false for safe rollout
    const config = context.getConfig();
    this.enabled = config.telemetryEnabled ?? false;
  }

  // Typed event subscription. EventEmitter's native types use `any` payloads;
  // these overrides give consumers a typed `event` parameter without forcing
  // a wholesale rewrite of the EventEmitter contract.
  on<K extends keyof TelemetryEventMap>(eventName: K, listener: TelemetryEventMap[K]): this {
    return super.on(eventName, listener as (...args: unknown[]) => void);
  }

  off<K extends keyof TelemetryEventMap>(eventName: K, listener: TelemetryEventMap[K]): this {
    return super.off(eventName, listener as (...args: unknown[]) => void);
  }

  once<K extends keyof TelemetryEventMap>(eventName: K, listener: TelemetryEventMap[K]): this {
    return super.once(eventName, listener as (...args: unknown[]) => void);
  }

  /**
   * Build-and-emit helper. The per-event `build` callback constructs the
   * payload; everything else (enabled check, try/catch, swallow-and-log)
   * lives here so the wrapping cannot drift between event types.
   */
  private emitWrapped(eventType: TelemetryEventType, build: () => TelemetryEvent): void {
    if (!this.enabled) return;
    const logger = this.context.getLogger();
    try {
      this.emit(eventType, build());
    } catch (error: any) {
      logger.log(LogLevel.debug, `Error emitting ${eventType}: ${error?.message ?? error}`);
    }
  }

  emitConnectionOpen(data: {
    sessionId: string;
    workspaceId?: string;
    /**
     * The full driver-configuration block (~1KB). Static for the process —
     * emit sites SHOULD pass it once per client and pass `undefined` on
     * subsequent CONNECTION_OPEN events (one client may open many sessions).
     * The aggregator and exporter both treat `undefined` as "no change since
     * the last metric on the same session lineage".
     */
    driverConfig?: DriverConfiguration;
    latencyMs: number;
  }): void {
    this.emitWrapped(TelemetryEventType.CONNECTION_OPEN, () => ({
      eventType: TelemetryEventType.CONNECTION_OPEN,
      timestamp: Date.now(),
      sessionId: data.sessionId,
      workspaceId: data.workspaceId,
      driverConfig: data.driverConfig,
      latencyMs: data.latencyMs,
    }));
  }

  emitConnectionClose(data: { sessionId: string; latencyMs: number }): void {
    this.emitWrapped(TelemetryEventType.CONNECTION_CLOSE, () => ({
      eventType: TelemetryEventType.CONNECTION_CLOSE,
      timestamp: Date.now(),
      sessionId: data.sessionId,
      latencyMs: data.latencyMs,
    }));
  }

  emitStatementStart(data: { statementId: string; sessionId?: string; operationType?: string }): void {
    this.emitWrapped(TelemetryEventType.STATEMENT_START, () => ({
      eventType: TelemetryEventType.STATEMENT_START,
      timestamp: Date.now(),
      statementId: data.statementId,
      sessionId: data.sessionId,
      operationType: data.operationType,
    }));
  }

  emitStatementComplete(data: {
    statementId: string;
    sessionId?: string;
    latencyMs?: number;
    resultFormat?: string;
    chunkCount?: number;
    bytesDownloaded?: number;
    pollCount?: number;
  }): void {
    this.emitWrapped(TelemetryEventType.STATEMENT_COMPLETE, () => ({
      eventType: TelemetryEventType.STATEMENT_COMPLETE,
      timestamp: Date.now(),
      statementId: data.statementId,
      sessionId: data.sessionId,
      latencyMs: data.latencyMs,
      resultFormat: data.resultFormat,
      chunkCount: data.chunkCount,
      bytesDownloaded: data.bytesDownloaded,
      pollCount: data.pollCount,
    }));
  }

  emitCloudFetchChunk(data: {
    statementId: string;
    chunkIndex: number;
    latencyMs?: number;
    bytes: number;
    compressed?: boolean;
  }): void {
    this.emitWrapped(TelemetryEventType.CLOUDFETCH_CHUNK, () => ({
      eventType: TelemetryEventType.CLOUDFETCH_CHUNK,
      timestamp: Date.now(),
      statementId: data.statementId,
      chunkIndex: data.chunkIndex,
      latencyMs: data.latencyMs,
      bytes: data.bytes,
      compressed: data.compressed,
    }));
  }

  /**
   * Emit an error event.
   *
   * Redaction happens HERE — not at the exporter — so any in-process listener
   * (this class extends `EventEmitter` and `getTelemetryEmitter()` is reachable
   * from any consumer of `@databricks/sql`) sees the same redacted strings
   * the export pipeline does. `redactSensitive` strips Bearer/Basic, Databricks
   * token prefixes, JWTs, JSON-encoded secrets, URL userinfo, and common
   * username-bearing filesystem paths, then caps length.
   *
   * `errorMessage` is also redacted, not only `errorStack` — operation error
   * messages can carry query fragments, table names, parameter values that the
   * SECRET_PATTERNS regex must scrub before they're emitted anywhere.
   */
  emitError(data: {
    statementId?: string;
    sessionId?: string;
    errorName: string;
    errorMessage: string;
    errorStack?: string;
    isTerminal: boolean;
  }): void {
    this.emitWrapped(TelemetryEventType.ERROR, () => ({
      eventType: TelemetryEventType.ERROR,
      timestamp: Date.now(),
      statementId: data.statementId,
      sessionId: data.sessionId,
      errorName: data.errorName,
      errorMessage: redactSensitive(data.errorMessage),
      errorStack: data.errorStack === undefined ? undefined : redactSensitive(data.errorStack),
      isTerminal: data.isTerminal,
    }));
  }
}
