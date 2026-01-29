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

/**
 * EventEmitter for driver telemetry.
 * Emits events at key driver operations.
 *
 * CRITICAL REQUIREMENT: ALL exceptions must be caught and logged at LogLevel.debug ONLY
 * (never warn/error) to avoid customer anxiety. NO console logging allowed - only IDBSQLLogger.
 *
 * All emit methods are wrapped in try-catch blocks that swallow exceptions completely.
 * Event emission respects the telemetryEnabled flag from context config.
 */
export default class TelemetryEventEmitter extends EventEmitter {
  private enabled: boolean;

  constructor(private context: IClientContext) {
    super();
    // Check if telemetry is enabled from config
    // Default to false for safe rollout
    const config = context.getConfig() as any;
    this.enabled = config.telemetryEnabled ?? false;
  }

  /**
   * Emit a connection open event.
   *
   * @param data Connection event data including sessionId, workspaceId, and driverConfig
   */
  emitConnectionOpen(data: { sessionId: string; workspaceId: string; driverConfig: DriverConfiguration }): void {
    if (!this.enabled) return;

    const logger = this.context.getLogger();
    try {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: data.sessionId,
        workspaceId: data.workspaceId,
        driverConfig: data.driverConfig,
      };
      this.emit(TelemetryEventType.CONNECTION_OPEN, event);
    } catch (error: any) {
      // Swallow all exceptions - log at debug level only
      logger.log(LogLevel.debug, `Error emitting connection event: ${error.message}`);
    }
  }

  /**
   * Emit a statement start event.
   *
   * @param data Statement start data including statementId, sessionId, and operationType
   */
  emitStatementStart(data: { statementId: string; sessionId: string; operationType?: string }): void {
    if (!this.enabled) return;

    const logger = this.context.getLogger();
    try {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_START,
        timestamp: Date.now(),
        statementId: data.statementId,
        sessionId: data.sessionId,
        operationType: data.operationType,
      };
      this.emit(TelemetryEventType.STATEMENT_START, event);
    } catch (error: any) {
      // Swallow all exceptions - log at debug level only
      logger.log(LogLevel.debug, `Error emitting statement start: ${error.message}`);
    }
  }

  /**
   * Emit a statement complete event.
   *
   * @param data Statement completion data including latency, result format, and metrics
   */
  emitStatementComplete(data: {
    statementId: string;
    sessionId: string;
    latencyMs?: number;
    resultFormat?: string;
    chunkCount?: number;
    bytesDownloaded?: number;
    pollCount?: number;
  }): void {
    if (!this.enabled) return;

    const logger = this.context.getLogger();
    try {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_COMPLETE,
        timestamp: Date.now(),
        statementId: data.statementId,
        sessionId: data.sessionId,
        latencyMs: data.latencyMs,
        resultFormat: data.resultFormat,
        chunkCount: data.chunkCount,
        bytesDownloaded: data.bytesDownloaded,
        pollCount: data.pollCount,
      };
      this.emit(TelemetryEventType.STATEMENT_COMPLETE, event);
    } catch (error: any) {
      // Swallow all exceptions - log at debug level only
      logger.log(LogLevel.debug, `Error emitting statement complete: ${error.message}`);
    }
  }

  /**
   * Emit a CloudFetch chunk download event.
   *
   * @param data CloudFetch chunk data including chunk index, latency, bytes, and compression
   */
  emitCloudFetchChunk(data: {
    statementId: string;
    chunkIndex: number;
    latencyMs?: number;
    bytes: number;
    compressed?: boolean;
  }): void {
    if (!this.enabled) return;

    const logger = this.context.getLogger();
    try {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CLOUDFETCH_CHUNK,
        timestamp: Date.now(),
        statementId: data.statementId,
        chunkIndex: data.chunkIndex,
        latencyMs: data.latencyMs,
        bytes: data.bytes,
        compressed: data.compressed,
      };
      this.emit(TelemetryEventType.CLOUDFETCH_CHUNK, event);
    } catch (error: any) {
      // Swallow all exceptions - log at debug level only
      logger.log(LogLevel.debug, `Error emitting cloudfetch chunk: ${error.message}`);
    }
  }

  /**
   * Emit an error event.
   *
   * @param data Error event data including error details and terminal status
   */
  emitError(data: {
    statementId?: string;
    sessionId?: string;
    errorName: string;
    errorMessage: string;
    isTerminal: boolean;
  }): void {
    if (!this.enabled) return;

    const logger = this.context.getLogger();
    try {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.ERROR,
        timestamp: Date.now(),
        statementId: data.statementId,
        sessionId: data.sessionId,
        errorName: data.errorName,
        errorMessage: data.errorMessage,
        isTerminal: data.isTerminal,
      };
      this.emit(TelemetryEventType.ERROR, event);
    } catch (error: any) {
      // Swallow all exceptions - log at debug level only
      logger.log(LogLevel.debug, `Error emitting error event: ${error.message}`);
    }
  }
}
