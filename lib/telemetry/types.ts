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

/**
 * Driver name constant for telemetry
 */
export const DRIVER_NAME = 'nodejs-sql-driver';

/**
 * Event types emitted by the telemetry system
 */
export enum TelemetryEventType {
  CONNECTION_OPEN = 'connection.open',
  STATEMENT_START = 'statement.start',
  STATEMENT_COMPLETE = 'statement.complete',
  CLOUDFETCH_CHUNK = 'cloudfetch.chunk',
  ERROR = 'error',
}

/**
 * Configuration for telemetry components
 */
export interface TelemetryConfiguration {
  /** Whether telemetry is enabled */
  enabled?: boolean;

  /** Maximum number of metrics to batch before flushing */
  batchSize?: number;

  /** Interval in milliseconds to flush metrics */
  flushIntervalMs?: number;

  /** Maximum retry attempts for export */
  maxRetries?: number;

  /** Whether to use authenticated export endpoint */
  authenticatedExport?: boolean;

  /** Circuit breaker failure threshold */
  circuitBreakerThreshold?: number;

  /** Circuit breaker timeout in milliseconds */
  circuitBreakerTimeout?: number;
}

/**
 * Default telemetry configuration values
 */
export const DEFAULT_TELEMETRY_CONFIG: Required<TelemetryConfiguration> = {
  enabled: true, // Enabled by default, gated by feature flag
  batchSize: 100,
  flushIntervalMs: 5000,
  maxRetries: 3,
  authenticatedExport: true,
  circuitBreakerThreshold: 5,
  circuitBreakerTimeout: 60000, // 1 minute
};

/**
 * Runtime telemetry event emitted by the driver
 */
export interface TelemetryEvent {
  /** Type of the event */
  eventType: TelemetryEventType | string;

  /** Timestamp when the event occurred (milliseconds since epoch) */
  timestamp: number;

  /** Session ID for correlation */
  sessionId?: string;

  /** Statement ID for correlation */
  statementId?: string;

  // Connection-specific fields
  /** Workspace ID */
  workspaceId?: string;

  /** Driver configuration */
  driverConfig?: DriverConfiguration;

  // Statement-specific fields
  /** Type of operation (SELECT, INSERT, etc.) */
  operationType?: string;

  /** Execution latency in milliseconds */
  latencyMs?: number;

  /** Result format (inline, cloudfetch, arrow) */
  resultFormat?: string;

  /** Number of result chunks */
  chunkCount?: number;

  /** Total bytes downloaded */
  bytesDownloaded?: number;

  /** Number of poll operations */
  pollCount?: number;

  // CloudFetch-specific fields
  /** Chunk index in the result set */
  chunkIndex?: number;

  /** Number of bytes in this chunk */
  bytes?: number;

  /** Whether compression was used */
  compressed?: boolean;

  // Error-specific fields
  /** Error name/type */
  errorName?: string;

  /** Error message */
  errorMessage?: string;

  /** Whether the error is terminal (non-retryable) */
  isTerminal?: boolean;
}

/**
 * Aggregated telemetry metric for export to Databricks
 */
export interface TelemetryMetric {
  /** Type of metric */
  metricType: 'connection' | 'statement' | 'error';

  /** Timestamp when the metric was created (milliseconds since epoch) */
  timestamp: number;

  /** Session ID for correlation */
  sessionId?: string;

  /** Statement ID for correlation */
  statementId?: string;

  /** Workspace ID */
  workspaceId?: string;

  /** Driver configuration (included in all metrics for context) */
  driverConfig?: DriverConfiguration;

  /** Execution latency in milliseconds */
  latencyMs?: number;

  /** Type of operation (SELECT, INSERT, etc.) */
  operationType?: string;

  /** Result format (inline, cloudfetch, arrow) */
  resultFormat?: string;

  /** Number of result chunks */
  chunkCount?: number;

  /** Total bytes downloaded */
  bytesDownloaded?: number;

  /** Number of poll operations */
  pollCount?: number;

  /** Whether compression was used */
  compressed?: boolean;

  /** Error name/type */
  errorName?: string;

  /** Error message */
  errorMessage?: string;
}

/**
 * Driver configuration metadata collected once per connection
 */
export interface DriverConfiguration {
  /** Driver version */
  driverVersion: string;

  /** Driver name */
  driverName: string;

  /** Node.js version */
  nodeVersion: string;

  /** Platform (linux, darwin, win32) */
  platform: string;

  /** OS version */
  osVersion: string;

  /** OS architecture (x64, arm64, etc.) */
  osArch: string;

  /** Runtime vendor (Node.js Foundation) */
  runtimeVendor: string;

  /** Locale name (e.g., en_US) */
  localeName: string;

  /** Character set encoding (e.g., UTF-8) */
  charSetEncoding: string;

  /** Process name */
  processName: string;

  /** Authentication type (pat, external-browser, oauth-m2m, custom) */
  authType: string;

  // Feature flags
  /** Whether CloudFetch is enabled */
  cloudFetchEnabled: boolean;

  /** Whether LZ4 compression is enabled */
  lz4Enabled: boolean;

  /** Whether Arrow format is enabled */
  arrowEnabled: boolean;

  /** Whether direct results are enabled */
  directResultsEnabled: boolean;

  // Configuration values
  /** Socket timeout in milliseconds */
  socketTimeout: number;

  /** Maximum retry attempts */
  retryMaxAttempts: number;

  /** Number of concurrent CloudFetch downloads */
  cloudFetchConcurrentDownloads: number;

  // Connection parameters for telemetry
  /** HTTP path for API calls */
  httpPath?: string;

  /** Whether metric view metadata is enabled */
  enableMetricViewMetadata?: boolean;
}

/**
 * Per-statement metrics aggregated from multiple events
 */
export interface StatementMetrics {
  /** Statement ID */
  statementId: string;

  /** Session ID */
  sessionId: string;

  /** Type of operation */
  operationType?: string;

  /** Start timestamp (milliseconds since epoch) */
  startTime: number;

  /** Total execution latency in milliseconds */
  executionLatencyMs?: number;

  /** Number of poll operations */
  pollCount: number;

  /** Total poll latency in milliseconds */
  pollLatencyMs: number;

  /** Result format (inline, cloudfetch, arrow) */
  resultFormat?: string;

  /** Number of CloudFetch chunks downloaded */
  chunkCount: number;

  /** Total bytes downloaded */
  totalBytesDownloaded: number;

  /** Whether compression was used */
  compressionEnabled?: boolean;
}
