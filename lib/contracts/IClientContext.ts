import IDBSQLLogger from './IDBSQLLogger';
import IDriver from './IDriver';
import IConnectionProvider from '../connection/contracts/IConnectionProvider';
import IThriftClient from './IThriftClient';
import IAuthentication from '../connection/contracts/IAuthentication';
import type TelemetryEventEmitter from '../telemetry/TelemetryEventEmitter';
import type MetricsAggregator from '../telemetry/MetricsAggregator';

export interface ClientConfig {
  directResultsDefaultMaxRows: number;
  fetchChunkDefaultMaxRows: number;

  arrowEnabled?: boolean;
  useArrowNativeTypes?: boolean;
  socketTimeout: number;

  retryMaxAttempts: number;
  retriesTimeout: number; // in milliseconds
  retryDelayMin: number; // in milliseconds
  retryDelayMax: number; // in milliseconds

  useCloudFetch: boolean;
  cloudFetchConcurrentDownloads: number;
  cloudFetchSpeedThresholdMBps: number;

  useLZ4Compression: boolean;
  enableMetricViewMetadata?: boolean;

  // When true, DECIMAL values are returned as exact strings and 64-bit
  // integers as JS `bigint`, instead of being coerced to a lossy `number`.
  // Off by default to preserve the long-standing representation on both the
  // Thrift and SEA backends. See `ConnectionOptions.preserveBigNumericPrecision`.
  preserveBigNumericPrecision?: boolean;

  // Telemetry configuration
  telemetryEnabled?: boolean;
  telemetryBatchSize?: number;
  telemetryFlushIntervalMs?: number;
  telemetryMaxRetries?: number;
  telemetryBackoffBaseMs?: number;
  telemetryBackoffMaxMs?: number;
  telemetryBackoffJitterMs?: number;
  telemetryAuthenticatedExport?: boolean;
  telemetryCircuitBreakerThreshold?: number;
  telemetryCircuitBreakerTimeout?: number;
  telemetryMaxPendingMetrics?: number;
  telemetryMaxErrorsPerStatement?: number;
  telemetryStatementTtlMs?: number;
  telemetryCloseTimeoutMs?: number;
  telemetryMaxStatementMetrics?: number;
  /**
   * If `true`, MetricsAggregator installs a `process.on('beforeExit')` hook
   * that triggers a synchronous-as-possible flush before Node.js shuts down.
   * Mitigates data loss when application code calls `process.exit(0)` without
   * awaiting `client.close()`. Default `false` because adding a `beforeExit`
   * listener changes process-exit semantics for hosts that monkey-patch
   * `process.exit` (e.g. some test runners).
   */
  telemetryFlushOnExit?: boolean;
  userAgentEntry?: string;

  /**
   * Extra HTTP headers attached to driver-owned out-of-band requests
   * (telemetry, feature flags). Populated by `DBSQLClient.connect()` from
   * `ConnectionOptions.customHeaders` plus an `x-databricks-org-id` header
   * derived from the `?o=` query parameter on `httpPath` when present, to
   * support SPOG (Single Panel of Glass) account-level routing on endpoints
   * that don't carry `?o=` in their URL path. NOT applied to Thrift or
   * OAuth/OIDC requests.
   */
  customHeaders?: Record<string, string>;
}

export default interface IClientContext {
  getConfig(): ClientConfig;

  getLogger(): IDBSQLLogger;

  getConnectionProvider(): Promise<IConnectionProvider>;

  getClient(): Promise<IThriftClient>;

  getDriver(): Promise<IDriver>;

  getAuthProvider?(): IAuthentication | undefined;

  // The two telemetry accessors below remain optional methods on this
  // interface for back-compat with mock contexts in tests and external
  // sub-contexts that predate the telemetry work. A future refactor should
  // pull them onto a dedicated `ITelemetrySink` that the host context
  // implements, so non-telemetry context consumers don't see telemetry
  // surface area. Tracked under follow-up; left in place to keep this PR
  // scoped.

  /** @internal Telemetry event emitter, undefined when telemetry is disabled. */
  getTelemetryEmitter?(): TelemetryEventEmitter | undefined;

  /** @internal Telemetry aggregator, undefined when telemetry is disabled. */
  getTelemetryAggregator?(): MetricsAggregator | undefined;
}
