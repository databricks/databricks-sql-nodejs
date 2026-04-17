import IDBSQLLogger from './IDBSQLLogger';
import IDriver from './IDriver';
import IConnectionProvider from '../connection/contracts/IConnectionProvider';
import IThriftClient from './IThriftClient';

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
  userAgentEntry?: string;
}

export default interface IClientContext {
  getConfig(): ClientConfig;

  getLogger(): IDBSQLLogger;

  getConnectionProvider(): Promise<IConnectionProvider>;

  getClient(): Promise<IThriftClient>;

  getDriver(): Promise<IDriver>;
}
