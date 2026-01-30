import thrift from 'thrift';
import Int64 from 'node-int64';
import os from 'os';

import { EventEmitter } from 'events';
import { HeadersInit } from 'node-fetch';
import TCLIService from '../thrift/TCLIService';
import { TProtocolVersion } from '../thrift/TCLIService_types';
import IDBSQLClient, { ClientOptions, ConnectionOptions, OpenSessionRequest } from './contracts/IDBSQLClient';
import IDriver from './contracts/IDriver';
import IClientContext, { ClientConfig } from './contracts/IClientContext';
import IThriftClient from './contracts/IThriftClient';
import HiveDriver from './hive/HiveDriver';
import DBSQLSession from './DBSQLSession';
import IDBSQLSession from './contracts/IDBSQLSession';
import IAuthentication from './connection/contracts/IAuthentication';
import HttpConnection from './connection/connections/HttpConnection';
import IConnectionOptions from './connection/contracts/IConnectionOptions';
import Status from './dto/Status';
import HiveDriverError from './errors/HiveDriverError';
import { buildUserAgentString, definedOrError } from './utils';
import PlainHttpAuthentication from './connection/auth/PlainHttpAuthentication';
import DatabricksOAuth, { OAuthFlow } from './connection/auth/DatabricksOAuth';
import IDBSQLLogger, { LogLevel } from './contracts/IDBSQLLogger';
import DBSQLLogger from './DBSQLLogger';
import CloseableCollection from './utils/CloseableCollection';
import IConnectionProvider from './connection/contracts/IConnectionProvider';
import FeatureFlagCache from './telemetry/FeatureFlagCache';
import TelemetryClientProvider from './telemetry/TelemetryClientProvider';
import TelemetryEventEmitter from './telemetry/TelemetryEventEmitter';
import MetricsAggregator from './telemetry/MetricsAggregator';
import DatabricksTelemetryExporter from './telemetry/DatabricksTelemetryExporter';
import { CircuitBreakerRegistry } from './telemetry/CircuitBreaker';
import { DriverConfiguration, DRIVER_NAME } from './telemetry/types';
import driverVersion from './version';

function prependSlash(str: string): string {
  if (str.length > 0 && str.charAt(0) !== '/') {
    return `/${str}`;
  }
  return str;
}

function getInitialNamespaceOptions(catalogName?: string, schemaName?: string) {
  if (!catalogName && !schemaName) {
    return {};
  }

  return {
    initialNamespace: {
      catalogName,
      schemaName,
    },
  };
}

export type ThriftLibrary = Pick<typeof thrift, 'createClient'>;

export default class DBSQLClient extends EventEmitter implements IDBSQLClient, IClientContext {
  private static defaultLogger?: IDBSQLLogger;

  private readonly config: ClientConfig;

  private connectionProvider?: IConnectionProvider;

  private authProvider?: IAuthentication;

  private client?: IThriftClient;

  private readonly driver = new HiveDriver({
    context: this,
  });

  private readonly logger: IDBSQLLogger;

  private thrift: ThriftLibrary = thrift;

  private readonly sessions = new CloseableCollection<DBSQLSession>();

  // Telemetry components (instance-based, NOT singletons)
  private host?: string;

  private featureFlagCache?: FeatureFlagCache;

  private telemetryClientProvider?: TelemetryClientProvider;

  private telemetryEmitter?: TelemetryEventEmitter;

  private telemetryAggregator?: MetricsAggregator;

  private circuitBreakerRegistry?: CircuitBreakerRegistry;

  private static getDefaultLogger(): IDBSQLLogger {
    if (!this.defaultLogger) {
      this.defaultLogger = new DBSQLLogger();
    }
    return this.defaultLogger;
  }

  private static getDefaultConfig(): ClientConfig {
    return {
      directResultsDefaultMaxRows: 100000,
      fetchChunkDefaultMaxRows: 100000,

      arrowEnabled: true,
      useArrowNativeTypes: true,
      socketTimeout: 15 * 60 * 1000, // 15 minutes

      retryMaxAttempts: 5,
      retriesTimeout: 15 * 60 * 1000, // 15 minutes
      retryDelayMin: 1 * 1000, // 1 second
      retryDelayMax: 60 * 1000, // 60 seconds (1 minute)

      useCloudFetch: true, // enabling cloud fetch by default.
      cloudFetchConcurrentDownloads: 10,
      cloudFetchSpeedThresholdMBps: 0.1,

      useLZ4Compression: true,

      // Telemetry defaults
      telemetryEnabled: true, // Enabled by default, gated by feature flag
      telemetryBatchSize: 100,
      telemetryFlushIntervalMs: 5000,
      telemetryMaxRetries: 3,
      telemetryAuthenticatedExport: true,
      telemetryCircuitBreakerThreshold: 5,
      telemetryCircuitBreakerTimeout: 60000, // 1 minute
    };
  }

  constructor(options?: ClientOptions) {
    super();
    this.config = DBSQLClient.getDefaultConfig();
    this.logger = options?.logger ?? DBSQLClient.getDefaultLogger();
    this.logger.log(LogLevel.info, 'Created DBSQLClient');
  }

  private getConnectionOptions(options: ConnectionOptions): IConnectionOptions {
    return {
      host: options.host,
      port: options.port || 443,
      path: prependSlash(options.path),
      https: true,
      socketTimeout: options.socketTimeout,
      proxy: options.proxy,
      headers: {
        'User-Agent': buildUserAgentString(options.userAgentEntry),
      },
    };
  }

  private createAuthProvider(options: ConnectionOptions, authProvider?: IAuthentication): IAuthentication {
    if (authProvider) {
      return authProvider;
    }

    switch (options.authType) {
      case undefined:
      case 'access-token':
        return new PlainHttpAuthentication({
          username: 'token',
          password: options.token,
          context: this,
        });
      case 'databricks-oauth':
        return new DatabricksOAuth({
          flow: options.oauthClientSecret === undefined ? OAuthFlow.U2M : OAuthFlow.M2M,
          host: options.host,
          persistence: options.persistence,
          azureTenantId: options.azureTenantId,
          clientId: options.oauthClientId,
          clientSecret: options.oauthClientSecret,
          useDatabricksOAuthInAzure: options.useDatabricksOAuthInAzure,
          context: this,
        });
      case 'custom':
        return options.provider;
      // no default
    }
  }

  private createConnectionProvider(options: ConnectionOptions): IConnectionProvider {
    return new HttpConnection(this.getConnectionOptions(options), this);
  }

  /**
   * Extract workspace ID from hostname.
   * @param host - The host string (e.g., "workspace-id.cloud.databricks.com")
   * @returns Workspace ID or host if extraction fails
   */
  private extractWorkspaceId(host: string): string {
    // Extract workspace ID from hostname (first segment before first dot)
    const parts = host.split('.');
    return parts.length > 0 ? parts[0] : host;
  }

  /**
   * Build driver configuration for telemetry reporting.
   * @returns DriverConfiguration object with current driver settings
   */
  private buildDriverConfiguration(): DriverConfiguration {
    return {
      driverVersion,
      driverName: DRIVER_NAME,
      nodeVersion: process.version,
      platform: process.platform,
      osVersion: os.release(),
      osArch: os.arch(),
      runtimeVendor: 'Node.js Foundation',
      localeName: this.getLocaleName(),
      charSetEncoding: 'UTF-8',
      processName: this.getProcessName(),

      // Feature flags
      cloudFetchEnabled: this.config.useCloudFetch ?? false,
      lz4Enabled: this.config.useLZ4Compression ?? false,
      arrowEnabled: this.config.arrowEnabled ?? false,
      directResultsEnabled: true, // Direct results always enabled

      // Configuration values
      socketTimeout: this.config.socketTimeout ?? 0,
      retryMaxAttempts: this.config.retryMaxAttempts ?? 0,
      cloudFetchConcurrentDownloads: this.config.cloudFetchConcurrentDownloads ?? 0,
    };
  }

  /**
   * Get locale name in format language_country (e.g., en_US).
   * Matches JDBC format: user.language + '_' + user.country
   */
  private getLocaleName(): string {
    try {
      // Try to get from environment variables
      const lang = process.env.LANG || process.env.LC_ALL || process.env.LC_MESSAGES || '';
      if (lang) {
        // LANG format is typically "en_US.UTF-8", extract "en_US"
        const match = lang.match(/^([a-z]{2}_[A-Z]{2})/);
        if (match) {
          return match[1];
        }
      }
      // Fallback to en_US
      return 'en_US';
    } catch {
      return 'en_US';
    }
  }

  /**
   * Get process name, similar to JDBC's ProcessNameUtil.
   * Returns the script name or process title.
   */
  private getProcessName(): string {
    try {
      // Try process.title first (can be set by application)
      if (process.title && process.title !== 'node') {
        return process.title;
      }
      // Try to get the main script name from argv[1]
      if (process.argv && process.argv.length > 1) {
        const scriptPath = process.argv[1];
        // Extract filename without path
        const filename = scriptPath.split('/').pop()?.split('\\').pop() || '';
        // Remove extension
        const nameWithoutExt = filename.replace(/\.[^.]*$/, '');
        if (nameWithoutExt) {
          return nameWithoutExt;
        }
      }
      return 'node';
    } catch {
      return 'node';
    }
  }

  /**
   * Initialize telemetry components if enabled.
   * CRITICAL: All errors swallowed and logged at LogLevel.debug ONLY.
   * Driver NEVER throws exceptions due to telemetry.
   */
  private async initializeTelemetry(): Promise<void> {
    if (!this.host) {
      return;
    }

    try {
      // Create feature flag cache instance
      this.featureFlagCache = new FeatureFlagCache(this);
      this.featureFlagCache.getOrCreateContext(this.host);

      // Check if telemetry enabled via feature flag
      const enabled = await this.featureFlagCache.isTelemetryEnabled(this.host);

      if (!enabled) {
        this.logger.log(LogLevel.debug, 'Telemetry disabled via feature flag');
        return;
      }

      // Create telemetry components (all instance-based)
      this.telemetryClientProvider = new TelemetryClientProvider(this);
      this.telemetryEmitter = new TelemetryEventEmitter(this);

      // Get or create telemetry client for this host (increments refCount)
      this.telemetryClientProvider.getOrCreateClient(this.host);

      // Create circuit breaker registry and exporter
      this.circuitBreakerRegistry = new CircuitBreakerRegistry(this);
      const exporter = new DatabricksTelemetryExporter(this, this.host, this.circuitBreakerRegistry);
      this.telemetryAggregator = new MetricsAggregator(this, exporter);

      // Wire up event listeners
      this.telemetryEmitter.on('connection.open', (event) => {
        try {
          this.telemetryAggregator?.processEvent(event);
        } catch (error: any) {
          this.logger.log(LogLevel.debug, `Error processing connection.open event: ${error.message}`);
        }
      });

      this.telemetryEmitter.on('statement.start', (event) => {
        try {
          this.telemetryAggregator?.processEvent(event);
        } catch (error: any) {
          this.logger.log(LogLevel.debug, `Error processing statement.start event: ${error.message}`);
        }
      });

      this.telemetryEmitter.on('statement.complete', (event) => {
        try {
          this.telemetryAggregator?.processEvent(event);
        } catch (error: any) {
          this.logger.log(LogLevel.debug, `Error processing statement.complete event: ${error.message}`);
        }
      });

      this.telemetryEmitter.on('cloudfetch.chunk', (event) => {
        try {
          this.telemetryAggregator?.processEvent(event);
        } catch (error: any) {
          this.logger.log(LogLevel.debug, `Error processing cloudfetch.chunk event: ${error.message}`);
        }
      });

      this.telemetryEmitter.on('error', (event) => {
        try {
          this.telemetryAggregator?.processEvent(event);
        } catch (error: any) {
          this.logger.log(LogLevel.debug, `Error processing error event: ${error.message}`);
        }
      });

      this.logger.log(LogLevel.debug, 'Telemetry initialized successfully');
    } catch (error: any) {
      // Swallow all telemetry initialization errors
      this.logger.log(LogLevel.debug, `Telemetry initialization failed: ${error.message}`);
    }
  }

  /**
   * Connects DBSQLClient to endpoint
   * @public
   * @param options - host, path, and token are required
   * @param authProvider - [DEPRECATED - use `authType: 'custom'] Optional custom authentication provider
   * @returns Session object that can be used to execute statements
   * @example
   * const session = client.connect({host, path, token});
   */
  public async connect(options: ConnectionOptions, authProvider?: IAuthentication): Promise<IDBSQLClient> {
    const deprecatedClientId = (options as any).clientId;
    if (deprecatedClientId !== undefined) {
      this.logger.log(
        LogLevel.warn,
        'Warning: The "clientId" option is deprecated. Please use "userAgentEntry" instead.',
      );
      if (!options.userAgentEntry) {
        options.userAgentEntry = deprecatedClientId;
      }
    }

    // Store host for telemetry
    this.host = options.host;

    // Store enableMetricViewMetadata configuration
    if (options.enableMetricViewMetadata !== undefined) {
      this.config.enableMetricViewMetadata = options.enableMetricViewMetadata;
    }

    // Override telemetry config if provided in options
    if (options.telemetryEnabled !== undefined) {
      this.config.telemetryEnabled = options.telemetryEnabled;
    }
    if (options.telemetryBatchSize !== undefined) {
      this.config.telemetryBatchSize = options.telemetryBatchSize;
    }
    if (options.telemetryAuthenticatedExport !== undefined) {
      this.config.telemetryAuthenticatedExport = options.telemetryAuthenticatedExport;
    }

    this.authProvider = this.createAuthProvider(options, authProvider);

    this.connectionProvider = this.createConnectionProvider(options);

    const thriftConnection = await this.connectionProvider.getThriftConnection();

    thriftConnection.on('error', (error: Error) => {
      // Error.stack already contains error type and message, so log stack if available,
      // otherwise fall back to just error type + message
      this.logger.log(LogLevel.error, error.stack || `${error.name}: ${error.message}`);
      try {
        this.emit('error', error);
      } catch (e) {
        // EventEmitter will throw unhandled error when emitting 'error' event.
        // Since we already logged it few lines above, just suppress this behaviour
      }
    });

    thriftConnection.on('reconnecting', (params: { delay: number; attempt: number }) => {
      this.logger.log(LogLevel.debug, `Reconnecting, params: ${JSON.stringify(params)}`);
      this.emit('reconnecting', params);
    });

    thriftConnection.on('close', () => {
      this.logger.log(LogLevel.debug, 'Closing connection.');
      this.emit('close');
    });

    thriftConnection.on('timeout', () => {
      this.logger.log(LogLevel.debug, 'Connection timed out.');
      this.emit('timeout');
    });

    // Initialize telemetry if enabled
    if (this.config.telemetryEnabled) {
      await this.initializeTelemetry();
    }

    return this;
  }

  /**
   * Starts new session
   * @public
   * @param request - Can be instantiated with initialSchema, empty by default
   * @returns Session object that can be used to execute statements
   * @throws {StatusError}
   * @example
   * const session = await client.openSession();
   */
  public async openSession(request: OpenSessionRequest = {}): Promise<IDBSQLSession> {
    // Track connection open latency
    const startTime = Date.now();

    // Prepare session configuration
    const configuration = request.configuration ? { ...request.configuration } : {};

    // Add metric view metadata config if enabled
    if (this.config.enableMetricViewMetadata) {
      configuration['spark.sql.thriftserver.metadata.metricview.enabled'] = 'true';
    }

    const response = await this.driver.openSession({
      client_protocol_i64: new Int64(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8),
      ...getInitialNamespaceOptions(request.initialCatalog, request.initialSchema),
      configuration,
      canUseMultipleCatalogs: true,
    });

    Status.assert(response.status);
    const session = new DBSQLSession({
      handle: definedOrError(response.sessionHandle),
      context: this,
      serverProtocolVersion: response.serverProtocolVersion,
    });
    this.sessions.add(session);

    // Emit connection.open telemetry event
    if (this.telemetryEmitter && this.host) {
      try {
        const latencyMs = Date.now() - startTime;
        const workspaceId = this.extractWorkspaceId(this.host);
        const driverConfig = this.buildDriverConfiguration();
        this.telemetryEmitter.emitConnectionOpen({
          sessionId: session.id,
          workspaceId,
          driverConfig,
          latencyMs,
        });
      } catch (error: any) {
        // CRITICAL: All telemetry exceptions swallowed
        this.logger.log(LogLevel.debug, `Error emitting connection.open event: ${error.message}`);
      }
    }

    return session;
  }

  public async close(): Promise<void> {
    await this.sessions.closeAll();

    // Cleanup telemetry
    if (this.host) {
      try {
        // Step 1: Close aggregator (stops timer, completes statements, final flush)
        if (this.telemetryAggregator) {
          this.telemetryAggregator.close();
        }

        // Step 2: Release telemetry client (decrements ref count, closes if last)
        if (this.telemetryClientProvider) {
          await this.telemetryClientProvider.releaseClient(this.host);
        }

        // Step 3: Release feature flag context (decrements ref count)
        if (this.featureFlagCache) {
          this.featureFlagCache.releaseContext(this.host);
        }
      } catch (error: any) {
        // Swallow all telemetry cleanup errors
        this.logger.log(LogLevel.debug, `Telemetry cleanup error: ${error.message}`);
      }
    }

    this.client = undefined;
    this.connectionProvider = undefined;
    this.authProvider = undefined;
  }

  public getConfig(): ClientConfig {
    return this.config;
  }

  public getLogger(): IDBSQLLogger {
    return this.logger;
  }

  public async getConnectionProvider(): Promise<IConnectionProvider> {
    if (!this.connectionProvider) {
      throw new HiveDriverError('DBSQLClient: not connected');
    }

    return this.connectionProvider;
  }

  public async getClient(): Promise<IThriftClient> {
    const connectionProvider = await this.getConnectionProvider();

    if (!this.client) {
      this.logger.log(LogLevel.info, 'DBSQLClient: initializing thrift client');
      this.client = this.thrift.createClient(TCLIService, await connectionProvider.getThriftConnection());
    }

    if (this.authProvider) {
      const authHeaders = await this.authProvider.authenticate();
      connectionProvider.setHeaders(authHeaders);
    }

    return this.client;
  }

  public async getDriver(): Promise<IDriver> {
    return this.driver;
  }

  /**
   * Gets authentication headers for HTTP requests.
   * Used by telemetry and feature flag fetching to authenticate REST API calls.
   * @returns Promise resolving to headers object with authentication, or empty object if no auth
   */
  public async getAuthHeaders(): Promise<HeadersInit> {
    if (this.authProvider) {
      try {
        return await this.authProvider.authenticate();
      } catch (error) {
        this.logger.log(LogLevel.debug, `Error getting auth headers: ${error}`);
        return {};
      }
    }
    return {};
  }
}
