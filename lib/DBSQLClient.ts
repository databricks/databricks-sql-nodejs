import thrift from 'thrift';
import Int64 from 'node-int64';
import os from 'os';

import { EventEmitter } from 'events';
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
import { buildUserAgentString, definedOrError, serializeQueryTags } from './utils';
import PlainHttpAuthentication from './connection/auth/PlainHttpAuthentication';
import DatabricksOAuth, { OAuthFlow } from './connection/auth/DatabricksOAuth';
import {
  TokenProviderAuthenticator,
  StaticTokenProvider,
  ExternalTokenProvider,
  CachedTokenProvider,
  FederationProvider,
  ITokenProvider,
} from './connection/auth/tokenProvider';
import IDBSQLLogger, { LogLevel } from './contracts/IDBSQLLogger';
import DBSQLLogger from './DBSQLLogger';
import CloseableCollection from './utils/CloseableCollection';
import IConnectionProvider from './connection/contracts/IConnectionProvider';
import TelemetryClient from './telemetry/TelemetryClient';
import TelemetryClientProvider from './telemetry/TelemetryClientProvider';
import TelemetryEventEmitter from './telemetry/TelemetryEventEmitter';
import MetricsAggregator from './telemetry/MetricsAggregator';
import { DriverConfiguration, DRIVER_NAME, TelemetryEventType, DEFAULT_TELEMETRY_CONFIG } from './telemetry/types';
import { safeEmit } from './telemetry/telemetryUtils';
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

/**
 * Copy any defined telemetry knob from `src` into `dst`. Both objects declare
 * identical types for these keys, so the assignment is structurally typed —
 * a wrong-shape value in `ConnectionOptions` is caught at the call site.
 *
 * Keep this in sync with the `telemetry*` knobs exposed in
 * `ConnectionOptions` (lib/contracts/IDBSQLClient.ts) and `ClientConfig`
 * (lib/contracts/IClientContext.ts). Adding a knob requires extending this
 * list AND the public option surface; otherwise the user-supplied override
 * silently does nothing.
 */
function copyDefinedTelemetryOptions(src: ConnectionOptions, dst: ClientConfig): void {
  if (src.telemetryEnabled !== undefined) dst.telemetryEnabled = src.telemetryEnabled;
  if (src.telemetryBatchSize !== undefined) dst.telemetryBatchSize = src.telemetryBatchSize;
  if (src.telemetryFlushIntervalMs !== undefined) dst.telemetryFlushIntervalMs = src.telemetryFlushIntervalMs;
  if (src.telemetryMaxRetries !== undefined) dst.telemetryMaxRetries = src.telemetryMaxRetries;
  if (src.telemetryAuthenticatedExport !== undefined)
    dst.telemetryAuthenticatedExport = src.telemetryAuthenticatedExport;
  if (src.telemetryCircuitBreakerThreshold !== undefined)
    dst.telemetryCircuitBreakerThreshold = src.telemetryCircuitBreakerThreshold;
  if (src.telemetryCircuitBreakerTimeout !== undefined)
    dst.telemetryCircuitBreakerTimeout = src.telemetryCircuitBreakerTimeout;
  if (src.telemetryCloseTimeoutMs !== undefined) dst.telemetryCloseTimeoutMs = src.telemetryCloseTimeoutMs;
  if (src.telemetryMaxStatementMetrics !== undefined)
    dst.telemetryMaxStatementMetrics = src.telemetryMaxStatementMetrics;
  if (src.telemetryMaxPendingMetrics !== undefined) dst.telemetryMaxPendingMetrics = src.telemetryMaxPendingMetrics;
}

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

  // Telemetry components — `telemetryClient` is the shared per-host owner
  // (process-wide via TelemetryClientProvider). The exporter, aggregator,
  // circuit-breaker registry and feature-flag cache live on it. Each
  // DBSQLClient still owns its own `telemetryEmitter` so it respects its
  // own `telemetryEnabled` flag.
  private host?: string;

  private httpPath?: string;

  private authType?: string;

  private telemetryClient?: TelemetryClient;

  private telemetryEmitter?: TelemetryEventEmitter;

  // True once we've shipped the full DriverConfiguration on a CONNECTION_OPEN
  // event for this client. Subsequent openSession events for the same client
  // strip the (~1KB, static-for-the-process) blob — a long-running client
  // opening N sessions would otherwise pay N×blob bytes for telemetry on data
  // that hasn't changed since the first session.
  private driverConfigShipped = false;

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

      // Telemetry defaults are sourced from DEFAULT_TELEMETRY_CONFIG so
      // every component reads from the same single frozen const. Mapping the
      // unprefixed TelemetryConfiguration keys to the `telemetry`-prefixed
      // ClientConfig keys is mechanical; doing it once here means that adding
      // a new knob to DEFAULT_TELEMETRY_CONFIG only requires extending the
      // ClientConfig surface and (optionally) adding to telemetryOverrides.
      // Previously this method declared 7 keys while DEFAULT_TELEMETRY_CONFIG
      // declared 15 — silent desync risk every time someone touched one but
      // not the other.
      telemetryEnabled: DEFAULT_TELEMETRY_CONFIG.enabled,
      telemetryBatchSize: DEFAULT_TELEMETRY_CONFIG.batchSize,
      telemetryFlushIntervalMs: DEFAULT_TELEMETRY_CONFIG.flushIntervalMs,
      telemetryMaxRetries: DEFAULT_TELEMETRY_CONFIG.maxRetries,
      telemetryBackoffBaseMs: DEFAULT_TELEMETRY_CONFIG.backoffBaseMs,
      telemetryBackoffMaxMs: DEFAULT_TELEMETRY_CONFIG.backoffMaxMs,
      telemetryBackoffJitterMs: DEFAULT_TELEMETRY_CONFIG.backoffJitterMs,
      telemetryAuthenticatedExport: DEFAULT_TELEMETRY_CONFIG.authenticatedExport,
      telemetryCircuitBreakerThreshold: DEFAULT_TELEMETRY_CONFIG.circuitBreakerThreshold,
      telemetryCircuitBreakerTimeout: DEFAULT_TELEMETRY_CONFIG.circuitBreakerTimeout,
      telemetryMaxPendingMetrics: DEFAULT_TELEMETRY_CONFIG.maxPendingMetrics,
      telemetryMaxErrorsPerStatement: DEFAULT_TELEMETRY_CONFIG.maxErrorsPerStatement,
      telemetryStatementTtlMs: DEFAULT_TELEMETRY_CONFIG.statementTtlMs,
      telemetryCloseTimeoutMs: DEFAULT_TELEMETRY_CONFIG.closeTimeoutMs,
      telemetryMaxStatementMetrics: DEFAULT_TELEMETRY_CONFIG.maxStatementMetrics,
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
      case 'token-provider':
        return new TokenProviderAuthenticator(
          this.wrapTokenProvider(
            options.tokenProvider,
            options.host,
            options.enableTokenFederation,
            options.federationClientId,
          ),
          this,
        );
      case 'external-token':
        return new TokenProviderAuthenticator(
          this.wrapTokenProvider(
            new ExternalTokenProvider(options.getToken),
            options.host,
            options.enableTokenFederation,
            options.federationClientId,
          ),
          this,
        );
      case 'static-token':
        return new TokenProviderAuthenticator(
          this.wrapTokenProvider(
            StaticTokenProvider.fromJWT(options.staticToken),
            options.host,
            options.enableTokenFederation,
            options.federationClientId,
          ),
          this,
        );
      // no default
    }
  }

  /**
   * Wraps a token provider with caching and optional federation.
   * Caching is always enabled by default. Federation is opt-in.
   */
  private wrapTokenProvider(
    provider: ITokenProvider,
    host: string,
    enableFederation?: boolean,
    federationClientId?: string,
  ): ITokenProvider {
    // Always wrap with caching first
    let wrapped: ITokenProvider = new CachedTokenProvider(provider);

    // Optionally wrap with federation
    if (enableFederation) {
      wrapped = new FederationProvider(wrapped, host, {
        clientId: federationClientId,
      });
    }

    return wrapped;
  }

  private createConnectionProvider(options: ConnectionOptions): IConnectionProvider {
    return new HttpConnection(this.getConnectionOptions(options), this);
  }

  /**
   * Extract the numeric workspace ID for telemetry.
   *
   * The only reliable carrier in the connection params today is the `?o=N`
   * query parameter on `httpPath` — Databricks SQL warehouses are typically
   * connected to via paths like `/sql/1.0/warehouses/<id>?o=12345678901234`.
   *
   * Host-based extraction was tried previously but produced confidently-wrong
   * values:
   *  - AWS `dbc-XXXXX-YYYY.cloud.databricks.com` → `dbc-XXXXX-YYYY`
   *    is the deployment shard prefix, not the workspace ID.
   *  - Azure `adb-NNNNNNNNNNNNN.NN.azuredatabricks.net` → the workspace ID is
   *    the numeric portion after the `adb-` prefix (and before the form-factor
   *    digit), not `adb-NNN`.
   *
   * Returns `undefined` when no workspace ID can be derived. Server-side
   * attribution is better off seeing a missing field than a wrong value.
   */
  private extractWorkspaceId(): string | undefined {
    const { httpPath } = this;
    if (!httpPath) {
      return undefined;
    }
    const queryIdx = httpPath.indexOf('?');
    if (queryIdx < 0) {
      return undefined;
    }
    const query = httpPath.slice(queryIdx + 1);
    // Match `o=<digits>` as the first param, an inner `&o=<digits>`, etc.
    // Workspace IDs are decimal integers; reject anything else so a stray
    // `o=tenant_42` doesn't ship as a workspace ID.
    const match = query.match(/(?:^|&)o=(\d+)(?:&|$)/);
    return match ? match[1] : undefined;
  }

  /**
   * Build the customHeaders map applied to telemetry POSTs and feature-flag
   * GETs (SPOG / Single Panel of Glass support). When `httpPath` carries
   * `?o=<workspaceId>` — account-level vanity URL routing — endpoints that
   * don't include the workspace in their path need the workspace conveyed via
   * the `x-databricks-org-id` header instead. A user-supplied value in
   * `options.customHeaders` (case-insensitively keyed) wins over the parsed
   * value.
   */
  private buildCustomHeaders(options: ConnectionOptions): Record<string, string> | undefined {
    const merged: Record<string, string> = { ...(options.customHeaders ?? {}) };
    const hasOrgIdAlready = Object.keys(merged).some((k) => k.toLowerCase() === 'x-databricks-org-id');
    if (!hasOrgIdAlready) {
      const orgId = this.extractWorkspaceId();
      if (orgId) {
        merged['x-databricks-org-id'] = orgId;
      }
    }
    return Object.keys(merged).length > 0 ? merged : undefined;
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
      authType: this.authType || 'pat',

      // Feature flags
      cloudFetchEnabled: this.config.useCloudFetch ?? false,
      lz4Enabled: this.config.useLZ4Compression ?? false,
      arrowEnabled: this.config.arrowEnabled ?? false,
      directResultsEnabled: true, // Direct results always enabled

      // Configuration values
      socketTimeout: this.config.socketTimeout ?? 0,
      retryMaxAttempts: this.config.retryMaxAttempts ?? 0,
      cloudFetchConcurrentDownloads: this.config.cloudFetchConcurrentDownloads ?? 0,

      // Connection parameters
      httpPath: this.httpPath,
      enableMetricViewMetadata: this.config.enableMetricViewMetadata,
    };
  }

  /**
   * Map Node.js auth type to telemetry auth enum string.
   * Distinguishes between U2M and M2M OAuth flows.
   */
  private mapAuthType(options: ConnectionOptions): string {
    switch (options.authType) {
      case 'databricks-oauth':
        return options.oauthClientSecret === undefined ? 'external-browser' : 'oauth-m2m';
      case 'custom':
        return 'custom';
      case 'token-provider':
        return 'token-provider';
      case 'external-token':
        return 'external-token';
      case 'static-token':
        return 'static-token';
      case 'access-token':
      case undefined:
        return 'pat';
      default:
        return 'unknown';
    }
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
      // Acquire (or create) the per-host TelemetryClient from the
      // process-wide provider. The shared client owns the circuit-breaker
      // registry, feature-flag cache, exporter, and aggregator. Multiple
      // DBSQLClient instances on the same host share these resources so
      // breaker counters and HTTP batches don't fragment per-instance.
      this.telemetryClient = TelemetryClientProvider.getInstance().getOrCreateClient(this, this.host);

      // Use the shared feature-flag cache (registered in the previous step).
      const enabled = await this.telemetryClient.getFeatureFlagCache().isTelemetryEnabled(this.host);

      if (!enabled) {
        // Release our refcount immediately; we won't be emitting.
        await TelemetryClientProvider.getInstance().releaseClient(this, this.host);
        this.telemetryClient = undefined;
        this.logger.log(LogLevel.debug, 'Telemetry: disabled');
        return;
      }

      // Each DBSQLClient still owns its own emitter so it respects its own
      // `telemetryEnabled` flag and feature-flag result. All emitters bridge
      // into the SHARED aggregator on the TelemetryClient.
      this.telemetryEmitter = new TelemetryEventEmitter(this);
      const sharedAggregator = this.telemetryClient.getAggregator();
      for (const eventType of Object.values(TelemetryEventType)) {
        this.telemetryEmitter.on(eventType, (event) => {
          sharedAggregator.processEvent(event);
        });
      }

      this.logger.log(LogLevel.debug, 'Telemetry: enabled');
    } catch (error: any) {
      // Swallow all telemetry initialization errors. If we acquired a refcount
      // before the throw, release it — otherwise the per-host TelemetryClient
      // (and its flush timer / exporter / FFCache) leaks for the lifetime of
      // the process on long-running supervisors that retry-connect.
      if (this.telemetryClient) {
        try {
          await TelemetryClientProvider.getInstance().releaseClient(this, this.host);
        } catch (releaseError: any) {
          this.logger.log(
            LogLevel.debug,
            `Telemetry release-after-init-failure error: ${releaseError?.message ?? releaseError}`,
          );
        }
        this.telemetryClient = undefined;
        this.telemetryEmitter = undefined;
      }
      this.logger.log(LogLevel.debug, `Telemetry initialization error: ${error?.message ?? error}`);
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

    // If connect() is being called a second time (reconnect, host switch),
    // release the prior telemetry refcount and emitter so we don't leak a
    // refcount in the process-wide TelemetryClientProvider for the old host.
    if (this.host && this.telemetryClient) {
      try {
        await TelemetryClientProvider.getInstance().releaseClient(this, this.host);
      } catch (error: any) {
        this.logger.log(LogLevel.debug, `Telemetry release-on-reconnect error: ${error.message}`);
      }
      this.telemetryClient = undefined;
      this.telemetryEmitter = undefined;
    }
    // Re-arm: the new connection is a fresh client-config lineage even if
    // the host is the same.
    this.driverConfigShipped = false;

    // Store connection params for telemetry
    this.host = options.host;
    this.httpPath = options.path;
    this.authType = this.mapAuthType(options);

    // Store enableMetricViewMetadata configuration
    if (options.enableMetricViewMetadata !== undefined) {
      this.config.enableMetricViewMetadata = options.enableMetricViewMetadata;
    }

    // Override telemetry config if provided in options. Per-key narrowed copy
    // preserves the structural type system: `ConnectionOptions` and
    // `ClientConfig` declare identical types for these knobs, so a user
    // passing `telemetryBatchSize: "100"` (string) gets a TS error instead of
    // silently writing a string into a number field that `MetricsAggregator`
    // would later read and break aggregation thresholds at runtime.
    copyDefinedTelemetryOptions(options, this.config);

    // Persist userAgentEntry so telemetry and feature-flag call sites reuse
    // the same value as the primary Thrift connection's User-Agent.
    if (options.userAgentEntry !== undefined) {
      this.config.userAgentEntry = options.userAgentEntry;
    }

    // SPOG: parse `?o=<workspaceId>` out of httpPath and stash it as
    // `x-databricks-org-id` for the telemetry + feature-flag clients, which
    // hit endpoints that don't carry the workspace in their URL path.
    this.config.customHeaders = this.buildCustomHeaders(options);

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

    // Initialize telemetry if enabled. The env var DATABRICKS_TELEMETRY_DISABLED
    // is a hard kill switch for ops/IT teams who can't redeploy app code.
    // Recognized truthy values: 1, true, yes, on (case-insensitive). Anything
    // else (empty, "0", "false", "no", "off") leaves the runtime config in
    // charge — avoiding the footgun where a sysadmin sets the var to "false"
    // expecting to enable telemetry.
    const envKill = process.env.DATABRICKS_TELEMETRY_DISABLED;
    const trimmedEnvKill = typeof envKill === 'string' ? envKill.trim() : '';
    const envDisabled = trimmedEnvKill.length > 0 && /^(1|true|yes|on)$/i.test(trimmedEnvKill);
    // Surface the misconfiguration: an ops engineer who sees the var name and
    // tries to "set it to false to keep telemetry on" otherwise gets the
    // opposite of what they expect (the var is then silently ignored, runtime
    // config stays in charge — default `true`). Warn on any non-empty value
    // that isn't recognized so the disable-failed shape is visible in logs.
    if (trimmedEnvKill.length > 0 && !envDisabled) {
      this.logger.log(
        LogLevel.warn,
        `DATABRICKS_TELEMETRY_DISABLED='${trimmedEnvKill}' was ignored. ` +
          `To disable telemetry, set the variable to one of: 1, true, yes, on. ` +
          `Telemetry remains controlled by the runtime config and feature flag.`,
      );
    }
    if (this.config.telemetryEnabled && !envDisabled) {
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

    // Serialize queryTags dict and set in configuration; takes precedence over configuration.QUERY_TAGS
    if (request.queryTags !== undefined) {
      const serialized = serializeQueryTags(request.queryTags);
      if (serialized) {
        configuration.QUERY_TAGS = serialized;
      } else {
        delete configuration.QUERY_TAGS;
      }
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

    // Emit connection.open telemetry event. The DriverConfiguration blob
    // (~1KB: runtime/OS/locale/process info) is static for the lifetime of
    // this DBSQLClient — ship it once, on the first openSession, and omit
    // on subsequent sessions for the same client. Server-side correlation
    // by sessionId still groups N sessions under the first event's config.
    safeEmit(this, (emitter) => {
      if (!this.host) return;
      const latencyMs = Date.now() - startTime;
      const workspaceId = this.extractWorkspaceId();
      const driverConfig = this.driverConfigShipped ? undefined : this.buildDriverConfiguration();
      if (driverConfig) {
        this.driverConfigShipped = true;
      }
      emitter.emitConnectionOpen({
        sessionId: session.id,
        workspaceId,
        driverConfig,
        latencyMs,
      });
    });

    return session;
  }

  /**
   * Closes the client, releasing sessions and telemetry resources.
   *
   * The internal telemetry flush timer uses `setInterval(...).unref()` so it
   * cannot keep the Node.js process alive on its own. As a consequence, any
   * telemetry buffered between flush ticks is lost if the process exits
   * without calling `close()`. Long-lived applications should `await` this
   * method on shutdown so the aggregator drains its remaining metrics.
   */
  public async close(): Promise<void> {
    await this.sessions.closeAll();

    // Cleanup telemetry. Releasing our refcount on the shared TelemetryClient
    // is awaited because the underlying close() drains the final HTTP POST —
    // a caller doing `await client.close(); process.exit(0)` would otherwise
    // truncate the in-flight request when this is the last refcount holder.
    if (this.host && this.telemetryClient) {
      try {
        await TelemetryClientProvider.getInstance().releaseClient(this, this.host);
      } catch (error: any) {
        this.logger.log(LogLevel.debug, `Telemetry cleanup error: ${error.message}`);
      }
      this.telemetryClient = undefined;
    }
    // Drop the emitter ref so post-close calls (e.g. session.close racing
    // with client.close) cannot smuggle events into the closed aggregator.
    this.telemetryEmitter = undefined;

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
   * Returns the authentication provider associated with this client, if any.
   * Intended for internal telemetry/feature-flag call sites that need to
   * obtain auth headers directly without routing through `IClientContext`.
   *
   * @internal Not part of the public API. May change without notice.
   */
  public getAuthProvider(): IAuthentication | undefined {
    return this.authProvider;
  }

  /** @internal */
  public getTelemetryEmitter(): TelemetryEventEmitter | undefined {
    return this.telemetryEmitter;
  }

  /** @internal */
  public getTelemetryAggregator(): MetricsAggregator | undefined {
    return this.telemetryClient?.getAggregator();
  }

  /**
   * Operator-visible snapshot of the client's telemetry state: current
   * buffer depth, in-flight statement aggregations, cumulative drops/
   * evictions, and circuit-breaker state. Returns `undefined` when
   * telemetry is disabled (config, env-kill, or feature-flag).
   *
   * Use this in health-check endpoints or shutdown banners to verify that
   * telemetry is flowing. A non-zero `droppedMetrics` between observations
   * means buffer overflow — raise `telemetryMaxPendingMetrics`.
   */
  public getTelemetryStats():
    | {
        host: string;
        pendingMetricsCount: number;
        inFlightStatements: number;
        droppedMetrics: number;
        evictedStatements: number;
        circuitBreakerState: string;
      }
    | undefined {
    return this.telemetryClient?.getTelemetryStats();
  }
}
