import thrift from 'thrift';

import { EventEmitter } from 'events';
import TCLIService from '../thrift/TCLIService';
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
import HiveDriverError from './errors/HiveDriverError';
import { buildUserAgentString } from './utils';
import IBackend from './contracts/IBackend';
import ThriftBackend from './thrift-backend/ThriftBackend';
import SeaBackend from './sea/SeaBackend';
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

function prependSlash(str: string): string {
  if (str.length > 0 && str.charAt(0) !== '/') {
    return `/${str}`;
  }
  return str;
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

  private backend?: IBackend;

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

    // Store enableMetricViewMetadata configuration
    if (options.enableMetricViewMetadata !== undefined) {
      this.config.enableMetricViewMetadata = options.enableMetricViewMetadata;
    }

    // Persist userAgentEntry so telemetry and feature-flag call sites reuse
    // the same value as the primary Thrift connection's User-Agent.
    if (options.userAgentEntry !== undefined) {
      this.config.userAgentEntry = options.userAgentEntry;
    }

    this.authProvider = this.createAuthProvider(options, authProvider);

    this.connectionProvider = this.createConnectionProvider(options);

    this.backend = options.useSEA
      ? new SeaBackend()
      : new ThriftBackend({
          context: this,
          onConnectionEvent: (event, payload) => this.forwardConnectionEvent(event, payload),
        });

    await this.backend.connect(options);

    return this;
  }

  private forwardConnectionEvent(event: 'error' | 'reconnecting' | 'close' | 'timeout', payload?: unknown): void {
    switch (event) {
      case 'error': {
        const error = payload as Error;
        this.logger.log(LogLevel.error, error.stack || `${error.name}: ${error.message}`);
        try {
          this.emit('error', error);
        } catch (e) {
          // EventEmitter throws when 'error' has no listeners; we've already logged it.
        }
        return;
      }
      case 'reconnecting':
        this.logger.log(LogLevel.debug, `Reconnecting, params: ${JSON.stringify(payload)}`);
        this.emit('reconnecting', payload);
        return;
      case 'close':
        this.logger.log(LogLevel.debug, 'Closing connection.');
        this.emit('close');
        return;
      case 'timeout':
        this.logger.log(LogLevel.debug, 'Connection timed out.');
        this.emit('timeout');
      // no default
    }
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
    if (!this.backend) {
      throw new HiveDriverError('DBSQLClient: not connected');
    }
    const sessionBackend = await this.backend.openSession(request);
    const session = new DBSQLSession({ backend: sessionBackend, context: this });
    this.sessions.add(session);
    return session;
  }

  public async close(): Promise<void> {
    await this.sessions.closeAll();
    await this.backend?.close();

    this.backend = undefined;
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
}
