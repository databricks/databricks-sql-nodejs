import thrift from 'thrift';
import Int64 from 'node-int64';

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
import { buildUserAgentString, definedOrError } from './utils';
import PlainHttpAuthentication from './connection/auth/PlainHttpAuthentication';
import DatabricksOAuth, { OAuthFlow } from './connection/auth/DatabricksOAuth';
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

      useCloudFetch: false,
      cloudFetchConcurrentDownloads: 10,

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
        'User-Agent': buildUserAgentString(options.userAgentHeader),
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
   * Connects DBSQLClient to endpoint
   * @public
   * @param options - host, path, and token are required
   * @param authProvider - [DEPRECATED - use `authType: 'custom'] Optional custom authentication provider
   * @returns Session object that can be used to execute statements
   * @example
   * const session = client.connect({host, path, token});
   */
  public async connect(options: ConnectionOptions, authProvider?: IAuthentication): Promise<IDBSQLClient> {
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
    const response = await this.driver.openSession({
      client_protocol_i64: new Int64(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8),
      ...getInitialNamespaceOptions(request.initialCatalog, request.initialSchema),
      canUseMultipleCatalogs: true,
    });

    Status.assert(response.status);
    const session = new DBSQLSession({
      handle: definedOrError(response.sessionHandle),
      context: this,
    });
    this.sessions.add(session);
    return session;
  }

  public async close(): Promise<void> {
    await this.sessions.closeAll();

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
}
