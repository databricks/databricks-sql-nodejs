import thrift from 'thrift';

import { EventEmitter } from 'events';
import TCLIService from '../thrift/TCLIService';
import { TProtocolVersion } from '../thrift/TCLIService_types';
import IDBSQLClient, { ClientOptions, ConnectionOptions, OpenSessionRequest } from './contracts/IDBSQLClient';
import HiveDriver from './hive/HiveDriver';
import { Int64 } from './hive/Types';
import DBSQLSession from './DBSQLSession';
import IDBSQLSession from './contracts/IDBSQLSession';
import IAuthentication from './connection/contracts/IAuthentication';
import HttpConnection from './connection/connections/HttpConnection';
import IConnectionOptions from './connection/contracts/IConnectionOptions';
import Status from './dto/Status';
import HiveDriverError from './errors/HiveDriverError';
import { buildUserAgentString, definedOrError } from './utils';
import PlainHttpAuthentication from './connection/auth/PlainHttpAuthentication';
import DatabricksOAuth from './connection/auth/DatabricksOAuth';
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

export default class DBSQLClient extends EventEmitter implements IDBSQLClient {
  private connectionProvider?: IConnectionProvider;

  private authProvider?: IAuthentication;

  private client?: TCLIService.Client;

  private readonly logger: IDBSQLLogger;

  private readonly thrift = thrift;

  private sessions = new CloseableCollection<DBSQLSession>();

  constructor(options?: ClientOptions) {
    super();
    this.logger = options?.logger || new DBSQLLogger();
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
        'User-Agent': buildUserAgentString(options.clientId),
      },
    };
  }

  private getAuthProvider(options: ConnectionOptions, authProvider?: IAuthentication): IAuthentication {
    if (authProvider) {
      return authProvider;
    }

    switch (options.authType) {
      case undefined:
      case 'access-token':
        return new PlainHttpAuthentication({
          username: 'token',
          password: options.token,
        });
      case 'databricks-oauth':
        return new DatabricksOAuth({
          host: options.host,
          logger: this.logger,
          persistence: options.persistence,
          azureTenantId: options.azureTenantId,
          clientId: options.oauthClientId,
          clientSecret: options.oauthClientSecret,
        });
      case 'custom':
        return options.provider;
      // no default
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
    this.authProvider = this.getAuthProvider(options, authProvider);

    this.connectionProvider = new HttpConnection(this.getConnectionOptions(options));

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
    const driver = new HiveDriver(() => this.getClient());

    const response = await driver.openSession({
      client_protocol_i64: new Int64(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8),
      ...getInitialNamespaceOptions(request.initialCatalog, request.initialSchema),
    });

    Status.assert(response.status);
    const session = new DBSQLSession(driver, definedOrError(response.sessionHandle), {
      logger: this.logger,
    });
    this.sessions.add(session);
    return session;
  }

  private async getClient() {
    if (!this.connectionProvider) {
      throw new HiveDriverError('DBSQLClient: not connected');
    }

    if (!this.client) {
      this.logger.log(LogLevel.info, 'DBSQLClient: initializing thrift client');
      this.client = this.thrift.createClient(TCLIService, await this.connectionProvider.getThriftConnection());
    }

    if (this.authProvider) {
      const agent = await this.connectionProvider.getAgent();
      const authHeaders = await this.authProvider.authenticate(agent);
      this.connectionProvider.setHeaders(authHeaders);
    }

    return this.client;
  }

  public async close(): Promise<void> {
    await this.sessions.closeAll();

    this.client = undefined;
    this.connectionProvider = undefined;
    this.authProvider = undefined;
  }
}
