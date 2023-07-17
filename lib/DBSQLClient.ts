import thrift from 'thrift';

import { EventEmitter } from 'events';
import TCLIService from '../thrift/TCLIService';
import { TProtocolVersion } from '../thrift/TCLIService_types';
import IDBSQLClient, { ConnectionOptions, OpenSessionRequest, ClientOptions } from './contracts/IDBSQLClient';
import HiveDriver from './hive/HiveDriver';
import { Int64 } from './hive/Types';
import DBSQLSession from './DBSQLSession';
import IDBSQLSession from './contracts/IDBSQLSession';
import IThriftConnection from './connection/contracts/IThriftConnection';
import IConnectionProvider from './connection/contracts/IConnectionProvider';
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
  private client: TCLIService.Client | null;

  private connection: IThriftConnection | null;

  private connectionProvider: IConnectionProvider;

  private readonly logger: IDBSQLLogger;

  private readonly thrift = thrift;

  constructor(options?: ClientOptions) {
    super();
    this.connectionProvider = new HttpConnection();
    this.logger = options?.logger || new DBSQLLogger();
    this.client = null;
    this.connection = null;
    this.logger.log(LogLevel.info, 'Created DBSQLClient');
  }

  private getConnectionOptions(options: ConnectionOptions): IConnectionOptions {
    const {
      host,
      port,
      path,
      clientId,
      authType,
      // @ts-expect-error TS2339: Property 'token' does not exist on type 'ConnectionOptions'
      token,
      // @ts-expect-error TS2339: Property 'persistence' does not exist on type 'ConnectionOptions'
      persistence,
      // @ts-expect-error TS2339: Property 'provider' does not exist on type 'ConnectionOptions'
      provider,
      ...otherOptions
    } = options;

    return {
      host,
      port: port || 443,
      options: {
        path: prependSlash(path),
        https: true,
        ...otherOptions,
        headers: {
          'User-Agent': buildUserAgentString(options.clientId),
        },
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
    authProvider = this.getAuthProvider(options, authProvider);

    this.connection = await this.connectionProvider.connect(this.getConnectionOptions(options), authProvider);

    this.client = this.thrift.createClient(TCLIService, this.connection.getConnection());

    this.connection.getConnection().on('error', (error: Error) => {
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

    this.connection.getConnection().on('reconnecting', (params: { delay: number; attempt: number }) => {
      this.logger.log(LogLevel.debug, `Reconnecting, params: ${JSON.stringify(params)}`);
      this.emit('reconnecting', params);
    });

    this.connection.getConnection().on('close', () => {
      this.logger.log(LogLevel.debug, 'Closing connection.');
      this.emit('close');
    });

    this.connection.getConnection().on('timeout', () => {
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
    if (!this.connection?.isConnected()) {
      throw new HiveDriverError('DBSQLClient: connection is lost');
    }

    const driver = new HiveDriver(this.getClient());

    const response = await driver.openSession({
      client_protocol_i64: new Int64(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6),
      ...getInitialNamespaceOptions(request.initialCatalog, request.initialSchema),
    });

    Status.assert(response.status);
    return new DBSQLSession(driver, definedOrError(response.sessionHandle), this.logger);
  }

  public getClient() {
    if (!this.client) {
      throw new HiveDriverError('DBSQLClient: client is not initialized');
    }

    return this.client;
  }

  public async close(): Promise<void> {
    if (this.connection) {
      const thriftConnection = this.connection.getConnection();

      if (typeof thriftConnection.end === 'function') {
        this.connection.getConnection().end();
      }

      this.connection = null;
    }
  }
}
