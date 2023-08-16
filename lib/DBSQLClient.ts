import thrift, { HttpHeaders } from 'thrift';

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
import { areHeadersEqual, buildUserAgentString, definedOrError } from './utils';
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
  private client: TCLIService.Client | null = null;

  private authProvider: IAuthentication | null = null;

  private connectionOptions: ConnectionOptions | null = null;

  private additionalHeaders: HttpHeaders = {};

  private readonly logger: IDBSQLLogger;

  private readonly thrift = thrift;

  private stagingAllowedLocalPath: string[] | null

  constructor(options?: ClientOptions) {
    super();
    this.logger = options?.logger || new DBSQLLogger();
    this.logger.log(LogLevel.info, 'Created DBSQLClient');
    this.stagingAllowedLocalPath = options?.stagingAllowedLocalPath || null
  }

  private getConnectionOptions(options: ConnectionOptions, headers: HttpHeaders): IConnectionOptions {
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
          ...headers,
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
    this.authProvider = this.getAuthProvider(options, authProvider);
    this.connectionOptions = options;
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
      client_protocol_i64: new Int64(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6),
      ...getInitialNamespaceOptions(request.initialCatalog, request.initialSchema),
    });

    Status.assert(response.status);
    return new DBSQLSession(driver, definedOrError(response.sessionHandle), this.logger, this.stagingAllowedLocalPath);
  }

  private async getClient() {
    if (!this.connectionOptions || !this.authProvider) {
      throw new HiveDriverError('DBSQLClient: not connected');
    }

    const authHeaders = await this.authProvider.authenticate();
    // When auth headers change - recreate client. Thrift library does not provide API for updating
    // changed options, therefore we have to recreate both connection and client to apply new headers
    if (!this.client || !areHeadersEqual(this.additionalHeaders, authHeaders)) {
      this.logger.log(LogLevel.info, 'DBSQLClient: initializing thrift client');
      this.additionalHeaders = authHeaders;
      const connectionOptions = this.getConnectionOptions(this.connectionOptions, this.additionalHeaders);

      const connection = await this.createConnection(connectionOptions);
      this.client = this.thrift.createClient(TCLIService, connection.getConnection());
    }

    return this.client;
  }

  private async createConnection(options: IConnectionOptions) {
    const connectionProvider = new HttpConnection();
    const connection = await connectionProvider.connect(options);
    const thriftConnection = connection.getConnection();

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

    return connection;
  }

  public async close(): Promise<void> {
    this.client = null;
    this.authProvider = null;
    this.connectionOptions = null;
  }
}
