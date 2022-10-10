import thrift from 'thrift';

import { EventEmitter } from 'events';
import TCLIService from '../thrift/TCLIService';
import { TProtocolVersion } from '../thrift/TCLIService_types';
import IDBSQLClient, { ConnectionOptions, OpenSessionRequest } from './contracts/IDBSQLClient';
import HiveDriver from './hive/HiveDriver';
import { Int64 } from './hive/Types';
import DBSQLSession from './DBSQLSession';
import IDBSQLSession from './contracts/IDBSQLSession';
import IThriftConnection from './connection/contracts/IThriftConnection';
import IConnectionProvider from './connection/contracts/IConnectionProvider';
import IAuthentication from './connection/contracts/IAuthentication';
import NoSaslAuthentication from './connection/auth/NoSaslAuthentication';
import HttpConnection from './connection/connections/HttpConnection';
import IConnectionOptions from './connection/contracts/IConnectionOptions';
import StatusFactory from './factory/StatusFactory';
import HiveDriverError from './errors/HiveDriverError';
import { buildUserAgentString, definedOrError } from './utils';
import PlainHttpAuthentication from './connection/auth/PlainHttpAuthentication';
import IDBSQLLogger from './contracts/IDBSQLLogger';
import DBSQLLogger from './DBSQLLogger';

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

  private statusFactory: StatusFactory;

  private connectionProvider: IConnectionProvider;

  private authProvider: IAuthentication;

  private logger: IDBSQLLogger;

  private thrift = thrift;

  constructor(logger?: IDBSQLLogger) {
    super();
    this.connectionProvider = new HttpConnection();
    this.authProvider = new NoSaslAuthentication();
    this.statusFactory = new StatusFactory();
    this.logger = logger || new DBSQLLogger();
    this.client = null;
    this.connection = null;
    this.logger.log('info', 'Created DBSQLClient');
  }

  private getConnectionOptions(options: ConnectionOptions): IConnectionOptions {
    const { host, port, token, clientId, ...otherOptions } = options;
    return {
      host,
      port: port || 443,
      options: {
        https: true,
        ...otherOptions,
      },
    };
  }

  /**
   * Connects DBSQLClient to endpoint
   * @public
   * @param options - host, path, and token are required
   * @returns Session object that can be used to execute statements
   * @example
   * const session = client.connect({host, path, token});
   */
  async connect(options: ConnectionOptions): Promise<IDBSQLClient> {
    this.authProvider = new PlainHttpAuthentication({
      username: 'token',
      password: options.token,
      headers: {
        'User-Agent': buildUserAgentString(options.clientId),
      },
    });

    this.connection = await this.connectionProvider.connect(this.getConnectionOptions(options), this.authProvider);

    this.client = this.thrift.createClient(TCLIService, this.connection.getConnection());

    this.connection.getConnection().on('error', (error: Error) => {
      this.logger.log('error', error.toString());
      this.emit('error', error);
    });

    this.connection.getConnection().on('reconnecting', (params: { delay: number; attempt: number }) => {
      this.logger.log('debug', `Reconnecting, params: ${params.toString()}`);
      this.emit('reconnecting', params);
    });

    this.connection.getConnection().on('close', () => {
      this.logger.log('debug', 'Closing connection.');
      this.emit('close');
    });

    this.connection.getConnection().on('timeout', () => {
      this.logger.log('debug', 'Connection timed out.');
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
  openSession(request: OpenSessionRequest = {}): Promise<IDBSQLSession> {
    if (!this.connection?.isConnected()) {
      return Promise.reject(new HiveDriverError('DBSQLClient: connection is lost'));
    }

    const driver = new HiveDriver(this.getClient());

    return driver
      .openSession({
        client_protocol_i64: new Int64(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6),
        ...getInitialNamespaceOptions(request.initialCatalog, request.initialSchema),
      })
      .then((response) => {
        this.statusFactory.create(response.status);
        return new DBSQLSession(driver, definedOrError(response.sessionHandle), this.logger);
      });
  }

  getClient() {
    if (!this.client) {
      throw new HiveDriverError('DBSQLClient: client is not initialized');
    }

    return this.client;
  }

  close(): Promise<void> {
    if (!this.connection) {
      return Promise.resolve();
    }

    const thriftConnection = this.connection.getConnection();

    if (typeof thriftConnection.end === 'function') {
      this.connection.getConnection().end();
    }

    return Promise.resolve();
  }
}
