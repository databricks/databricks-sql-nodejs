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

  private thrift = thrift;

  constructor() {
    super();
    this.connectionProvider = new HttpConnection();
    this.authProvider = new NoSaslAuthentication();
    this.statusFactory = new StatusFactory();
    this.client = null;
    this.connection = null;
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
      this.emit('error', error);
    });

    this.connection.getConnection().on('reconnecting', (params: { delay: number; attempt: number }) => {
      this.emit('reconnecting', params);
    });

    this.connection.getConnection().on('close', () => {
      this.emit('close');
    });

    this.connection.getConnection().on('timeout', () => {
      this.emit('timeout');
    });

    return this;
  }

  /**
   * Starts new session
   *
   * @param request
   * @throws {StatusError}
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
        return new DBSQLSession(driver, definedOrError(response.sessionHandle));
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
