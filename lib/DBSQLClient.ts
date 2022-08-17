import thrift from 'thrift';

import { EventEmitter } from 'events';
import TCLIService from '../thrift/TCLIService';
import TCLIService_types, { TOpenSessionReq } from '../thrift/TCLIService_types';
import IDBSQLClient, { IDBSQLConnectionOptions } from './contracts/IDBSQLClient';
import HiveDriver from './hive/HiveDriver';
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
import XhrConnection from './connection/connections/XhrConnection';

function isNodejs() { return typeof "process" !== "undefined" && process && process.versions && process.versions.node; }

export default class DBSQLClient extends EventEmitter implements IDBSQLClient {
  private client: TCLIService.Client | null;

  private connection: IThriftConnection | null;

  private statusFactory: StatusFactory;

  private connectionProvider: IConnectionProvider;

  private authProvider: IAuthentication;

  private thrift = thrift;

  constructor() {
    super();
    if(isNodejs()) {
      console.log("Bad");
      this.connectionProvider = new HttpConnection();
    }
    else {
      this.connectionProvider = new XhrConnection();
      console.log("Good");
    }
    this.statusFactory = new StatusFactory();
    this.client = null;
    this.connection = null;
    this.authProvider = new NoSaslAuthentication();
  }

  private getConnectionOptions(options: IDBSQLConnectionOptions): IConnectionOptions {
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

  async connect(options: IDBSQLConnectionOptions): Promise<IDBSQLClient> {
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
  openSession(request?: TOpenSessionReq): Promise<IDBSQLSession> {
    if (!this.connection?.isConnected()) {
      return Promise.reject(new HiveDriverError('DBSQLClient: connection is lost'));
    }

    const driver = new HiveDriver(this.getClient());

    if (!request) {
      request = {
        client_protocol: TCLIService_types.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6,
      };
    }

    return driver.openSession(request).then((response) => {
      this.statusFactory.create(response.status);

      const session = new DBSQLSession(driver, definedOrError(response.sessionHandle));

      return session;
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
