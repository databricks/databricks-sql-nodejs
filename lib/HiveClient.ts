const thrift = require('thrift');

import TCLIService from '../thrift/TCLIService';
import { TOpenSessionReq } from '../thrift/TCLIService_types';
import IHiveClient from './contracts/IHiveClient';
import HiveDriver from './hive/HiveDriver';
import HiveSession from './HiveSession';
import IHiveSession from './contracts/IHiveSession';
import IThriftConnection from './connection/contracts/IThriftConnection';
import IConnectionProvider from './connection/contracts/IConnectionProvider';
import IAuthentication from './connection/contracts/IAuthentication';
import NoSaslAuthentication from './connection/auth/NoSaslAuthentication';
import HttpConnection from './connection/connections/HttpConnection';
import IConnectionOptions from './connection/contracts/IConnectionOptions';
import { EventEmitter } from 'events';
import StatusFactory from './factory/StatusFactory';
import HiveDriverError from './errors/HiveDriverError';
import { definedOrError } from './utils';

export default class HiveClient extends EventEmitter implements IHiveClient {
  private client: TCLIService.Client | null;
  private connection: IThriftConnection | null;
  private statusFactory: StatusFactory;
  private connectionProvider: IConnectionProvider;
  private authProvider: IAuthentication;
  private thrift: any;

  constructor() {
    super();
    this.thrift = thrift;
    this.connectionProvider = new HttpConnection();
    this.authProvider = new NoSaslAuthentication();
    this.statusFactory = new StatusFactory();
    this.client = null;
    this.connection = null;
  }

  async connect(
    options: IConnectionOptions,
    connectionProvider?: IConnectionProvider,
    authProvider?: IAuthentication,
  ): Promise<HiveClient> {
    if (connectionProvider) {
      this.connectionProvider = connectionProvider;
    }

    if (authProvider) {
      this.authProvider = authProvider;
    }

    this.connection = await this.connectionProvider.connect(options, this.authProvider);

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
  openSession(request: TOpenSessionReq): Promise<IHiveSession> {
    if (!this.connection?.isConnected()) {
      return Promise.reject(new HiveDriverError('HiveClient: connection is lost'));
    }

    const driver = new HiveDriver(this.getClient());

    return driver.openSession(request).then((response) => {
      this.statusFactory.create(response.status);

      const session = new HiveSession(driver, definedOrError(response.sessionHandle));

      return session;
    });
  }

  getClient() {
    if (!this.client) {
      throw new HiveDriverError('HiveClient: client is not initialized');
    }

    return this.client;
  }

  close(): void {
    if (!this.connection) {
      return;
    }

    const thriftConnection = this.connection.getConnection();

    if (typeof thriftConnection.end === 'function') {
      this.connection.getConnection().end();
    }
  }
}
