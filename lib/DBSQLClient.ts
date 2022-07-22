import TCLIService from '../thrift/TCLIService';
import TCLIService_types from '../thrift/TCLIService_types';
import HiveClient from './HiveClient';
import HiveUtils from './utils/HiveUtils';
import PlainHttpAuthentication from './connection/auth/PlainHttpAuthentication';
import HttpConnection from './connection/connections/HttpConnection';

import IHiveSession from './contracts/IHiveSession';

import { buildUserAgentString } from './utils';

interface EventEmitter extends NodeJS.EventEmitter {}

interface IConnectionOptions {
  host: string;
  port?: number;
  path: string;
  token: string;
  clientId?: string;
}

/**
 * @see IHiveClient
 */
interface IDBSQLClient {
  connect(options: IConnectionOptions): Promise<IDBSQLClient>;
  openSession(): Promise<IHiveSession>;
  close(): void;
}

function prependSlash(str: string): string {
  if (str.length > 0 && str.charAt(0) !== '/') {
    return `/${str}`;
  }
  return str;
}

export default class DBSQLClient implements IDBSQLClient, EventEmitter {
  static utils = new HiveUtils(TCLIService_types);

  private client: HiveClient = new HiveClient(TCLIService, TCLIService_types);

  connect(options: IConnectionOptions) {
    return this.client
      .connect(
        {
          host: options.host,
          port: options.port || 443,
          options: {
            path: prependSlash(options.path),
            https: true,
          },
        },
        new HttpConnection(),
        new PlainHttpAuthentication({
          username: 'token',
          password: options.token,
          headers: {
            'User-Agent': buildUserAgentString(options.clientId),
          },
        }),
      )
      .then(() => this);
  }

  openSession() {
    return this.client.openSession({
      client_protocol: TCLIService_types.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6,
    });
  }

  close() {
    this.client.close();
  }

  // EventEmitter
  addListener(event: string | symbol, listener: (...args: any[]) => void) {
    this.client.addListener(event, listener);
    return this;
  }
  on(event: string | symbol, listener: (...args: any[]) => void) {
    this.client.on(event, listener);
    return this;
  }
  once(event: string | symbol, listener: (...args: any[]) => void) {
    this.client.once(event, listener);
    return this;
  }
  removeListener(event: string | symbol, listener: (...args: any[]) => void) {
    this.client.removeListener(event, listener);
    return this;
  }
  off(event: string | symbol, listener: (...args: any[]) => void) {
    this.client.off(event, listener);
    return this;
  }
  removeAllListeners(event?: string | symbol) {
    this.client.removeAllListeners(event);
    return this;
  }
  setMaxListeners(n: number) {
    this.client.setMaxListeners(n);
    return this;
  }
  getMaxListeners() {
    return this.client.getMaxListeners();
  }
  listeners(event: string | symbol) {
    return this.client.listeners(event);
  }
  rawListeners(event: string | symbol) {
    return this.client.rawListeners(event);
  }
  emit(event: string | symbol, ...args: any[]) {
    return this.client.emit(event, ...args);
  }
  listenerCount(type: string | symbol) {
    return this.client.listenerCount(type);
  }
  prependListener(event: string | symbol, listener: (...args: any[]) => void) {
    this.client.prependListener(event, listener);
    return this;
  }
  prependOnceListener(event: string | symbol, listener: (...args: any[]) => void) {
    this.client.prependOnceListener(event, listener);
    return this;
  }
  eventNames() {
    return this.client.eventNames();
  }
}
