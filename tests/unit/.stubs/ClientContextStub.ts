import { HeadersInit } from 'node-fetch';
import IClientContext, { ClientConfig } from '../../../lib/contracts/IClientContext';
import IConnectionProvider from '../../../lib/connection/contracts/IConnectionProvider';
import IDriver from '../../../lib/contracts/IDriver';
import IThriftClient from '../../../lib/contracts/IThriftClient';
import IDBSQLLogger from '../../../lib/contracts/IDBSQLLogger';
import DBSQLClient from '../../../lib/DBSQLClient';

import LoggerStub from './LoggerStub';
import ThriftClientStub from './ThriftClientStub';
import DriverStub from './DriverStub';
import ConnectionProviderStub from './ConnectionProviderStub';

export default class ClientContextStub implements IClientContext {
  public configOverrides: Partial<ClientConfig>;

  public logger = new LoggerStub();

  public thriftClient = new ThriftClientStub();

  public driver = new DriverStub();

  public connectionProvider = new ConnectionProviderStub();

  constructor(configOverrides: Partial<ClientConfig> = {}) {
    this.configOverrides = configOverrides;
  }

  public getConfig(): ClientConfig {
    const defaultConfig = DBSQLClient['getDefaultConfig']();
    return {
      ...defaultConfig,
      ...this.configOverrides,
    };
  }

  public getLogger(): IDBSQLLogger {
    return this.logger;
  }

  public async getConnectionProvider(): Promise<IConnectionProvider> {
    return this.connectionProvider;
  }

  public async getClient(): Promise<IThriftClient> {
    return this.thriftClient;
  }

  public async getDriver(): Promise<IDriver> {
    return this.driver;
  }

  public async getAuthHeaders(): Promise<HeadersInit> {
    return {};
  }
}
