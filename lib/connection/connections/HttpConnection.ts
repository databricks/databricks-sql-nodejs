import thrift from 'thrift';
import https from 'https';
import http from 'http';
import { HeadersInit } from 'node-fetch';

import IConnectionProvider from '../contracts/IConnectionProvider';
import IConnectionOptions from '../contracts/IConnectionOptions';
import globalConfig from '../../globalConfig';

import ThriftHttpConnection from './ThriftHttpConnection';

export default class HttpConnection implements IConnectionProvider {
  private readonly options: IConnectionOptions;

  private headers: HeadersInit = {};

  private connection?: ThriftHttpConnection;

  constructor(options: IConnectionOptions) {
    this.options = options;
  }

  public setHeaders(headers: HeadersInit) {
    this.headers = headers;
    this.connection?.setHeaders({
      ...this.options.headers,
      ...this.headers,
    });
  }

  private async getAgent(): Promise<http.Agent> {
    const { options } = this;

    const httpAgentOptions: http.AgentOptions = {
      keepAlive: true,
      maxSockets: 5,
      keepAliveMsecs: 10000,
      timeout: options.socketTimeout ?? globalConfig.socketTimeout,
    };

    const httpsAgentOptions: https.AgentOptions = {
      ...httpAgentOptions,
      minVersion: 'TLSv1.2',
      rejectUnauthorized: false,
      ca: options.ca,
      cert: options.cert,
      key: options.key,
    };

    return options.https ? new https.Agent(httpsAgentOptions) : new http.Agent(httpAgentOptions);
  }

  public async getThriftConnection(): Promise<any> {
    if (!this.connection) {
      const { options } = this;
      const agent = await this.getAgent();

      this.connection = new ThriftHttpConnection(
        {
          url: `${options.https ? 'https' : 'http'}://${options.host}:${options.port}${options.path ?? '/'}`,
          transport: thrift.TBufferedTransport,
          protocol: thrift.TBinaryProtocol,
        },
        {
          agent,
          timeout: options.socketTimeout ?? globalConfig.socketTimeout,
          headers: {
            ...options.headers,
            ...this.headers,
          },
        },
      );
    }

    return this.connection;
  }
}
