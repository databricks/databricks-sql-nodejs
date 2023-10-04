import thrift from 'thrift';
import https from 'https';
import http from 'http';
import { HeadersInit } from 'node-fetch';
import { ProxyAgent } from 'proxy-agent';

import IConnectionProvider from '../contracts/IConnectionProvider';
import IConnectionOptions, { ProxyOptions } from '../contracts/IConnectionOptions';
import globalConfig from '../../globalConfig';

import ThriftHttpConnection from './ThriftHttpConnection';

export default class HttpConnection implements IConnectionProvider {
  private readonly options: IConnectionOptions;

  private headers: HeadersInit = {};

  private connection?: ThriftHttpConnection;

  private agent?: http.Agent;

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

  public async getAgent(): Promise<http.Agent> {
    if (!this.agent) {
      if (this.options.proxy !== undefined) {
        this.agent = this.createProxyAgent(this.options.proxy);
      } else {
        this.agent = this.options.https ? this.createHttpsAgent() : this.createHttpAgent();
      }
    }

    return this.agent;
  }

  private getAgentDefaultOptions(): http.AgentOptions {
    return {
      keepAlive: true,
      maxSockets: 5,
      keepAliveMsecs: 10000,
      timeout: this.options.socketTimeout ?? globalConfig.socketTimeout,
    };
  }

  private createHttpAgent(): http.Agent {
    const httpAgentOptions = this.getAgentDefaultOptions();
    return new http.Agent(httpAgentOptions);
  }

  private createHttpsAgent(): https.Agent {
    const httpsAgentOptions: https.AgentOptions = {
      ...this.getAgentDefaultOptions(),
      minVersion: 'TLSv1.2',
      rejectUnauthorized: false,
      ca: this.options.ca,
      cert: this.options.cert,
      key: this.options.key,
    };
    return new https.Agent(httpsAgentOptions);
  }

  private createProxyAgent(proxyOptions: ProxyOptions): ProxyAgent {
    const proxyAuth = proxyOptions.auth?.username
      ? `${proxyOptions.auth.username}:${proxyOptions.auth?.password ?? ''}@`
      : '';
    const proxyUrl = `${proxyOptions.protocol}://${proxyAuth}${proxyOptions.host}:${proxyOptions.port}`;

    const proxyProtocol = `${proxyOptions.protocol}:`;

    return new ProxyAgent({
      ...this.getAgentDefaultOptions(),
      getProxyForUrl: () => proxyUrl,
      httpsAgent: this.createHttpsAgent(),
      httpAgent: this.createHttpAgent(),
      protocol: proxyProtocol,
    });
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
