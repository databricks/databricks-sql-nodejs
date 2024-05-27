import thrift from 'thrift';
import https from 'https';
import http from 'http';
import { HeadersInit } from 'node-fetch';
import { ProxyAgent } from 'proxy-agent';

import IConnectionProvider, { HttpTransactionDetails } from '../contracts/IConnectionProvider';
import IConnectionOptions, { ProxyOptions } from '../contracts/IConnectionOptions';
import IClientContext from '../../contracts/IClientContext';

import ThriftHttpConnection from './ThriftHttpConnection';
import IRetryPolicy from '../contracts/IRetryPolicy';
import HttpRetryPolicy from './HttpRetryPolicy';

export default class HttpConnection implements IConnectionProvider {
  private readonly options: IConnectionOptions;

  private readonly context: IClientContext;

  private headers: HeadersInit = {};

  private connection?: ThriftHttpConnection;

  private agent?: http.Agent;

  constructor(options: IConnectionOptions, context: IClientContext) {
    this.options = options;
    this.context = context;
  }

  public setHeaders(headers: HeadersInit) {
    this.headers = headers;
    this.connection?.setHeaders({
      ...this.options.headers,
      ...this.headers,
    });
  }

  public async getAgent(): Promise<http.Agent | undefined> {
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
    const clientConfig = this.context.getConfig();

    return {
      keepAlive: true,
      keepAliveMsecs: 10000,
      maxSockets: Infinity, // no limit
      timeout: this.options.socketTimeout ?? clientConfig.socketTimeout,
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

    return new ProxyAgent({
      ...this.getAgentDefaultOptions(),
      getProxyForUrl: () => proxyUrl,
      httpsAgent: this.createHttpsAgent(),
      httpAgent: this.createHttpAgent(),
    });
  }

  public async getThriftConnection(): Promise<any> {
    if (!this.connection) {
      const { options } = this;
      const clientConfig = this.context.getConfig();
      const agent = await this.getAgent();

      this.connection = new ThriftHttpConnection(
        {
          url: `${options.https ? 'https' : 'http'}://${options.host}:${options.port}${options.path ?? '/'}`,
          transport: thrift.TBufferedTransport,
          protocol: thrift.TBinaryProtocol,
          getRetryPolicy: () => this.getRetryPolicy(),
        },
        {
          agent,
          timeout: options.socketTimeout ?? clientConfig.socketTimeout,
          headers: {
            ...options.headers,
            ...this.headers,
          },
        },
      );
    }

    return this.connection;
  }

  public async getRetryPolicy(): Promise<IRetryPolicy<HttpTransactionDetails>> {
    return new HttpRetryPolicy(this.context);
  }
}
