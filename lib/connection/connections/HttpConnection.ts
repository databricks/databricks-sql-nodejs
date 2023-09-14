import thrift from 'thrift';
import https from 'https';
import http from 'http';

import { ProxyAgent } from 'proxy-agent';

import IThriftConnection from '../contracts/IThriftConnection';
import IConnectionProvider from '../contracts/IConnectionProvider';
import IConnectionOptions, { ProxyOptions } from '../contracts/IConnectionOptions';
import globalConfig from '../../globalConfig';

import ThriftHttpConnection from './ThriftHttpConnection';

function buildProxyUrl(options: ProxyOptions): string {
  const auth = options.auth?.username ? `${options.auth.username}:${options.auth?.password ?? ''}@` : '';
  return `${options.protocol}://${auth}${options.host}:${options.port}`;
}

export default class HttpConnection implements IConnectionProvider, IThriftConnection {
  private connection: any;

  async connect(options: IConnectionOptions): Promise<IThriftConnection> {
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

    let agent: http.Agent | undefined;

    if (options.proxy !== undefined) {
      const proxyUrl = buildProxyUrl(options.proxy);
      const proxyProtocol = `${options.proxy.protocol}:`;

      agent = new ProxyAgent({
        ...httpAgentOptions,
        getProxyForUrl: () => proxyUrl,
        httpsAgent: new https.Agent(httpsAgentOptions),
        httpAgent: new http.Agent(httpAgentOptions),
        protocol: proxyProtocol,
      });
    } else {
      agent = options.https ? new https.Agent(httpsAgentOptions) : new http.Agent(httpAgentOptions);
    }

    this.connection = new ThriftHttpConnection(
      {
        url: `${options.https ? 'https' : 'http'}://${options.host}:${options.port}${options.path ?? '/'}`,
        transport: thrift.TBufferedTransport,
        protocol: thrift.TBinaryProtocol,
      },
      {
        agent,
        timeout: options.socketTimeout ?? globalConfig.socketTimeout,
        headers: options.headers,
      },
    );

    return this;
  }

  getConnection() {
    return this.connection;
  }

  isConnected(): boolean {
    return !!this.connection;
  }
}
