import thrift from 'thrift';
import https from 'https';
import http from 'http';

import IThriftConnection from '../contracts/IThriftConnection';
import IConnectionProvider from '../contracts/IConnectionProvider';
import IConnectionOptions from '../contracts/IConnectionOptions';
import globalConfig from '../../globalConfig';

import AxiosHttpConnection from './AxiosHttpConnection';

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

    this.connection = new AxiosHttpConnection(
      {
        url: `${options.https ? 'https' : 'http'}://${options.host}:${options.port}${options.path ?? '/'}`,
        httpsAgent: new https.Agent(httpsAgentOptions),
        httpAgent: new http.Agent(httpAgentOptions),
        timeout: options.socketTimeout ?? globalConfig.socketTimeout,
        headers: options.headers,
      },
      {
        transport: thrift.TBufferedTransport,
        protocol: thrift.TBinaryProtocol,
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
