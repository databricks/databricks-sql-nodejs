import thrift from 'thrift';
import https from 'https';
import http from 'http';

import IThriftConnection from '../contracts/IThriftConnection';
import IConnectionProvider from '../contracts/IConnectionProvider';
import IConnectionOptions, { Options } from '../contracts/IConnectionOptions';
import globalConfig from '../../globalConfig';

import AxiosHttpConnection from './AxiosHttpConnection';

type NodeOptions = {
  ca?: Buffer | string;
  cert?: Buffer | string;
  key?: Buffer | string;
  rejectUnauthorized?: boolean;
};

export default class HttpConnection implements IConnectionProvider, IThriftConnection {
  private thrift = thrift;

  private connection: any;

  async connect(options: IConnectionOptions): Promise<IThriftConnection> {
    const agentOptions: http.AgentOptions = {
      keepAlive: true,
      maxSockets: 5,
      keepAliveMsecs: 10000,
      timeout: options.options?.socketTimeout ?? globalConfig.socketTimeout,
    };

    this.connection = new AxiosHttpConnection(
      {
        // TODO: Proper url construction based on options
        url: `${options.options?.https ? 'https' : 'http'}://${options.host}:${options.port}${options.options?.path}`,
        httpsAgent: new https.Agent({ ...agentOptions, minVersion: 'TLSv1.2' }),
        httpAgent: new http.Agent(agentOptions),
        timeout: options.options?.socketTimeout ?? globalConfig.socketTimeout,
        headers: options.options?.headers,
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
    if (this.connection) {
      return true;
    }
    return false;
  }

  private getNodeOptions(options: Options): object {
    const { ca, cert, key, https: useHttps } = options;
    const nodeOptions: NodeOptions = {};

    if (ca) {
      nodeOptions.ca = ca;
    }
    if (cert) {
      nodeOptions.cert = cert;
    }
    if (key) {
      nodeOptions.key = key;
    }

    if (useHttps) {
      nodeOptions.rejectUnauthorized = false;
    }

    return nodeOptions;
  }
}
