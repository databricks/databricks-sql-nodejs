import thrift from 'thrift';
import https from 'https';

import IThriftConnection from '../contracts/IThriftConnection';
import IConnectionProvider from '../contracts/IConnectionProvider';
import IConnectionOptions, { Options } from '../contracts/IConnectionOptions';
import IAuthentication from '../contracts/IAuthentication';
import XhrTransport from '../transports/XhrTransport';

export default class XhrConnection implements IConnectionProvider, IThriftConnection {
    private thrift = thrift;
    private connection: any;
  
    connect(options: IConnectionOptions, authProvider: IAuthentication): Promise<IThriftConnection> {
        const xhrTransport = new XhrTransport({
            transport: thrift.TBufferedTransport,
            protocol: thrift.TJSONProtocol,
            headers: {
              'Content-Type': 'application/vnd.apache.thrift.json',
             },
            ...options.options,
        });
        return authProvider.authenticate(xhrTransport).then(() => {
            this.connection = this.thrift.createXHRConnection(options.host, options.port, xhrTransport.getOptions());
            return this;
        });
    }

    isConnected(): boolean {
        if (this.connection) {
          return true;
        } else {
          return false;
        }
      }

    getConnection() {
        return this.connection;
    }
}