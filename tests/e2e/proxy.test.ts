import { expect } from 'chai';
import sinon from 'sinon';
import httpProxy from 'http-proxy';
import { IncomingHttpHeaders, ClientRequest, OutgoingHttpHeaders } from 'http';
import https from 'https';
import { DBSQLClient } from '../../lib';
import { ProxyOptions } from '../../lib/connection/contracts/IConnectionOptions';

import config from './utils/config';

class HttpProxyMock {
  public readonly requests: Array<{
    method: string;
    url: string;
    requestHeaders: OutgoingHttpHeaders;
    responseHeaders: IncomingHttpHeaders;
  }> = [];

  public readonly config: ProxyOptions;

  public readonly target = `https://${config.host}`;

  public readonly proxy: httpProxy;

  constructor(target: string, port: number) {
    this.config = {
      protocol: 'http',
      host: 'localhost',
      port,
    };

    this.proxy = httpProxy.createServer({
      target: this.target,
      agent: new https.Agent({
        rejectUnauthorized: false,
      }),
    });

    this.proxy.on('proxyRes', (proxyRes) => {
      const req = (proxyRes as any).req as ClientRequest;
      this.requests.push({
        method: req.method?.toUpperCase(),
        url: `${req.protocol}//${req.host}${req.path}`,
        requestHeaders: { ...req.getHeaders() },
        responseHeaders: { ...proxyRes.headers },
      });
    });

    this.proxy.listen(port);
    // eslint-disable-next-line no-console
    console.log(`Proxy listening at ${this.config.host}:${this.config.port} -> ${this.target}`);
  }

  close() {
    this.proxy.close(() => {
      // eslint-disable-next-line no-console
      console.log(`Proxy stopped at ${this.config.host}:${this.config.port}`);
    });
  }
}

describe('Proxy', () => {
  it('should use http proxy', async () => {
    const proxy = new HttpProxyMock(`https://${config.host}`, 9090);
    try {
      const client = new DBSQLClient();

      // Our proxy mock is HTTP -> HTTPS, but DBSQLClient is hard-coded to use HTTPS.
      // Here we override default behavior to make DBSQLClient work with HTTP proxy
      // @ts-expect-error TS2341: Property getConnectionOptions is private
      const originalGetConnectionOptions = client.getConnectionOptions;
      // @ts-expect-error TS2341: Property getConnectionOptions is private
      client.getConnectionOptions = (...args) => {
        const result = originalGetConnectionOptions.apply(client, args);
        result.https = false;
        result.port = 80;
        return result;
      };

      const clientConfig = client.getConfig();
      sinon.stub(client, 'getConfig').returns(clientConfig);

      const connection = await client.connect({
        host: config.host,
        path: config.path,
        token: config.token,
        proxy: proxy.config,
      });

      await connection.openSession({
        initialCatalog: config.catalog,
        initialSchema: config.schema,
      });

      expect(proxy.requests.length).to.be.gte(1);
      expect(proxy.requests[0].method).to.be.eq('POST');
      expect(proxy.requests[0].url).to.be.eq(`https://${config.host}${config.path}`);
    } finally {
      proxy.close();
    }
  });
});
