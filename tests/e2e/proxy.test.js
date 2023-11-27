const { expect } = require('chai');
const sinon = require('sinon');
const httpProxy = require('http-proxy');
const https = require('https');
const config = require('./utils/config');
const { DBSQLClient } = require('../..');

class HttpProxyMock {
  constructor(target, port) {
    this.requests = [];

    this.config = {
      protocol: 'http',
      host: 'localhost',
      port,
    };

    this.target = `https://${config.host}`;

    this.proxy = httpProxy.createServer({
      target: this.target,
      agent: new https.Agent({
        rejectUnauthorized: false,
      }),
    });

    this.proxy.on('proxyRes', (proxyRes) => {
      const req = proxyRes.req;
      this.requests.push({
        method: req.method?.toUpperCase(),
        url: `${req.protocol}//${req.host}${req.path}`,
        requestHeaders: { ...req.getHeaders() },
        responseHeaders: proxyRes.headers,
      });
    });

    this.proxy.listen(port);
    console.log(`Proxy listening at ${this.config.host}:${this.config.port} -> ${this.target}`);
  }

  close() {
    this.proxy.close(() => {
      console.log(`Proxy stopped at ${this.config.host}:${this.config.port}`);
    });
  }
}

describe('Proxy', () => {
  it('should use http proxy', async () => {
    const proxy = new HttpProxyMock(`https://${config.host}`, 9090);
    try {
      const client = new DBSQLClient();
      const clientConfig = client.getConfig();
      sinon.stub(client, 'getConfig').returns(clientConfig);

      const connection = await client.connect({
        host: config.host,
        path: config.path,
        token: config.token,
        proxy: proxy.config,
      });

      const session = await connection.openSession({
        initialCatalog: config.database[0],
        initialSchema: config.database[1],
      });

      expect(proxy.requests.length).to.be.gte(1);
      expect(proxy.requests[0].method).to.be.eq('POST');
      expect(proxy.requests[0].url).to.be.eq(`https://${config.host}${config.path}`);
    } finally {
      proxy.close();
    }
  });
});
