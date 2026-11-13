import { expect } from 'chai';
import http from 'http';
import { AddressInfo, Socket } from 'net';
import httpProxy from 'http-proxy';
import { DBSQLClient } from '../../lib';
import { ConnectionOptions } from '../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../lib/contracts/InternalConnectionOptions';

import config from './utils/config';

// Application --HTTP--> local reverse proxy --HTTPS--> Databricks
describe('Plain HTTP transport through a local reverse proxy', () => {
  const requests: Array<{ method?: string; url?: string }> = [];
  const sockets = new Set<Socket>();
  let server: http.Server;
  let port: number;

  before(async () => {
    const proxy = httpProxy.createProxyServer({ target: `https://${config.host}`, changeOrigin: true, secure: true });
    server = http.createServer((req, res) => {
      requests.push({ method: req.method, url: req.url });
      proxy.web(req, res, {}, (error) => {
        res.writeHead(502);
        res.end(String(error));
      });
    });
    server.on('connection', (socket: Socket) => {
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
    });
    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', resolve);
    });
    port = (server.address() as AddressInfo).port;
  });

  after(async () => {
    const closed = new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
    sockets.forEach((socket) => socket.destroy());
    await closed;
  });

  beforeEach(() => {
    requests.length = 0;
  });

  [false, true].forEach((useKernel) => {
    it(`runs a query with https: false (${useKernel ? 'kernel' : 'thrift'} backend)`, async () => {
      const client = new DBSQLClient();
      await client.connect({
        host: '127.0.0.1',
        port,
        path: config.path,
        token: config.token,
        https: false,
        useKernel,
      } as ConnectionOptions & InternalConnectionOptions);

      const session = await client.openSession({ initialCatalog: config.catalog, initialSchema: config.schema });
      const operation = await session.executeStatement('SELECT 1 AS one');
      const rows = (await operation.fetchAll()) as Array<Record<string, unknown>>;
      await operation.close();
      await session.close();
      await client.close();

      expect(rows).to.have.length(1);
      expect(Number(rows[0].one)).to.equal(1);
      // The SQL traffic went through the plain-HTTP proxy: Thrift to the
      // warehouse path, the kernel to the SQL Statement Execution API.
      if (useKernel) {
        expect(requests.some((request) => request.url?.startsWith('/api/2.0/sql/'))).to.be.true;
      } else {
        expect(requests.length).to.be.gte(1);
        const path = config.path.startsWith('/') ? config.path : `/${config.path}`;
        expect(requests.every((request) => request.url === path)).to.be.true;
      }
    });
  });
});
