import { expect } from 'chai';
import http, { IncomingHttpHeaders } from 'http';
import { AddressInfo } from 'net';
import httpProxy from 'http-proxy';
import { DBSQLClient } from '../../lib';
import { ConnectionOptions } from '../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../lib/contracts/InternalConnectionOptions';

import config from './utils/config';

const customHeaders = {
  'X-Databricks-Spec-Header-A': 'session-017-a',
  'X-Databricks-Spec-Header-B': 'session-017-b',
};

async function selectOne(client: DBSQLClient) {
  const session = await client.openSession({ initialCatalog: config.catalog, initialSchema: config.schema });
  const operation = await session.executeStatement('SELECT 1 AS test_value');
  const rows = (await operation.fetchAll()) as Array<Record<string, unknown>>;
  await operation.close();
  await session.close();
  return rows;
}

describe('Custom HTTP headers', () => {
  it('are sent on every Thrift request', async () => {
    // Local plain-HTTP reverse proxy to the warehouse that records the headers
    // the driver sends.
    const seen: Array<IncomingHttpHeaders> = [];
    const proxy = httpProxy.createProxyServer({ target: `https://${config.host}`, changeOrigin: true });
    const server = http.createServer((req, res) => {
      seen.push(req.headers);
      proxy.web(req, res);
    });
    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', resolve);
    });

    try {
      const client = new DBSQLClient();
      // DBSQLClient always uses https; point the Thrift transport at the local proxy.
      // @ts-expect-error TS2341: Property getConnectionOptions is private
      const originalGetConnectionOptions = client.getConnectionOptions;
      // @ts-expect-error TS2341: Property getConnectionOptions is private
      client.getConnectionOptions = (...args) => ({
        ...originalGetConnectionOptions.apply(client, args),
        host: '127.0.0.1',
        port: (server.address() as AddressInfo).port,
        https: false,
      });

      await client.connect({
        host: config.host,
        path: config.path,
        token: config.token,
        telemetryEnabled: false,
        customHeaders,
      });
      const rows = await selectOne(client);
      await client.close();

      expect(rows).to.have.length(1);
      expect(Number(rows[0].test_value)).to.equal(1);
      // OpenSession, ExecuteStatement, ... CloseSession.
      expect(seen.length).to.be.gte(3);
      for (const headers of seen) {
        expect(headers['x-databricks-spec-header-a']).to.equal('session-017-a');
        expect(headers['x-databricks-spec-header-b']).to.equal('session-017-b');
      }
    } finally {
      proxy.close();
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
    }
  });

  it('do not break a kernel session', async () => {
    const client = new DBSQLClient();
    await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
      useKernel: true,
      customHeaders,
    } as ConnectionOptions & InternalConnectionOptions);
    const rows = await selectOne(client);
    await client.close();

    expect(rows).to.have.length(1);
    expect(Number(rows[0].test_value)).to.equal(1);
  });
});
