import { expect } from 'chai';
import { createServer } from 'node:http';
import { AddressInfo } from 'node:net';
import { tryGetKernelNative } from '../../../lib/kernel/KernelNativeLoader';
import DBSQLClient from '../../../lib/DBSQLClient';
import DBSQLSession from '../../../lib/DBSQLSession';
import KernelSessionBackend from '../../../lib/kernel/KernelSessionBackend';

describe('native staging close', () => {
  const native = tryGetKernelNative();
  if (native === undefined) {
    it.skip('kernel native binding not available on this platform');
    return;
  }

  it('closes a session during a pending staged statement with default options', async function () {
    this.timeout(10000);
    let polled!: () => void;
    const polling = new Promise<void>((resolve) => {
      polled = resolve;
    });
    const server = createServer((request, response) => {
      response.setHeader('content-type', 'application/json');
      if (request.method === 'POST' && request.url?.endsWith('/sessions')) {
        response.end(JSON.stringify({ session_id: 'session', warehouse_id: 'wh' }));
      } else if (request.url?.startsWith('/api/2.0/sql/statements')) {
        response.end(JSON.stringify({ statement_id: 'pending', status: { state: 'RUNNING' } }));
        if (request.method === 'GET') polled();
      } else {
        response.end('{}');
      }
    });
    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', resolve);
    });
    const context = new DBSQLClient({ logger: { log() {} } });
    const connection = await native.openSession({
      hostName: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
      httpPath: '/sql/1.0/warehouses/wh',
      token: 'test-token',
      telemetryEnabled: false,
    });
    const session = new DBSQLSession({
      backend: new KernelSessionBackend({ connection, context, id: connection.sessionId }),
      context,
    });
    try {
      const executing = session.executeStatement('REMOVE remote', { stagingAllowedLocalPath: [] }).then(
        () => {
          throw new Error('pending statement completed');
        },
        (error) => error,
      );
      await polling;
      const started = Date.now();
      await session.close();
      expect(Date.now() - started).to.be.lessThan(1000);
      expect(await executing).to.be.instanceOf(Error);
    } finally {
      await session.close();
      server.closeAllConnections();
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
    }
  });
});
