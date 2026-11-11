import { createHash } from 'crypto';
import { createServer } from 'http';
import { AddressInfo, connect, Socket } from 'net';
import { expect } from 'chai';
import { DBSQLClient } from '../../../lib';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../../lib/contracts/InternalConnectionOptions';
import config from '../utils/config';

describe('kernel CloudFetch configuration', function suite() {
  this.timeout(120_000);

  for (const useCloudFetch of [false, true]) {
    it(
      useCloudFetch ? 'requires storage access when enabled' : 'returns complete large results when disabled',
      async () => {
        let workspaceConnections = 0;
        let blockedConnections = 0;
        const sockets = new Set<Socket>();
        const proxy = createServer();
        proxy.on('connection', (socket) => {
          sockets.add(socket);
          socket.on('close', () => sockets.delete(socket));
        });
        proxy.on('connect', (request, socket, head) => {
          if (request.url !== `${config.host}:443`) {
            blockedConnections += 1;
            socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
            return;
          }
          const upstream = connect(443, config.host, () => {
            workspaceConnections += 1;
            socket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
            upstream.write(head);
            upstream.pipe(socket);
            socket.pipe(upstream);
          });
          socket.on('close', () => upstream.destroy());
          socket.on('error', () => upstream.destroy());
          upstream.on('error', () => socket.destroy());
        });
        await new Promise<void>((resolve) => {
          proxy.listen(0, '127.0.0.1', resolve);
        });
        const { port } = proxy.address() as AddressInfo;
        const client = new DBSQLClient({ logger: { log() {} } });
        client.getConfig().useCloudFetch = useCloudFetch;
        try {
          await client.connect({
            host: config.host,
            path: config.path,
            token: config.token,
            useKernel: true,
            proxy: { protocol: 'http', host: '127.0.0.1', port },
          } as ConnectionOptions & InternalConnectionOptions);
          const session = await client.openSession();
          try {
            let failure: unknown;
            try {
              const operation = await session.executeStatement(
                'SELECT id, sha2(CAST(id AS STRING), 512) AS payload FROM range(250000) ORDER BY id',
              );
              try {
                const rows = (await operation.fetchAll()) as Array<{ id: number | bigint; payload: string }>;
                const digest = createHash('sha256');
                let payloadBytes = 0;
                rows.forEach((row, index) => {
                  expect(Number(row.id)).to.equal(index);
                  expect(row.payload).to.equal(createHash('sha512').update(String(index)).digest('hex'));
                  digest.update(`${index}\t${row.payload}\n`);
                  payloadBytes += row.payload.length;
                });
                expect(rows.length).to.equal(250000);
                expect(payloadBytes).to.equal(32000000);
                expect(digest.digest('hex')).to.equal(
                  'a25ada4c6c476b53e97eb6e8a945e9ce4c14cc432c7ad672dc6fb607d660c86a',
                );
              } finally {
                await operation.close();
              }
            } catch (error) {
              failure = error;
            }
            expect(workspaceConnections).to.be.greaterThan(0);
            if (useCloudFetch) {
              expect(failure).to.not.equal(undefined);
              expect(blockedConnections).to.be.greaterThan(0);
            } else {
              if (failure) throw failure;
              expect(blockedConnections).to.equal(0);
            }
          } finally {
            await session.close();
          }
        } finally {
          await client.close();
          sockets.forEach((socket) => socket.destroy());
          await new Promise<void>((resolve, reject) => {
            proxy.close((error) => (error ? reject(error) : resolve()));
          });
        }
      },
    );
  }
});
