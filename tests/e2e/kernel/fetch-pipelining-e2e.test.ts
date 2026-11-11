// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { expect } from 'chai';
import http from 'http';
import net from 'net';
import { Duplex } from 'stream';
import { DBSQLClient } from '../../../lib';
import { InternalConnectionOptions } from '../../../lib/contracts/InternalConnectionOptions';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import IDBSQLSession from '../../../lib/contracts/IDBSQLSession';
import IOperation from '../../../lib/contracts/IOperation';
import { getKernelNative, KernelNativeAsyncResultHandle } from '../../../lib/kernel/KernelNativeLoader';

async function within<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Timed out after ${ms} ms`)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

// Tunnel TLS unchanged; pause storage responses while warehouse RPCs continue.
async function cloudFetchGate(workspaceHost: string) {
  const sockets = new Set<Duplex>();
  const storage = new Set<net.Socket>();
  let paused = false;
  const server = http.createServer();
  server.on('connect', (request, downstream, head) => {
    const target = new URL(`https://${request.url}`);
    const upstream = net.connect(Number(target.port || 443), target.hostname);
    const isStorage = target.hostname !== workspaceHost;
    for (const socket of [downstream, upstream]) {
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
      socket.on('error', () => {
        downstream.destroy();
        upstream.destroy();
      });
    }
    if (isStorage) {
      storage.add(upstream);
      upstream.on('close', () => storage.delete(upstream));
    }
    upstream.on('connect', () => {
      downstream.write('HTTP/1.1 200 Connection Established\r\n\r\n');
      upstream.write(head);
      downstream.pipe(upstream);
      upstream.pipe(downstream);
      if (isStorage && paused) upstream.pause();
    });
  });
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });
  return {
    port: (server.address() as net.AddressInfo).port,
    pause() {
      expect(storage.size, 'expected CloudFetch storage tunnels').to.be.greaterThan(0);
      paused = true;
      storage.forEach((socket) => socket.pause());
    },
    resume() {
      paused = false;
      storage.forEach((socket) => socket.resume());
    },
    close() {
      sockets.forEach((socket) => socket.destroy());
      return new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
    },
  };
}

describe('kernel fetch pipelining — live CloudFetch lifecycle', function suite() {
  this.timeout(180_000);
  const host = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME || process.env.E2E_HOST;
  const path = process.env.DATABRICKS_PECOTESTING_HTTP_PATH || process.env.E2E_PATH;
  const token = process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL || process.env.E2E_ACCESS_TOKEN;

  before(function gate() {
    if (!host || !path || !token) {
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  for (const method of ['close', 'cancel'] as const) {
    it(`${method} returns while a native CloudFetch prefetch remains blocked`, async () => {
      const workspaceHost = new URL(`https://${host?.replace(/^https?:\/\//, '')}`).hostname;
      const { prototype } = getKernelNative().AsyncResultHandle;
      const proxy = await cloudFetchGate(workspaceHost);
      const originalFetch = prototype.fetchNextBatch;
      const pending = new Set<ReturnType<typeof originalFetch>>();
      let fetchCalls = 0;
      // Observe real native futures; do not model or replace their locks.
      prototype.fetchNextBatch = function trackFetch(this: KernelNativeAsyncResultHandle) {
        fetchCalls += 1;
        const promise = originalFetch.call(this);
        pending.add(promise);
        promise.then(
          () => pending.delete(promise),
          () => pending.delete(promise),
        );
        return promise;
      };
      let cloudFetch = false;
      const client = new DBSQLClient({
        logger: {
          getLevel: () => LogLevel.info,
          log(_level, message) {
            if (message.includes('Using CloudFetch reader:')) cloudFetch = true;
          },
        },
      });
      let session: IDBSQLSession | undefined;
      let operation: IOperation | undefined;
      let consuming: Promise<Array<object>> | undefined;
      try {
        await client.connect({
          host: workspaceHost,
          path: path!,
          token: token!,
          proxy: { protocol: 'http', host: '127.0.0.1', port: proxy.port },
          useKernel: true,
          disableRowMaterialization: true,
          telemetryEnabled: false,
        } as Parameters<DBSQLClient['connect']>[0] & InternalConnectionOptions);
        session = await client.openSession({ configuration: { use_cached_result: 'false' } });
        // Exceed the kernel's downloaded-chunk buffer without depending on a table.
        operation = await session.executeStatement(
          'SELECT id, sha2(CAST(id AS STRING), 512) AS payload FROM range(0, 5000000)',
          { runAsync: true, statementConf: { use_cached_result: 'false' } },
        );
        await operation.finished();
        expect(await operation.fetchChunk({ maxRows: 1 })).to.have.length(1);
        proxy.pause();

        // Drain buffered chunks until the native fetch really waits on storage.
        let blocked = false;
        while (!blocked) {
          consuming = operation.fetchChunk({ maxRows: 65536 });
          // eslint-disable-next-line no-await-in-loop
          const rows = await Promise.race([
            consuming,
            new Promise<undefined>((resolve) => {
              setTimeout(resolve, 100);
            }),
          ]);
          if (rows === undefined) blocked = pending.size > 0;
          else expect(rows.length, 'query exhausted before exercising a blocked prefetch').to.be.greaterThan(0);
          if (!blocked) {
            // eslint-disable-next-line no-await-in-loop
            await consuming;
          }
        }
        expect(fetchCalls).to.be.greaterThan(2);
        const blockedFetch = [...pending][0];
        const status = await within(operation[method](), 5_000);
        expect(status.isSuccess).to.be.true;
        expect(pending.has(blockedFetch), 'lifecycle call waited for the native fetch').to.be.true;
        // CloudFetch logging is delivered asynchronously through napi.
        expect(cloudFetch).to.be.true;
      } finally {
        proxy.resume();
        try {
          await within(Promise.allSettled([...(consuming ? [consuming] : []), ...pending]), 30_000);
          await operation?.close();
          await session?.close();
          await client.close();
        } finally {
          prototype.fetchNextBatch = originalFetch;
          await proxy.close();
        }
      }
    });
  }
});
