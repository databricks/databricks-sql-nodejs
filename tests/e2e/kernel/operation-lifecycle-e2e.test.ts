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

/**
 * End-to-end tests for the SEA operation lifecycle (cancel / close /
 * finished) wired through `KernelOperationBackend`.
 *
 * The impl-execution feature has not yet wired
 * `DBSQLClient.connect({ useKernel: true })` to dispatch into
 * `KernelBackend`, so this test drives the lifecycle by:
 *   1. Calling the napi `openSession(...)` free function directly to
 *      get a kernel `Connection`.
 *   2. Calling `connection.executeStatement(...)` to get a napi
 *      `Statement` handle.
 *   3. Wrapping that handle in a `KernelOperationBackend` and exercising
 *      its `cancel()` / `close()` / `waitUntilReady()` methods.
 *
 * This mirrors how the eventual `KernelSessionBackend.executeStatement`
 * call path will assemble the operation — we just inline the kernel
 * call here since the session backend is being built in parallel.
 *
 * Path note: the original task spec referenced
 * `tests/integration/kernel/operation-lifecycle-e2e.test.ts`. The
 * existing project structure uses `tests/e2e/**` (with its own
 * `.mocharc.js`), so this file lives under `tests/e2e/kernel/` to be
 * picked up by `npm run e2e` automatically.
 */

import { expect } from 'chai';
import IClientContext from '../../../lib/contracts/IClientContext';
import IDBSQLLogger, { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import { getKernelNative } from '../../../lib/kernel/KernelNativeLoader';
import KernelOperationBackend from '../../../lib/kernel/KernelOperationBackend';
import OperationStateError, { OperationStateErrorCode } from '../../../lib/errors/OperationStateError';

// Minimal binding type shapes (mirrors the napi `index.d.ts`).
interface NativeBinding {
  openSession(opts: { hostName: string; httpPath: string; token: string }): Promise<NativeConnection>;
}

interface NativeConnection {
  executeStatement(
    sql: string,
    options: {
      initialCatalog?: string;
      initialSchema?: string;
      sessionConfig?: Record<string, string>;
    },
  ): Promise<NativeStatement>;
  close(): Promise<void>;
}

interface NativeStatement {
  fetchNextBatch(): Promise<{ ipcBytes: Buffer } | null>;
  // schema() is synchronous on the merged-kernel binding.
  schema(): { ipcBytes: Buffer };
  cancel(): Promise<void>;
  close(): Promise<void>;
}

class NoopLogger implements IDBSQLLogger {
  log(_level: LogLevel, _message: string): void {
    // no-op for e2e runs
  }
}

function makeContext(): IClientContext {
  const logger = new NoopLogger();
  const notUsed = () => {
    throw new Error('IClientContext member not expected in lifecycle e2e');
  };
  return {
    getConfig: notUsed,
    getLogger: () => logger,
    getConnectionProvider: notUsed,
    getClient: notUsed,
    getDriver: notUsed,
  } as unknown as IClientContext;
}

describe('SEA operation lifecycle — end-to-end', function suite() {
  // Live-warehouse tests can take >2s through warm-up; bump the
  // mocha default (2000ms) generously. The base `tests/e2e/.mocharc.js`
  // already sets 300s but we keep this explicit so the file is robust
  // when run via `npx mocha …` outside the e2e harness.
  this.timeout(120_000);

  const hostName = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME || process.env.E2E_HOST;
  const httpPath = process.env.DATABRICKS_PECOTESTING_HTTP_PATH || process.env.E2E_PATH;
  const token = process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL || process.env.E2E_ACCESS_TOKEN;

  before(function gate() {
    // eslint-disable-next-line no-invalid-this
    const self = this;
    if (!hostName || !httpPath || !token) {
      self.skip();
      return;
    }
    // Creds present but the native binding not built/installed (e.g. a CI
    // runner without the .node) must SKIP, not error: probe getKernelNative()
    // here so every test isn't an uncaught-throw at its first call.
    try {
      getKernelNative();
    } catch {
      self.skip();
    }
  });

  it('cancel() succeeds against a live SEA statement', async () => {
    const binding = getKernelNative() as unknown as NativeBinding;

    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
    });

    let statement: NativeStatement | null = null;
    try {
      // Use a query that is long-enough running that cancel actually
      // has work to do. `range(0, 100_000_000)` is large enough that
      // even with kernel-side optimizations the server has not yet
      // produced the full result by the time we cancel.
      statement = await connection.executeStatement('SELECT * FROM range(0, 100000000)', {});
      expect(statement).to.be.an('object');

      const op = new KernelOperationBackend({
        statement: statement as unknown as NativeStatement,
        context: makeContext(),
      });

      // Assert behavior (cancel resolves with success), not wall-clock latency
      // — a hard ms budget against a live warehouse is flaky on slow networks.
      const status = await op.cancel();
      expect(status.isSuccess).to.equal(true);
    } finally {
      // Bypass `op.close()` here because we want to verify cancel
      // alone — close is exercised in the next test.
      if (statement !== null) {
        try {
          await statement.close();
        } catch (_) {
          // Cancelled statements may surface a close error from the
          // server; ignore for cleanup.
        }
      }
      await connection.close();
    }
  });

  it('cancel mid-fetch — subsequent fetchChunk throws OperationStateError', async () => {
    const binding = getKernelNative() as unknown as NativeBinding;

    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
    });

    let statement: NativeStatement | null = null;
    try {
      statement = await connection.executeStatement('SELECT * FROM range(0, 100000000)', {});

      const op = new KernelOperationBackend({
        statement: statement as unknown as NativeStatement,
        context: makeContext(),
      });

      await op.cancel();

      // After cancel, fetchChunk must throw the cancellation error
      // (regardless of whether the underlying fetch implementation
      // is wired — the lifecycle gate runs first).
      let thrown: unknown;
      try {
        await op.fetchChunk({ limit: 100 });
      } catch (err) {
        thrown = err;
      }
      expect(thrown).to.be.instanceOf(OperationStateError);
      expect((thrown as OperationStateError).errorCode).to.equal(OperationStateErrorCode.Canceled);
    } finally {
      if (statement !== null) {
        try {
          await statement.close();
        } catch (_) {
          // ignore cleanup error after cancel
        }
      }
      await connection.close();
    }
  });

  it('close() succeeds against a SEA statement and is idempotent', async () => {
    const binding = getKernelNative() as unknown as NativeBinding;

    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
    });

    try {
      const statement = await connection.executeStatement('SELECT 1', {});

      const op = new KernelOperationBackend({
        statement: statement as unknown as NativeStatement,
        context: makeContext(),
      });

      const status1 = await op.close();
      expect(status1.isSuccess).to.equal(true);

      // Idempotent — a second close is a no-op on the JS side and
      // does not hit the binding (which would already have taken the
      // inner handle).
      const status2 = await op.close();
      expect(status2.isSuccess).to.equal(true);
    } finally {
      await connection.close();
    }
  });

  it('finished() resolves immediately and fires the progress callback', async () => {
    const binding = getKernelNative() as unknown as NativeBinding;

    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
    });

    let statement: NativeStatement | null = null;
    try {
      statement = await connection.executeStatement('SELECT 1', {});

      const op = new KernelOperationBackend({
        statement: statement as unknown as NativeStatement,
        context: makeContext(),
      });

      let ticks = 0;
      await op.waitUntilReady({
        callback: () => {
          ticks += 1;
        },
      });

      // M0 finished() is a no-op that resolves immediately and fires the
      // progress callback exactly once. Assert the behavior, not a wall-clock
      // budget (flaky against a live warehouse).
      expect(ticks).to.equal(1);
    } finally {
      if (statement !== null) {
        await statement.close();
      }
      await connection.close();
    }
  });
});
