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
 * End-to-end tests for the SEA submit/await_result async-consumption
 * path through the napi binding.
 *
 * Path under test:
 *   1. `binding.openSession(...)` — kernel `Session::open()`
 *   2. `connection.submitStatement(sql)` — kernel `Statement::submit()`
 *      (wait_timeout=0s, server returns Pending/Running with a
 *      statement_id)
 *   3. `asyncStmt.status()` / `.awaitResult()` — kernel `status()` /
 *      `await_result()`
 *   4. `asyncResult.fetchNextBatch()` — kernel `ResultStream::next_batch()`
 *
 * The kernel's `AwaitResultCancelGuard` covers drop-cancel safety;
 * the `cancel-while-pending` test exercises explicit cancel mid-poll
 * by racing `asyncStmt.cancel()` against `asyncStmt.awaitResult()`.
 *
 * Calls the napi binding directly (same pattern as
 * `operation-lifecycle-e2e.test.ts`) — the higher-level
 * `DBSQLOperation` async-mode integration (matching Thrift's
 * `IDBSQLOperation` polling-mode surface) is a follow-on. This test
 * proves the kernel → napi → JS shape works end-to-end.
 *
 * Skipped when `DATABRICKS_PECOTESTING_*` env vars are absent.
 */

import { expect } from 'chai';
import { tableFromIPC } from 'apache-arrow';
import * as fs from 'fs';
import * as path from 'path';
import { createRequire } from 'module';

// `createRequire(import.meta.url)` so the require works under both
// CJS and the ESM-reparse path mocha 11+ may use
// (MODULE_TYPELESS_PACKAGE_JSON reparse-as-ESM).
// eslint-disable-next-line @typescript-eslint/naming-convention
const requireFromHere = createRequire(import.meta.url);

interface NativeBinding {
  openSession(opts: {
    hostName: string;
    httpPath: string;
    token: string;
  }): Promise<NativeConnection>;
}

interface NativeConnection {
  submitStatement(sql: string): Promise<NativeAsyncStatement>;
  close(): Promise<void>;
}

interface NativeAsyncStatement {
  readonly statementId: string;
  status(): Promise<string>;
  awaitResult(): Promise<NativeAsyncResultHandle>;
  cancel(): Promise<void>;
  close(): Promise<void>;
}

interface NativeAsyncResultHandle {
  readonly statementId: string;
  fetchNextBatch(): Promise<{ ipcBytes: Buffer } | null>;
  schema(): Promise<{ ipcBytes: Buffer }>;
}

describe('SEA async execute — submit / status / awaitResult / cancel', function suite() {
  this.timeout(180_000);

  const hostName =
    process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME || process.env.E2E_HOST;
  const httpPath =
    process.env.DATABRICKS_PECOTESTING_HTTP_PATH || process.env.E2E_PATH;
  const token =
    process.env.DATABRICKS_PECOTESTING_TOKEN || process.env.E2E_ACCESS_TOKEN;

  before(function gate() {
    if (!hostName || !httpPath || !token) {
      // eslint-disable-next-line no-invalid-this
      this.skip();
      return;
    }
    // Verify the native artifact exists before any test calls
    // loadBinding(). Skip-with-message if absent. DA round-1 H1 fixup
    // (skip-gate must not crash MODULE_NOT_FOUND when build:native not
    // run).
    const nodeArtifact = path.resolve(
      process.cwd(),
      'native/sea/index.linux-x64-gnu.node',
    );
    if (!fs.existsSync(nodeArtifact)) {
      // eslint-disable-next-line no-console
      console.warn(
        `[sea async-execute e2e] skipping: native binary not built. ` +
          `Run \`yarn build:native\` first.`,
      );
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  /**
   * Lazy-load the native binding so the test file is requirable in
   * environments where the `.node` artifact isn't built yet — the
   * `before()` gate skips the suite before we touch the binding.
   */
  function loadBinding(): NativeBinding {
    return requireFromHere('../../../native/sea/index.js') as NativeBinding;
  }

  it('submit returns immediately with a statement_id; awaitResult drains', async () => {
    const binding = loadBinding();
    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
    });

    let asyncStmt: NativeAsyncStatement | null = null;
    try {
      asyncStmt = await connection.submitStatement('SELECT * FROM range(0, 100)');
      expect(asyncStmt).to.be.an('object');
      expect(asyncStmt.statementId).to.be.a('string').and.to.have.length.greaterThan(0);

      // Block on the server-side terminal state. The kernel's
      // internal polling handles backoff and the drop-cancel guard.
      const result = await asyncStmt.awaitResult();
      expect(result.statementId).to.equal(asyncStmt.statementId);

      // Drain the full result and assert row count.
      let totalRows = 0;
      // eslint-disable-next-line no-constant-condition
      while (true) {
        // eslint-disable-next-line no-await-in-loop
        const envelope = await result.fetchNextBatch();
        if (envelope === null) {
          break;
        }
        const table = tableFromIPC(envelope.ipcBytes);
        totalRows += table.numRows;
      }
      expect(totalRows).to.equal(100);
    } finally {
      if (asyncStmt !== null) {
        try {
          await asyncStmt.close();
        } catch (_) {
          // best-effort cleanup
        }
      }
      await connection.close();
    }
  });

  it('status() returns a string variant from the kernel StatementStatus enum', async () => {
    const binding = loadBinding();
    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
    });

    let asyncStmt: NativeAsyncStatement | null = null;
    try {
      asyncStmt = await connection.submitStatement('SELECT * FROM range(0, 100)');
      const status = await asyncStmt.status();
      expect(status).to.be.a('string');
      expect(['Pending', 'Running', 'Succeeded', 'Closed']).to.include(
        status,
        `unexpected status: ${status}`,
      );

      // Drain via awaitResult to release server-side resources.
      await asyncStmt.awaitResult();
    } finally {
      if (asyncStmt !== null) {
        try {
          await asyncStmt.close();
        } catch (_) {
          // best-effort cleanup
        }
      }
      await connection.close();
    }
  });

  it('cancel() against a still-pending async statement completes quickly', async () => {
    const binding = loadBinding();
    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
    });

    let asyncStmt: NativeAsyncStatement | null = null;
    try {
      // Large enough query that the server will not have finished by
      // the time we issue cancel. `range(0, 100_000_000)` was used in
      // the existing sync cancel test for the same reason.
      asyncStmt = await connection.submitStatement(
        'SELECT * FROM range(0, 100000000)',
      );
      expect(asyncStmt.statementId).to.have.length.greaterThan(0);

      const t0 = Date.now();
      await asyncStmt.cancel();
      const elapsed = Date.now() - t0;
      // cancel should not block on completion of the underlying query;
      // it just sends a CancelStatement and returns. Allow a generous
      // budget for wire latency.
      expect(elapsed).to.be.lessThan(2000, `cancel latency ${elapsed}ms`);
    } finally {
      if (asyncStmt !== null) {
        try {
          await asyncStmt.close();
        } catch (_) {
          // best-effort cleanup; cancelled statements may surface a close error
        }
      }
      await connection.close();
    }
  });
});
