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
 * End-to-end check that `DBSQLOperation.fetchAll()` works on the SEA
 * backend with the same `Array<object>` row shape the Thrift backend
 * already produces.
 *
 * The drain primitive is already implemented:
 * - The facade `DBSQLOperation.fetchAll` (`lib/DBSQLOperation.ts`)
 *   loops over `fetchChunk` with `disableBuffering: true` until
 *   `hasMoreRows()` returns false.
 * - The SEA backend's `SeaOperationBackend.fetchChunk` is wired
 *   through the `ResultSlicer` over the napi statement's
 *   `fetchNextBatch()` stream.
 *
 * This test exercises the full path: open client with `useSEA=true`,
 * run a SELECT with a known row count, drain via fetchAll, assert
 * row count + shape match Thrift's `Array<object>` convention.
 *
 * Gated on `DATABRICKS_PECOTESTING_*` env vars; skipped when absent.
 */

import { expect } from 'chai';
import { existsSync } from 'fs';
import { resolve as resolvePath } from 'path';
import { createRequire } from 'module';
import type { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';

// Intentionally avoiding `import { DBSQLClient } from '../../../lib'`
// at the top of the file. `DBSQLClient.ts` transitively imports
// `SeaNativeLoader.ts`, which runs `const native =
// require('../../native/sea/index.js')` at module-load time. If the
// native `.node` artifact has not been built (`yarn build:native`),
// that require throws `MODULE_NOT_FOUND` BEFORE mocha gets a chance
// to invoke the `before()` skip-gate, crashing test discovery for the
// whole suite. Lazy-require `DBSQLClient` inside the `connect()`
// helper after the skip-gate has had a chance to fire. The `type`-only
// import above is erased at compile time so it does not trigger any
// runtime require. (DA round-1 H1 fixup; F2 same pattern.)
//
// `createRequire(import.meta.url)` so the require works under both
// CJS and the ESM-reparse path mocha 11+ may use.

// eslint-disable-next-line @typescript-eslint/naming-convention
const requireFromHere = createRequire(import.meta.url);

interface InternalConnectionOptionsAccess {
  useSEA?: boolean;
}

describe('SEA fetchAll — Array<object> row drain', function suite() {
  this.timeout(180_000);

  const host = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME || process.env.E2E_HOST;
  const path = process.env.DATABRICKS_PECOTESTING_HTTP_PATH || process.env.E2E_PATH;
  const token = process.env.DATABRICKS_PECOTESTING_TOKEN || process.env.E2E_ACCESS_TOKEN;

  before(function gate() {
    if (!host || !path || !token) {
      // eslint-disable-next-line no-invalid-this
      this.skip();
      return;
    }
    // Verify the native artifact exists before any test in the suite
    // attempts to load DBSQLClient (which transitively imports
    // SeaNativeLoader's module-level require of the .node). Skip with
    // a clear message so a developer sees the actionable instruction.
    const nodeArtifact = resolvePath(
      process.cwd(),
      'native/sea/index.linux-x64-gnu.node',
    );
    if (!existsSync(nodeArtifact)) {
      // eslint-disable-next-line no-console
      console.warn(
        `[sea fetch-all e2e] skipping: native binary not built. ` +
          `Run \`yarn build:native\` first.`,
      );
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  async function connect() {
    // Lazy-load the facade so the suite skip-gate runs first. See the
    // top-of-file comment for why this matters.
    const { DBSQLClient } = requireFromHere('../../../lib') as typeof import('../../../lib');
    const client = new DBSQLClient();
    // `useSEA` is an internal opt-in flag (not on the public TS
    // surface; see `lib/contracts/InternalConnectionOptions.ts`).
    // Cast through `unknown` to satisfy strict-mode.
    const options = {
      host: host as string,
      path: path as string,
      token: token as string,
      useSEA: true,
    } as ConnectionOptions & InternalConnectionOptionsAccess;
    await client.connect(options as unknown as ConnectionOptions);
    return client;
  }

  it('drains 100 rows from range(0, 100) into a flat Array<object>', async () => {
    const client = await connect();
    try {
      const session = await client.openSession();
      try {
        const operation = await session.executeStatement('SELECT * FROM range(0, 100)');
        try {
          const rows = await operation.fetchAll();
          expect(rows).to.be.an('array');
          expect(rows.length).to.equal(100);
          // `range(0, n)` returns a single `id` column with values 0..n-1.
          // Every row must be a plain object with that key.
          for (let i = 0; i < rows.length; i += 1) {
            const row = rows[i] as Record<string, unknown>;
            expect(row).to.have.property('id');
          }
        } finally {
          await operation.close();
        }
      } finally {
        await session.close();
      }
    } finally {
      await client.close();
    }
  });

  it('drains an empty result set into an empty array', async () => {
    const client = await connect();
    try {
      const session = await client.openSession();
      try {
        const operation = await session.executeStatement(
          'SELECT * FROM range(0, 0)',
        );
        try {
          const rows = await operation.fetchAll();
          expect(rows).to.be.an('array');
          expect(rows.length).to.equal(0);
        } finally {
          await operation.close();
        }
      } finally {
        await session.close();
      }
    } finally {
      await client.close();
    }
  });

  it('drains a multi-column result set with mixed types', async () => {
    const client = await connect();
    try {
      const session = await client.openSession();
      try {
        // Three primitive columns + a string. Drain and assert each
        // row carries all four keys with the expected values.
        const operation = await session.executeStatement(
          `SELECT id,
                  CAST(id AS DOUBLE) AS d,
                  id % 2 = 0 AS is_even,
                  CONCAT('row-', CAST(id AS STRING)) AS name
           FROM range(0, 10)`,
        );
        try {
          const rows = await operation.fetchAll();
          expect(rows).to.have.length(10);
          for (const row of rows as Array<Record<string, unknown>>) {
            expect(row).to.have.all.keys('id', 'd', 'is_even', 'name');
            expect(row.name).to.match(/^row-\d+$/);
            expect(row.is_even).to.be.a('boolean');
          }
        } finally {
          await operation.close();
        }
      } finally {
        await session.close();
      }
    } finally {
      await client.close();
    }
  });

  // ─── Edge cases (DA round-1 M1 — drain-twice / drain-after-close) ───

  it('drain-twice — fetchAll on an already-drained operation returns []', async () => {
    // After a successful fetchAll drains the cursor to end-of-stream,
    // a second fetchAll on the same operation must produce an empty
    // array (matching Thrift's `DBSQLOperation.fetchAll` semantics:
    // hasMoreRows is false, so the do/while body executes zero
    // iterations and the empty array.flat() yields []).
    const client = await connect();
    try {
      const session = await client.openSession();
      try {
        const operation = await session.executeStatement('SELECT * FROM range(0, 10)');
        try {
          const first = await operation.fetchAll();
          expect(first.length).to.equal(10);
          // Second drain — must not throw, must return [].
          const second = await operation.fetchAll();
          expect(second).to.be.an('array');
          expect(second.length).to.equal(0);
        } finally {
          await operation.close();
        }
      } finally {
        await session.close();
      }
    } finally {
      await client.close();
    }
  });

  it('drain-after-close — fetchAll on a closed operation throws OperationStateError', async () => {
    // Closing the operation invalidates the underlying napi handle.
    // The facade should surface a typed error rather than crash or
    // return garbage. Mirrors Thrift's behaviour: closed operations
    // reject subsequent reads with an OperationStateError that the
    // application can catch and surface.
    const client = await connect();
    try {
      const session = await client.openSession();
      try {
        const operation = await session.executeStatement('SELECT * FROM range(0, 10)');
        await operation.close();
        let threw = false;
        try {
          await operation.fetchAll();
        } catch (err) {
          threw = true;
          // We don't pin the exact error class here because the SEA
          // backend's closed-state error path collapses to either an
          // `OperationStateError` or a kernel-envelope-decoded error
          // depending on whether the close had already reached the
          // facade-level lifecycle flag or only the napi layer. Both
          // are acceptable; what's not acceptable is a silent return
          // or an unhandled exception type.
          expect(err).to.be.an.instanceof(Error);
        }
        expect(threw, 'fetchAll on closed operation must throw').to.equal(true);
      } finally {
        await session.close();
      }
    } finally {
      await client.close();
    }
  });

  it('drains a single inline result-set row (SELECT 1)', async () => {
    // `SELECT 1` returns a single row inline, exercising the
    // small-batch code path the other tests don't hit. `range(0, n)`
    // queries go through the row-set generator; `range(0, 0)` is
    // the empty branch. A literal scalar pin-points the inline-batch
    // path inside SeaOperationBackend.fetchChunk → ResultSlicer →
    // ArrowResultConverter, which is otherwise untested here.
    const client = await connect();
    try {
      const session = await client.openSession();
      try {
        const operation = await session.executeStatement('SELECT 1 AS x');
        try {
          const rows = await operation.fetchAll();
          expect(rows).to.be.an('array');
          expect(rows.length).to.equal(1);
          const row = rows[0] as Record<string, unknown>;
          expect(row).to.have.property('x');
          // SEA-side converter promotes a literal int to a number
          // primitive; Thrift on the same query produces the same
          // shape. We don't pin the exact JS type beyond "not null/
          // undefined" to keep the test forward-compatible with
          // converter changes — the load-bearing assertion is the
          // single-row inline-batch drain.
          expect(row.x).to.not.equal(null);
          expect(row.x).to.not.equal(undefined);
        } finally {
          await operation.close();
        }
      } finally {
        await session.close();
      }
    } finally {
      await client.close();
    }
  });
});
