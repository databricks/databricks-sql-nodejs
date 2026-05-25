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
 * End-to-end check that `maxConnections` flows from
 * `ConnectionOptions` through `buildSeaConnectionOptions` through the
 * napi `openSession` into the kernel's `HttpConfig::pool_max_idle_per_host`,
 * and that a session opened with a custom value round-trips a real
 * query against pecotesting.
 *
 * We can't directly observe the underlying reqwest connection pool
 * from JS, so the meaningful assertion is "session opens + query
 * succeeds when the option is set". Combined with the unit tests
 * that mock-verify the napi-call shape, this proves the option
 * reaches the kernel without breaking the wire path.
 *
 * DA round-1 F1 L2 ("live") fixup.
 *
 * Skipped when `DATABRICKS_PECOTESTING_*` env vars are absent.
 */

import { expect } from 'chai';
import { existsSync } from 'fs';
import { resolve as resolvePath } from 'path';
import { createRequire } from 'module';

// eslint-disable-next-line @typescript-eslint/naming-convention
const requireFromHere = createRequire(import.meta.url);

interface NativeBinding {
  openSession(opts: {
    hostName: string;
    httpPath: string;
    token: string;
    maxConnections?: number;
  }): Promise<NativeConnection>;
}

interface NativeConnection {
  executeStatement(sql: string): Promise<NativeStatement>;
  close(): Promise<void>;
}

interface NativeStatement {
  fetchNextBatch(): Promise<{ ipcBytes: Buffer } | null>;
  schema(): Promise<{ ipcBytes: Buffer }>;
  cancel(): Promise<void>;
  close(): Promise<void>;
}

describe('SEA maxConnections — live round-trip', function suite() {
  this.timeout(120_000);

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
    const nodeArtifact = resolvePath(
      process.cwd(),
      'native/sea/index.linux-x64-gnu.node',
    );
    if (!existsSync(nodeArtifact)) {
      // eslint-disable-next-line no-console
      console.warn(
        `[sea max-connections e2e] skipping: native binary not built. ` +
          `Run \`yarn build:native\` first.`,
      );
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  function loadBinding(): NativeBinding {
    return requireFromHere('../../../native/sea/index.js') as NativeBinding;
  }

  it('opens a session with maxConnections=1 and runs a query', async () => {
    // `maxConnections=1` is the minimum bound the JS layer accepts —
    // exercises the low end of the validation range against a real
    // warehouse. The kernel's `pool_max_idle_per_host=1` doesn't cap
    // *active* connections (reqwest opens more as needed); it caps
    // *idle* connections retained in the pool. So a query still
    // succeeds; this asserts the option doesn't break the wire path.
    const binding = loadBinding();
    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
      maxConnections: 1,
    });
    let statement: NativeStatement | null = null;
    try {
      statement = await connection.executeStatement('SELECT 1 AS x');
      const envelope = await statement.fetchNextBatch();
      expect(envelope).to.not.equal(null);
    } finally {
      if (statement !== null) {
        try {
          await statement.close();
        } catch (_) {
          // best-effort cleanup
        }
      }
      await connection.close();
    }
  });

  it('opens a session with maxConnections=200 and runs a query', async () => {
    // High-end value within typical SEA workload range. Proves the
    // option carries values much larger than the kernel default
    // (100) without breaking the wire path.
    const binding = loadBinding();
    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
      maxConnections: 200,
    });
    let statement: NativeStatement | null = null;
    try {
      statement = await connection.executeStatement('SELECT 1 AS x');
      const envelope = await statement.fetchNextBatch();
      expect(envelope).to.not.equal(null);
    } finally {
      if (statement !== null) {
        try {
          await statement.close();
        } catch (_) {
          // best-effort cleanup
        }
      }
      await connection.close();
    }
  });

  it('omitting maxConnections (kernel default 100) still works', async () => {
    // Default-path regression check — proves we haven't broken the
    // existing no-options call site.
    const binding = loadBinding();
    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
    });
    let statement: NativeStatement | null = null;
    try {
      statement = await connection.executeStatement('SELECT 1 AS x');
      const envelope = await statement.fetchNextBatch();
      expect(envelope).to.not.equal(null);
    } finally {
      if (statement !== null) {
        try {
          await statement.close();
        } catch (_) {
          // best-effort cleanup
        }
      }
      await connection.close();
    }
  });
});
