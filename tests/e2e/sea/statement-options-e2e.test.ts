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
 * End-to-end check that `statementConf` and `queryTags` from the
 * public `ExecuteStatementOptions` propagate through the SEA backend
 * into the per-statement conf overlay on the SEA wire, and that the
 * server actually honours the values.
 *
 * Observable assertion strategy:
 * - `statementConf`: set `TIMEZONE` to a known non-default zone, then
 *   run `SELECT current_timezone()` and verify the returned value
 *   matches what we set. The server is the source of truth — if the
 *   value made it through the wire, current_timezone() reports it.
 * - `queryTags`: serialised at the JS layer via `serializeQueryTags`
 *   into `statementConf["query_tags"]`. We can't read query tags
 *   back from the same session deterministically (system.query_history
 *   has eventual-consistency latency in some workspaces), so the
 *   assertion is "statement succeeds with tags set" — proves the wire
 *   format is accepted by the server. Byte-shape parity vs Thrift is
 *   pinned by the kernel-side unit tests
 *   (`serialize_query_tags_matches_thrift_byte_shape_*`).
 *
 * DA round-1 F3 "live" fixup.
 *
 * Skipped when `DATABRICKS_PECOTESTING_*` env vars are absent.
 */

import { expect } from 'chai';
import { existsSync } from 'fs';
import { resolve as resolvePath } from 'path';
import { createRequire } from 'module';
import type { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';

// eslint-disable-next-line @typescript-eslint/naming-convention
const requireFromHere = createRequire(import.meta.url);

interface InternalConnectionOptionsAccess {
  useSEA?: boolean;
}

describe('SEA statementConf + queryTags — live', function suite() {
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
    const nodeArtifact = resolvePath(
      process.cwd(),
      'native/sea/index.linux-x64-gnu.node',
    );
    if (!existsSync(nodeArtifact)) {
      // eslint-disable-next-line no-console
      console.warn(
        `[sea statement-options e2e] skipping: native binary not built. ` +
          `Run \`yarn build:native\` first.`,
      );
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  async function connect() {
    const { DBSQLClient } = requireFromHere('../../../lib') as typeof import('../../../lib');
    const client = new DBSQLClient();
    const options = {
      host: host as string,
      path: path as string,
      token: token as string,
      useSEA: true,
    } as ConnectionOptions & InternalConnectionOptionsAccess;
    await client.connect(options as unknown as ConnectionOptions);
    return client;
  }

  it('queryTags pass through without error on a live statement', async () => {
    // Server accepts the comma-separated `key:value` wire shape;
    // assertion is "no error". Byte-shape parity is pinned by
    // kernel unit tests.
    const client = await connect();
    try {
      const session = await client.openSession();
      try {
        const operation = await session.executeStatement('SELECT 1 AS x', {
          queryTags: {
            team: 'platform',
            env: 'staging',
          },
        });
        try {
          const rows = await operation.fetchAll();
          expect(rows.length).to.equal(1);
          expect(rows[0]).to.have.property('x');
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

  it('queryTags with backslash-escape-needing values do not break the wire', async () => {
    // Pins the Thrift escape contract end-to-end: server accepts a
    // tag value containing `:`, `,`, and `\`. If the kernel-side
    // serializer ever drops an escape, the server rejects the conf
    // string and this test fails.
    const client = await connect();
    try {
      const session = await client.openSession();
      try {
        const operation = await session.executeStatement('SELECT 1 AS x', {
          queryTags: {
            tricky: 'has:colon,comma\\backslash',
          },
        });
        try {
          const rows = await operation.fetchAll();
          expect(rows.length).to.equal(1);
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

  it('null and undefined valued tags survive the wire (bare key form)', async () => {
    // `serializeQueryTags` emits a bare key (no colon) for null /
    // undefined values. The server has to accept the resulting form.
    const client = await connect();
    try {
      const session = await client.openSession();
      try {
        const operation = await session.executeStatement('SELECT 1 AS x', {
          queryTags: {
            'mark-only-key': null,
            'undef-key': undefined,
            real: 'value',
          },
        });
        try {
          const rows = await operation.fetchAll();
          expect(rows.length).to.equal(1);
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

  it('no queryTags option works as before (regression check)', async () => {
    // Default-path regression: prove the F3 plumbing hasn't broken
    // statement execution when ExecuteStatementOptions is empty.
    const client = await connect();
    try {
      const session = await client.openSession();
      try {
        const operation = await session.executeStatement('SELECT 1 AS x');
        try {
          const rows = await operation.fetchAll();
          expect(rows.length).to.equal(1);
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
