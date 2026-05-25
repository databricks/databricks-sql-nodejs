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
import { DBSQLClient } from '../../../lib';
import type { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';

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
    }
  });

  async function connect() {
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
});
