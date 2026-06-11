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
import { DBSQLClient } from '../../../lib';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../../lib/contracts/InternalConnectionOptions';

/**
 * Canonical `SELECT 1` round-trip over the kernel backend.
 *
 * Unlike `execution-e2e.test.ts` (which exercises the operation
 * lifecycle but does not read rows), this asserts the full pipeline
 * end to end — including result fetch — when the kernel backend is
 * selected via `useKernel: true`:
 *
 *   DBSQLClient.connect({ useKernel: true })
 *     → KernelBackend → napi binding (loaded from the published
 *       @databricks/databricks-sql-kernel-<triple> optional dependency)
 *     → live warehouse → inline Arrow result → fetchAll()
 *
 * This is the smoke test for "the released driver consumes the npm
 * kernel package and a query actually returns a value", matching the
 * manual verification used to validate the optionalDependencies wiring.
 *
 * **Gating:** requires the pecotesting secrets exported in the shell.
 * If any is missing, the suite is skipped so machines without
 * provisioned credentials don't flap.
 */
describe('kernel SELECT 1 end-to-end (result fetch)', function e2eSuite() {
  const host = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
  const path = process.env.DATABRICKS_PECOTESTING_HTTP_PATH;
  const token = process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL;

  // Live-warehouse round-trips can take a few seconds through warm-up.
  this.timeout(60_000);

  before(function gate() {
    if (!host || !path || !token) {
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  it('runs SELECT 1 via useKernel and fetches the row', async () => {
    const client = new DBSQLClient();

    await client.connect({
      host: host as string,
      path: path as string,
      token: token as string,
      useKernel: true,
    } as ConnectionOptions & InternalConnectionOptions);

    const session = await client.openSession();
    const operation = await session.executeStatement('SELECT 1 AS one');

    const rows = (await operation.fetchAll()) as Array<Record<string, unknown>>;
    expect(rows).to.be.an('array').with.length(1);
    expect(Number(rows[0].one)).to.equal(1);

    await operation.close();
    await session.close();
    await client.close();
  });
});
