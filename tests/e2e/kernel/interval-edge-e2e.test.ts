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

/* eslint-disable no-console */

import { expect } from 'chai';
import { DBSQLClient } from '../../../lib';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../../lib/contracts/InternalConnectionOptions';
import { getKernelNative } from '../../../lib/kernel/KernelNativeLoader';

// INTERVAL edge cases the unit suite can't easily build (null, multi-row).
// Verified byte-identical to the Thrift backend against a live warehouse.
// Requires the pecotesting secrets AND the native binding — skips otherwise.

interface PecoSecrets {
  host: string;
  path: string;
  token: string;
}

function readSecrets(): PecoSecrets | null {
  const host = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
  const path = process.env.DATABRICKS_PECOTESTING_HTTP_PATH;
  const token = process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL;
  if (!host || !path || !token) return null;
  return { host, path, token };
}

async function kernelValues(sql: string, secrets: PecoSecrets): Promise<unknown[]> {
  const client = new DBSQLClient();
  await client.connect({ ...secrets, useKernel: true } as ConnectionOptions & InternalConnectionOptions);
  try {
    const session = await client.openSession();
    const op = await session.executeStatement(sql);
    const rows = (await op.fetchAll()) as Array<Record<string, unknown>>;
    await op.close();
    await session.close();
    return rows.map((r) => r.v);
  } finally {
    await client.close();
  }
}

describe('SEA INTERVAL edge cases end-to-end', function suite() {
  this.timeout(120_000);

  const secrets = readSecrets();

  before(function gate() {
    // eslint-disable-next-line no-invalid-this
    const self = this;
    if (!secrets) {
      self.skip();
      return;
    }
    // Skip (not error) when the native binding isn't built/installed.
    try {
      getKernelNative();
    } catch {
      self.skip();
    }
  });

  it('NULL INTERVAL DAY-TIME → null', async () => {
    const values = await kernelValues('SELECT CAST(NULL AS INTERVAL DAY TO SECOND) AS v', secrets as PecoSecrets);
    expect(values).to.deep.equal([null]);
  });

  it('multi-row INTERVAL DAY-TIME batch formats every row', async () => {
    const values = await kernelValues(
      "SELECT * FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2 03:00:00' DAY TO SECOND), (INTERVAL '0' DAY) AS t(v)",
      secrets as PecoSecrets,
    );
    expect(values).to.deep.equal(['1 00:00:00.000000000', '2 03:00:00.000000000', '0 00:00:00.000000000']);
  });
});
