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

// Integration suite: connect through both backends, run a probe query,
// and assert byte-identical row output (the M0 parity gate). Requires
// the developer's shell to export the pecotesting secrets:
//   - DATABRICKS_PECOTESTING_SERVER_HOSTNAME
//   - DATABRICKS_PECOTESTING_HTTP_PATH
//   - DATABRICKS_PECOTESTING_TOKEN_PERSONAL
// If any is missing, the suite skips so CI / sandboxes without
// credentials don't flap.

const PROBE_QUERY = "SELECT 1 AS x, 'hello' AS s, true AS b, CAST(1.5 AS DECIMAL(10,2)) AS d, DATE '2026-01-01' AS dt";

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

async function fetchProbeRows(useKernel: boolean, secrets: PecoSecrets): Promise<Array<Record<string, unknown>>> {
  const client = new DBSQLClient();
  await client.connect({
    host: secrets.host,
    path: secrets.path,
    token: secrets.token,
    useKernel,
  } as ConnectionOptions & InternalConnectionOptions);
  try {
    const session = await client.openSession();
    try {
      const operation = await session.executeStatement(PROBE_QUERY);
      try {
        const rows = (await operation.fetchAll()) as Array<Record<string, unknown>>;
        return rows;
      } finally {
        await operation.close();
      }
    } finally {
      await session.close();
    }
  } finally {
    await client.close();
  }
}

// JSON-safe normalisation for byte-identical comparison. Buffers, Dates
// and BigInts each have distinct JSON representations; we coerce them
// to stable strings so deep.equal compares value-for-value across
// backends. The thrift converter and the SEA converter both surface
// these as JS Date / Buffer / Number — but we still normalise here so
// a future divergence (e.g. one path returning a string while the
// other returns a Date) trips the assertion explicitly.
function canonical(value: unknown): unknown {
  if (value === null || value === undefined) return value;
  if (Buffer.isBuffer(value)) return `__buffer__:${value.toString('hex')}`;
  if (value instanceof Date) return `__date__:${value.toISOString()}`;
  if (typeof value === 'bigint') return `__bigint__:${value.toString()}`;
  if (Array.isArray(value)) return value.map(canonical);
  if (typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      out[k] = canonical(v);
    }
    return out;
  }
  return value;
}

describe('SEA results end-to-end (pecotesting parity gate)', function suite() {
  this.timeout(120_000);

  const secrets = readSecrets();

  before(function gate() {
    if (!secrets) {
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  it('SEA backend returns one row with expected columns', async () => {
    const rows = await fetchProbeRows(true, secrets as PecoSecrets);
    expect(rows.length).to.equal(1);
    const row = rows[0];
    expect(row).to.have.property('x');
    expect(row).to.have.property('s');
    expect(row).to.have.property('b');
    expect(row).to.have.property('d');
    expect(row).to.have.property('dt');
    expect(Number(row.x)).to.equal(1);
    expect(row.s).to.equal('hello');
    expect(row.b).to.equal(true);
    expect(Number(row.d)).to.equal(1.5);
  });

  it('Thrift and SEA produce byte-identical rows for the probe query (parity gate)', async () => {
    const kernelRows = await fetchProbeRows(true, secrets as PecoSecrets);
    const thriftRows = await fetchProbeRows(false, secrets as PecoSecrets);
    expect(kernelRows.map(canonical)).to.deep.equal(thriftRows.map(canonical));
  });
});
