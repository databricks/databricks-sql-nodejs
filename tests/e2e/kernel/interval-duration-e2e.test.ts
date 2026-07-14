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

// Exercises the KernelArrowIpcDurationFix rewriter against a REAL kernel-produced
// Arrow Duration buffer (Spark INTERVAL DAY-TIME surfaces as Arrow Duration,
// type id 18, which apache-arrow@13 can't decode). Without the rewriter this
// query throws `Unrecognized type: "Duration" (18)`, so a passing fetch proves
// the hand-rolled FlatBuffer schema re-encode + Int64 splice-back ran.
//
// On THIS layer (kernel execution + results, PR 2/3) the converter does not yet
// consume the `duration_unit` marker, so the value surfaces as a raw Int64 —
// asserted here. The Phase-1 formatter that turns it into the thrift string
// "1 02:03:04.000000000" lands in PR 3/3 (#411), where KernelIntervalParity covers
// the formatted output. Requires the pecotesting secrets; skips otherwise.

const DURATION_QUERY = "SELECT INTERVAL '1 02:03:04' DAY TO SECOND AS dt";

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

async function fetchOneRow(useKernel: boolean, secrets: PecoSecrets): Promise<Record<string, unknown>> {
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
      const operation = await session.executeStatement(DURATION_QUERY);
      try {
        const rows = (await operation.fetchAll()) as Array<Record<string, unknown>>;
        return rows[0];
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

describe('kernel INTERVAL DAY-TIME (Arrow Duration rewriter) end-to-end', function suite() {
  this.timeout(120_000);

  const secrets = readSecrets();

  before(function gate() {
    if (!secrets) {
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  it('decodes a real Arrow Duration column without the "Duration (18)" crash (rewriter ran)', async () => {
    const row = await fetchOneRow(true, secrets as PecoSecrets);
    // A returned row at all means the schema rewrite + Int64 splice-back ran;
    // an un-rewritten Duration would have thrown during RecordBatchReader.from.
    expect(row).to.have.property('dt');
    expect(row.dt, 'INTERVAL DAY-TIME value should be present').to.not.equal(undefined);
  });

  it('surfaces the value as the formatted thrift INTERVAL DAY-TIME string (#411 formatter wired)', async () => {
    const row = await fetchOneRow(true, secrets as PecoSecrets);
    // #411 wires the duration_unit formatter, so the raw Int64 the rewriter
    // produces is rendered as the thrift "D HH:mm:ss.fffffffff" string —
    // byte-identical to the Thrift path. (On the #410 layer this surfaced as
    // the raw integer count.)
    expect(typeof row.dt).to.equal('string');
    expect(row.dt).to.equal('1 02:03:04.000000000');
  });
});
