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
import { tableFromIPC } from 'apache-arrow';
import { tryGetSeaNative, Connection, Statement } from '../../../lib/sea/SeaNativeLoader';

// End-to-end smoke test against a live warehouse:
//   1. Open a kernel `Session` over PAT.
//   2. Execute `SELECT 1`, decode the IPC payload, assert the value is 1.
//   3. Exercise lifecycle negative paths (drain-past-null, double-close).
//   4. Close the statement, then the connection.
//
// Required env vars:
//   - DATABRICKS_PECOTESTING_SERVER_HOSTNAME
//   - DATABRICKS_PECOTESTING_HTTP_PATH
//   - DATABRICKS_PECOTESTING_TOKEN_PERSONAL
//
// On dev machines without the secrets the suite is skipped. In CI
// (process.env.CI === 'true') missing secrets are fatal — a silent
// skip would let credential-rotation regressions reach prod.

const REQUIRED_ENV = [
  'DATABRICKS_PECOTESTING_SERVER_HOSTNAME',
  'DATABRICKS_PECOTESTING_HTTP_PATH',
  'DATABRICKS_PECOTESTING_TOKEN_PERSONAL',
] as const;

function missingEnvVars(): string[] {
  return REQUIRED_ENV.filter((name) => !process.env[name]);
}

describe('SEA native binding — end-to-end smoke', function smoke() {
  // Live-warehouse tests can take >2s through warm-up.
  this.timeout(60_000);

  const binding = tryGetSeaNative();
  if (binding === undefined) {
    // Optional dependency absent — never reach the live path.
    it.skip('SEA native binding not available on this platform');
    return;
  }

  const missing = missingEnvVars();
  if (missing.length > 0) {
    if (process.env.CI === 'true') {
      // Fail loudly so credential-rotation regressions surface in CI.
      it('fails when required env vars are missing in CI', () => {
        expect.fail(`Missing required env vars in CI: ${missing.join(', ')}. Set CI=false to skip locally.`);
      });
      return;
    }
    it.skip(`skipped — missing env vars: ${missing.join(', ')}`);
    return;
  }

  const hostName = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME as string;
  const httpPath = process.env.DATABRICKS_PECOTESTING_HTTP_PATH as string;
  const token = process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL as string;

  it('opens a session, runs SELECT 1, decodes the IPC payload to 1', async () => {
    const connection: Connection = await binding.openSession({ hostName, httpPath, token });
    expect(connection).to.be.an('object');

    let statement: Statement | null = null;
    try {
      statement = await connection.executeStatement('SELECT 1', {});
      expect(statement).to.be.an('object');

      const batch = await statement.fetchNextBatch();
      expect(batch).to.not.equal(null);
      expect(batch!.ipcBytes).to.be.instanceOf(Buffer);
      expect(batch!.ipcBytes.length).to.be.greaterThan(0);

      // Decode the IPC payload and verify the value, not just the shape.
      const table = tableFromIPC(batch!.ipcBytes);
      expect(table.numRows).to.equal(1);
      expect(Number(table.getChildAt(0)!.get(0))).to.equal(1);

      // Drain-past-null: subsequent fetch returns null.
      const after = await statement.fetchNextBatch();
      expect(after).to.equal(null);

      // Drain-past-drained: another fetch still returns null (idempotent).
      const afterAgain = await statement.fetchNextBatch();
      expect(afterAgain).to.equal(null);
    } finally {
      if (statement !== null) {
        await statement.close();
      }
      await connection.close();
    }
  });

  it('returns a schema IPC payload before any batch is fetched', async () => {
    const connection: Connection = await binding.openSession({ hostName, httpPath, token });
    try {
      const statement = await connection.executeStatement('SELECT 1', {});
      try {
        const schema = await statement.schema();
        expect(schema.ipcBytes).to.be.instanceOf(Buffer);
        expect(schema.ipcBytes.length).to.be.greaterThan(0);
      } finally {
        await statement.close();
      }
    } finally {
      await connection.close();
    }
  });
});
