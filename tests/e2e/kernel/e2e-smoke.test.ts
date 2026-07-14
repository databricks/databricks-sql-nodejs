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
import { tryGetKernelNative, KernelConnection, KernelStatement } from '../../../lib/kernel/KernelNativeLoader';
import config from '../utils/config';

// End-to-end smoke test against a live warehouse:
//   1. Open a kernel `Session` over PAT.
//   2. Execute `SELECT 1`, decode the IPC payload, assert the value is 1.
//   3. Exercise lifecycle negative paths (drain-past-null, double-close).
//   4. Close the statement, then the connection.
//
// Credentials come from the shared e2e config (tests/e2e/utils/config.ts:
// E2E_HOST / E2E_PATH / E2E_ACCESS_TOKEN) — the single credential source
// used by every other e2e test, so `npm run e2e` has one consistent
// skip/fail contract rather than two.

describe('kernel native binding — end-to-end smoke', function smoke() {
  // Live-warehouse tests can take >2s through warm-up.
  this.timeout(60_000);

  const binding = tryGetKernelNative();
  if (binding === undefined) {
    // Optional dependency absent on this platform — never reach the live path.
    it.skip('kernel native binding not available on this platform');
    return;
  }

  const { host: hostName, path: httpPath, token } = config;

  it('opens a session, runs SELECT 1, decodes the IPC payload to 1', async () => {
    const connection: KernelConnection = await binding.openSession({ hostName, httpPath, token });
    expect(connection).to.be.an('object');

    let statement: KernelStatement | null = null;
    try {
      statement = await connection.executeStatement('SELECT 1');
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
    const connection: KernelConnection = await binding.openSession({ hostName, httpPath, token });
    try {
      const statement = await connection.executeStatement('SELECT 1');
      try {
        // schema() is synchronous on the binding (cached at construction).
        const schema = statement.schema();
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
