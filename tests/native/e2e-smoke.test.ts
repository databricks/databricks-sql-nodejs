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
import { getSeaNative } from '../../lib/sea/SeaNativeLoader';

// Round 2 end-to-end smoke test:
//   1. Open a kernel `Session` via `Database.open(...)` over PAT.
//   2. Execute `SELECT 1`.
//   3. Fetch the first batch — assert the IPC bytes are non-empty.
//   4. Close the statement, then the connection.
//
// Requires three env vars (exported by the developer's shell):
//   - DATABRICKS_PECOTESTING_SERVER_HOSTNAME
//   - DATABRICKS_PECOTESTING_HTTP_PATH
//   - DATABRICKS_PECOTESTING_TOKEN_PERSONAL
// If any is missing, the test is skipped (so CI can keep the file in
// the suite without flapping when secrets aren't provisioned).

interface NativeBinding {
  openSession(opts: {
    hostName: string;
    httpPath: string;
    token: string;
  }): Promise<NativeConnection>;
}

interface NativeConnection {
  executeStatement(
    sql: string,
    options: {
      initialCatalog?: string;
      initialSchema?: string;
      sessionConfig?: Record<string, string>;
    },
  ): Promise<NativeStatement>;
  close(): Promise<void>;
}

interface NativeStatement {
  fetchNextBatch(): Promise<{ ipcBytes: Buffer } | null>;
  schema(): Promise<{ ipcBytes: Buffer }>;
  cancel(): Promise<void>;
  close(): Promise<void>;
}

describe('SEA native binding — Round 2 end-to-end smoke test', function smoke() {
  const hostName = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
  const httpPath = process.env.DATABRICKS_PECOTESTING_HTTP_PATH;
  const token = process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL;

  // Live-warehouse tests can take >2s through warm-up, so bump the
  // mocha default (2000ms) generously.
  this.timeout(60_000);

  before(function gate() {
    if (!hostName || !httpPath || !token) {
      // Use `this.skip()` so the suite is reported as skipped rather
      // than failing on dev machines without the secrets.
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  it('opens a session, runs SELECT 1, and reads the first batch', async () => {
    const binding = getSeaNative() as unknown as NativeBinding;

    const connection = await binding.openSession({
      hostName: hostName as string,
      httpPath: httpPath as string,
      token: token as string,
    });
    expect(connection).to.be.an('object');

    let statement: NativeStatement | null = null;
    try {
      statement = await connection.executeStatement('SELECT 1', {});
      expect(statement).to.be.an('object');

      const batch = await statement.fetchNextBatch();
      expect(batch).to.not.equal(null);
      expect(batch!.ipcBytes).to.be.instanceOf(Buffer);
      expect(batch!.ipcBytes.length).to.be.greaterThan(0);

      // Draining: subsequent fetch should return null (one-row result).
      const after = await statement.fetchNextBatch();
      expect(after).to.equal(null);
    } finally {
      if (statement !== null) {
        await statement.close();
      }
      await connection.close();
    }
  });
});
