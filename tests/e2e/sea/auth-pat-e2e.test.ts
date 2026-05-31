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
 * sea-auth M0 end-to-end:
 *   1. Construct a DBSQLClient.
 *   2. `connect({ useSEA: true, token })` against pecotesting.
 *   3. `openSession()` — round-trips through the napi binding.
 *   4. Close the session, then the client.
 *
 * No query is executed here — execution is the responsibility of the
 * sea-execution feature's own e2e. This test exists solely to confirm
 * the PAT round-trips end-to-end and the napi binding's `openSession`
 * surface is reachable from `DBSQLClient`.
 *
 * Required env (exported by `~/.zshrc` on the developer machine):
 *   - DATABRICKS_PECOTESTING_SERVER_HOSTNAME
 *   - DATABRICKS_PECOTESTING_HTTP_PATH
 *   - DATABRICKS_PECOTESTING_TOKEN_PERSONAL  (preferred — personal PAT)
 *   - DATABRICKS_PECOTESTING_TOKEN           (fallback — shared PAT)
 *
 * If any of the three required env vars is missing, the suite is skipped
 * so CI machines without secrets don't fail-flap.
 */
describe('sea-auth e2e — PAT through DBSQLClient ↔ SeaBackend ↔ napi binding', function suite() {
  const host = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
  const path = process.env.DATABRICKS_PECOTESTING_HTTP_PATH;
  const token =
    process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL || process.env.DATABRICKS_PECOTESTING_TOKEN;

  this.timeout(120_000);

  before(function gate() {
    if (!host || !path || !token) {
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  it('connects, opens a session, closes the session, closes the client', async () => {
    const client = new DBSQLClient();

    const connected = await client.connect({
      host: host as string,
      path: path as string,
      token: token as string,
      // `useSEA` is an internal opt-in (InternalConnectionOptions), not a
      // public ConnectionOptions field — cast exactly as DBSQLClient.connect
      // does internally so the literal passes excess-property checking.
      useSEA: true,
    } as ConnectionOptions & InternalConnectionOptions);
    expect(connected).to.equal(client);

    const session = await client.openSession();
    expect(session).to.exist;
    expect(session.id).to.be.a('string');
    expect(session.id.length).to.be.greaterThan(0);

    const status = await session.close();
    expect(status.isSuccess).to.equal(true);

    await client.close();
  });
});
