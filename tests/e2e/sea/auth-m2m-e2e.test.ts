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

/**
 * sea-auth M1 OAuth M2M end-to-end:
 *   1. Construct a DBSQLClient.
 *   2. `connect({ useSEA: true, authType: 'databricks-oauth', oauthClientId,
 *      oauthClientSecret })` against pecotesting.
 *   3. `openSession()` — kernel runs OIDC discovery + client_credentials
 *      exchange. Successful openSession is the proof that the kernel-side
 *      OAuth M2M plumbing works end-to-end: discovery + token exchange +
 *      Bearer header on the create-session request all succeeded.
 *   4. Close the session, then the client.
 *
 * No query is executed here — execution is the responsibility of the
 * sea-execution feature's own e2e (mirror of the M0 PAT e2e scope at
 * `auth-pat-e2e.test.ts`). If kernel-side OAuth fails, `openSession()`
 * raises before returning.
 *
 * Required env (exported by `~/.zshrc` on the developer machine):
 *   - DATABRICKS_PECOTESTING_SERVER_HOSTNAME
 *   - DATABRICKS_PECOTESTING_HTTP_PATH
 *   - DATABRICKS_PECO_CLIENT_ID
 *   - DATABRICKS_PECO_CLIENT_SECRET
 *
 * Skipped (not failed) when any of the four env vars is missing, so CI
 * machines without OAuth credentials don't fail-flap.
 */
describe('sea-auth e2e — OAuth M2M through DBSQLClient ↔ SeaBackend ↔ napi binding', function suite() {
  const host = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
  const path = process.env.DATABRICKS_PECOTESTING_HTTP_PATH;
  const oauthClientId = process.env.DATABRICKS_PECO_CLIENT_ID;
  const oauthClientSecret = process.env.DATABRICKS_PECO_CLIENT_SECRET;

  this.timeout(120_000);

  before(function gate() {
    if (!host || !path || !oauthClientId || !oauthClientSecret) {
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  it('connects, opens a session, closes the session, closes the client', async () => {
    const client = new DBSQLClient();

    const connected = await client.connect({
      host: host as string,
      path: path as string,
      authType: 'databricks-oauth',
      oauthClientId: oauthClientId as string,
      oauthClientSecret: oauthClientSecret as string,
      useSEA: true,
    });
    expect(connected).to.equal(client);

    const session = await client.openSession();
    expect(session.id).to.be.a('string');
    expect(session.id.length).to.be.greaterThan(0);

    const status = await session.close();
    expect(status.isSuccess).to.equal(true);

    await client.close();
  });
});
