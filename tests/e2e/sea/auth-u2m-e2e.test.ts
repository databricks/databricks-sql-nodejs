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
 * sea-auth M1 OAuth U2M end-to-end — **SKIPPED pending browser harness**.
 *
 * U2M is interactive: the kernel opens a system browser
 * (`auth/oauth/u2m.rs:414`, via the `open` crate), binds a local
 * listener on port 8030 (via the JS adapter's hardcoded override), and
 * waits up to 120s for the user to authenticate.
 *
 * Driving this from CI requires Playwright/Puppeteer to navigate the
 * browser through the workspace login + consent screens. That harness
 * is tracked as `TBD-oauth_u2m_test_harness` in testing-agent's
 * findings; until it exists, this test stays `it.skip` so the e2e
 * suite carries a slot for whoever lands the harness work.
 *
 * The intended assertion sequence (mirrors `auth-m2m-e2e.test.ts`):
 *   1. `client.connect({ useSEA: true, authType: 'databricks-oauth' })`
 *      — NO `oauthClientSecret` → kernel picks the U2M flow.
 *   2. `openSession()` — kernel opens browser, waits for callback on
 *      localhost:8030, exchanges the auth code, returns Bearer token,
 *      issues the create-session request to SEA.
 *   3. `session.close()` then `client.close()`.
 *
 * Required env (gated additionally via `it.skip` until the harness
 * lands, so absent env is a no-op today):
 *   - DATABRICKS_PECOTESTING_SERVER_HOSTNAME
 *   - DATABRICKS_PECOTESTING_HTTP_PATH
 *   - (no client_id/secret — U2M uses kernel default `databricks-cli`)
 */
describe('sea-auth e2e — OAuth U2M through DBSQLClient ↔ SeaBackend ↔ napi binding', function suite() {
  this.timeout(300_000);

  it.skip('[pending TBD-oauth_u2m_test_harness] interactive U2M round-trip', async () => {
    const host = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME as string;
    const path = process.env.DATABRICKS_PECOTESTING_HTTP_PATH as string;

    const client = new DBSQLClient();

    const connected = await client.connect({
      host,
      path,
      authType: 'databricks-oauth',
      useSEA: true,
    } as ConnectionOptions & InternalConnectionOptions);
    expect(connected).to.equal(client);

    const session = await client.openSession();
    expect(session.id).to.be.a('string');

    const status = await session.close();
    expect(status.isSuccess).to.equal(true);

    await client.close();
  });
});
