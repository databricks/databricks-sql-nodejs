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
import * as fs from 'fs';
import * as os from 'os';
import * as nodePath from 'path';
import { DBSQLClient } from '../../../lib';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../../lib/contracts/InternalConnectionOptions';

/**
 * kernel-auth OAuth U2M end-to-end, focused on the `tokenCacheEnabled`
 * control (PR #513 / kernel #283).
 *
 * U2M is **interactive**: the kernel opens a system browser, binds a local
 * listener on port 8030 (via the JS adapter's hardcoded override), and waits
 * for the user to complete the workspace login + consent. Because a human has
 * to click through the browser, this suite cannot run unattended — it is
 * therefore gated behind an explicit opt-in env var (`DATABRICKS_KERNEL_U2M_INTERACTIVE`)
 * on top of the workspace host/path, so CI (which sets neither) skips it and
 * never flaps.
 *
 * What it proves end-to-end, through
 *   DBSQLClient.connect({ useKernel: true, authType: 'databricks-oauth' })
 *     → KernelBackend → napi binding → live workspace U2M browser flow:
 *
 *   1. **Disabled by default.** With `tokenCacheEnabled` unset, the connector
 *      passes `tokenCacheEnabled: false` to the kernel, so NO token is written
 *      to the on-disk cache — matching the Thrift backend's in-memory posture.
 *   2. **Opt-in enable.** With `tokenCacheEnabled: true`, the kernel persists
 *      the U2M refresh token (AES-256 encrypted, not plaintext) to its on-disk
 *      cache under `dirs::config_dir()/databricks-sql-kernel/oauth/`.
 *
 * Required env (suite skips unless ALL are set):
 *   - DATABRICKS_PECOTESTING_SERVER_HOSTNAME
 *   - DATABRICKS_PECOTESTING_HTTP_PATH
 *   - DATABRICKS_KERNEL_U2M_INTERACTIVE   (any non-empty value — the human opt-in)
 *
 * Each case opens a fresh browser login (the cache dir is emptied between
 * cases so a prior case's file can't produce a cache-hit that skips the
 * browser). A human must complete both logins within the suite timeout.
 */

/**
 * The kernel stores U2M cache files at `dirs::config_dir()/databricks-sql-kernel/oauth/`
 * (see `src/auth/oauth/cache.rs`). Mirror the Rust `dirs` crate's per-platform
 * `config_dir()` here so the assertion checks the same directory the kernel
 * writes to — on macOS that is `~/Library/Application Support`, NOT `~/.config`
 * (the Linux path the docstrings mention).
 */
function kernelOAuthCacheDir(): string {
  const home = os.homedir();
  if (process.platform === 'darwin') {
    return nodePath.join(home, 'Library', 'Application Support', 'databricks-sql-kernel', 'oauth');
  }
  if (process.platform === 'win32') {
    const appData = process.env.APPDATA || nodePath.join(home, 'AppData', 'Roaming');
    return nodePath.join(appData, 'databricks-sql-kernel', 'oauth');
  }
  const xdg = process.env.XDG_CONFIG_HOME || nodePath.join(home, '.config');
  return nodePath.join(xdg, 'databricks-sql-kernel', 'oauth');
}

/** Cache files are named `{sha256}.json` (`CacheKey::to_filename`). */
function listCacheFiles(dir: string): string[] {
  if (!fs.existsSync(dir)) {
    return [];
  }
  return fs.readdirSync(dir).filter((f) => f.endsWith('.json'));
}

describe('kernel-auth e2e — OAuth U2M token cache (interactive)', function suite() {
  const host = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
  const httpPath = process.env.DATABRICKS_PECOTESTING_HTTP_PATH;
  const interactive = process.env.DATABRICKS_KERNEL_U2M_INTERACTIVE;

  const cacheDir = kernelOAuthCacheDir();
  let backupDir: string | undefined;

  // Interactive browser login + live warehouse round-trip; give the human time.
  this.timeout(300_000);

  before(function gate() {
    if (!host || !httpPath || !interactive) {
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
    // Never destroy a real user's cached tokens: move any existing cache
    // aside for the duration of the suite and restore it in `after`.
    if (fs.existsSync(cacheDir)) {
      backupDir = `${cacheDir}.e2e-backup-${process.pid}`;
      fs.renameSync(cacheDir, backupDir);
    }
  });

  after(() => {
    // Remove whatever the test wrote, then restore the user's originals.
    if (fs.existsSync(cacheDir)) {
      fs.rmSync(cacheDir, { recursive: true, force: true });
    }
    if (backupDir && fs.existsSync(backupDir)) {
      fs.renameSync(backupDir, cacheDir);
    }
  });

  beforeEach(() => {
    // Start each case from an empty cache dir so a prior case's file cannot
    // leak into this assertion — nor produce a cache-hit that skips the browser.
    if (fs.existsSync(cacheDir)) {
      fs.rmSync(cacheDir, { recursive: true, force: true });
    }
  });

  it('default (tokenCacheEnabled unset) persists NO token to disk', async () => {
    const client = new DBSQLClient();
    await client.connect({
      host: host as string,
      path: httpPath as string,
      authType: 'databricks-oauth',
      useKernel: true,
    } as ConnectionOptions & InternalConnectionOptions);

    const session = await client.openSession();
    expect(session.id).to.be.a('string');

    const operation = await session.executeStatement('SELECT 1 AS one');
    const rows = (await operation.fetchAll()) as Array<Record<string, unknown>>;
    expect(Number(rows[0].one)).to.equal(1);

    await operation.close();
    await session.close();
    await client.close();

    expect(listCacheFiles(cacheDir), 'no on-disk token cache when tokenCacheEnabled is unset').to.have.length(0);
  });

  it('tokenCacheEnabled:true persists an encrypted token to disk', async () => {
    const client = new DBSQLClient();
    await client.connect({
      host: host as string,
      path: httpPath as string,
      authType: 'databricks-oauth',
      useKernel: true,
      tokenCacheEnabled: true,
    } as ConnectionOptions & InternalConnectionOptions);

    const session = await client.openSession();
    expect(session.id).to.be.a('string');

    const operation = await session.executeStatement('SELECT 1 AS one');
    const rows = (await operation.fetchAll()) as Array<Record<string, unknown>>;
    expect(Number(rows[0].one)).to.equal(1);

    await operation.close();
    await session.close();
    await client.close();

    const files = listCacheFiles(cacheDir);
    expect(files, 'a token cache file is written when tokenCacheEnabled is true').to.have.length.greaterThan(0);

    // Encrypted at rest: the persisted file is AES-256 ciphertext, so it must
    // NOT parse as the plaintext JSON token structure.
    const raw = fs.readFileSync(nodePath.join(cacheDir, files[0]));
    expect(raw.length, 'cache file is non-empty').to.be.greaterThan(0);
    let parsedAsJson = false;
    try {
      JSON.parse(raw.toString('utf8'));
      parsedAsJson = true;
    } catch {
      // expected — ciphertext is not valid UTF-8 JSON
    }
    expect(parsedAsJson, 'cache file must be encrypted, not plaintext JSON').to.equal(false);
  });
});
