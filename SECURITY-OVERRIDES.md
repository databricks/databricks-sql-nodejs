# Security Overrides

The `overrides` block in `package.json` pins transitive (and one direct) dependencies to versions that clear known CVEs. Each entry is debt — when the underlying ecosystem moves on, the corresponding entry should be removed.

This file documents the provenance and exit condition for each override. **When adding or removing an override, update this file in the same commit.**

## Conventions

- **Class**: `runtime` if the package ends up in the published `dist/` runtime path; `dev` if it's used only by tooling (eslint, mocha, nyc, prettier, etc.). The published tarball excludes everything except `dist/`, `thrift/`, `native/`, `LICENSE`, `NOTICE`, `package.json`, `README.md` — so dev-tooling overrides do not ship to consumers but DO surface in customer-side scanners (Dependabot, Snyk, OSV) that read our lockfile.
- **Exit condition**: the smallest change that would let us drop the override entry. Usually "upstream bump", sometimes "upstream widens the patched version into its dep range".

---

## Entries

### `basic-ftp: ^6.2.1`

- **Class**: runtime
- **Path**: `proxy-agent → pac-proxy-agent → get-uri → basic-ftp`
- **CVEs cleared**: GHSA-5rq4-664w-9x2c, GHSA-6v7q-wjvx-w8wg, GHSA-rp42-5vxx-qpwr, GHSA-rpmf-866q-6p89, GHSA-c475-qrg2-pj4r
- **Why an override is needed**: `get-uri` (through 8.0.1) caps `basic-ftp` at `^5.3.1`, below the 6.2.1 fix. The `Client` API get-uri uses is unchanged in 6.x; the 6.0 major only defaults `allowSeparateTransferHost` to `false`, so a PASV reply naming a different host is rejected.
- **Exit**: `get-uri` widens its `basic-ftp` range to include `^6.2.1`.

### `@75lb/deep-merge: ^1.1.2`

- **Class**: dev (apache-arrow's CLI tooling — not in runtime path)
- **Path**: `apache-arrow → command-line-usage → table-layout → @75lb/deep-merge`
- **CVEs cleared**: GHSA-28mc-g557-92m7
- **Exit**: `table-layout` bumps its dep. Note `apache-arrow@13` ships unused CLI tooling — bumping arrow to `15.x+` drops this dep entirely.

### `ip-address: ^10.1.1`

- **Class**: runtime
- **Path**: `proxy-agent → socks-proxy-agent → socks → ip-address`
- **CVEs cleared**: GHSA-v2v4-37r5-5v8g (IPv6 parsing DoS)
- **Why an override is needed**: `socks` caps its `ip-address` dependency below the patched `^10.1.1`, so a plain bump of the parent can't reach the fix — the override is required to force the patched version.
- **Exit**: `socks` widens its `ip-address` range to include `^10.x`. Note: `ip-address@10` is CommonJS with conditional exports — verify any future bump retains CJS compat for our `dist/`.

### `form-data: ^4.0.4`

- **Class**: runtime
- **Path**: `node-fetch → form-data` (multipart bodies)
- **CVEs cleared**: GHSA-fjxv-7rqg-78g4 (unsafe random boundary generation)
- **Exit**: `node-fetch` bumps its `form-data` dep range to include the patched line.

### `serialize-javascript: ^7.0.5`

- **Class**: dev (mocha)
- **Path**: `mocha → serialize-javascript`
- **CVEs cleared**: GHSA-5c6j-r48x-rmvq (XSS via prototype pollution)
- **Note**: the patched line requires Node ≥ 20, which is satisfied by the supported Node 22.23.4 minimum.
- **Exit**: mocha bumps its declared range to the patched line.

### `uuid: ^11.1.1`

- **Class**: dev (nyc)
- **Path**: `nyc → istanbul-lib-processinfo → uuid`
- **CVEs cleared**: GHSA-w5hq-g745-h8pq (buffer-bounds in v3/v5/v6, fixed in 11.1.1)
- **Why an override is needed**: `istanbul-lib-processinfo` declares `uuid: ^8.3.2`, below the fix. The runtime paths (our top-level `uuid` and `thrift@0.25 → uuid`) already declare `^11.1.1` and do not depend on this override, which matters because npm never applies it for consumers. Keep `thrift`'s uuid range CommonJS-compatible (≤ 11); `uuid@12+` is ESM-only and breaks `require()` in Jest (#461). `tests/unit/thrift-cjs-compat.test.ts` checks this.
- **Exit**: `istanbul-lib-processinfo` (or nyc) moves to `uuid ^11.1.1` or drops the dep.

---

## How to audit

```bash
# Show what depends on a specific override target:
npm ls <package-name>

# Re-run the lockfile against OSV-Scanner to verify findings are still cleared:
osv-scanner scan source --lockfile=package-lock.json
```

When all entries' exit conditions are met, this file should be deleted along with the corresponding `overrides` block.
