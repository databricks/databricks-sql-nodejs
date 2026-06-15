# Kernel Bug Blitz — Node.js driver (kernel mode)

Runnable scripts for the "Kernel Bug Blitz" CUJ scenarios, exercising
`@databricks/sql@1.16.0-rc.1` in **kernel mode** (`useKernel: true`) against a
real warehouse.

## Setup (clean-room, like a new user)
```bash
cd ~/kernel-bugbash          # this folder
npm install @databricks/sql@1.16.0-rc.1   # already installed here
node -e "console.log(require('@databricks/sql/native/kernel').version())"  # -> 0.2.0
```

## Credentials (env — source ~/.zshrc)
- `DATABRICKS_PECOTESTING_SERVER_HOSTNAME` — host (e.g. adb-…azuredatabricks.net)
- `DATABRICKS_PECOTESTING_HTTP_PATH2` — `/sql/1.0/warehouses/00adc7b6c00429b8`
- `DATABRICKS_PECOTESTING_TOKEN` — PAT
- `DATABRICKS_PECO_CLIENT_ID_PERSONAL` / `DATABRICKS_PECO_CLIENT_SECRET_PERSONAL` — M2M (the non-`_PERSONAL` pair is rejected `invalid_client`)

## Run
```bash
node scripts/00-smoke.js            # SELECT 1 via kernel — run first
node scripts/02-query-execution.js  # one category
node scripts/run-all.js             # everything, combined results table
```
Each scenario prints `PASS|FAIL|SKIP | <name> | <note> | <ms>`.

## Scripts ↔ sheet categories
| Script | Category |
|---|---|
| `00-smoke.js` | kernel SELECT 1 smoke |
| `01-connection-auth.js` | Connection & Auth (M2M, U2M*, SPOG, proxy*) |
| `02-query-execution.js` | inline, CloudFetch, STATEMENT_TIMEOUT, cancel, concurrent |
| `03-metadata-tags.js` | metadata calls, query tags, metric view |
| `04-data-types.js` | primitives, complex, timestamps, VARIANT, GEO |
| `05-errors-resilience.js` | invalid-query error, retry* |
| `06-cujs.js` | CRUD+switch, large cells, bulk 50k, negative paths |
| `07-lifecycle-session.js` | reuse, multi-op, re-run, double/after-close, async, interleave, session config, UA, query id |
| `08-params-fetch.js` | named/positional params, scalar types, zero-rows, wide row, NULL-heavy, unicode, chunks, multi-statement |

## Skipped / can't-run-here (need infra or manual)
- **OAuth U2M** — interactive browser SSO; run by hand.
- **HTTP forward proxy (Basic auth)** — needs a running proxy.
- **Retry 503/429 + backoff** — needs a fault-injecting proxy.
- **getImportedKeys / getExportedKeys** — not in the Node public API (only `getCrossReference`); expected limitation.
- **Python-only rows** (cursor.rowcount, Arrow/pandas) — N/A for Node.

`lib.js` holds the shared connect/run helpers.
