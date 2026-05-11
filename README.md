# Databricks SQL Driver for Node.js

![http://www.apache.org/licenses/LICENSE-2.0.txt](http://img.shields.io/:license-Apache%202-brightgreen.svg)
[![npm](https://img.shields.io/npm/v/@databricks/sql?color=blue&style=flat)](https://www.npmjs.com/package/@databricks/sql)
[![test](https://github.com/databricks/databricks-sql-nodejs/workflows/test/badge.svg?branch=main)](https://github.com/databricks/databricks-sql-nodejs/actions?query=workflow%3Atest+branch%3Amain)
[![coverage](https://codecov.io/gh/databricks/databricks-sql-nodejs/branch/main/graph/badge.svg)](https://codecov.io/gh/databricks/databricks-sql-nodejs)

## Description

The Databricks SQL Driver for Node.js is a Javascript driver for applications that connect to Databricks clusters and SQL warehouses. This project is a fork of [Hive Driver](https://github.com/lenchv/hive-driver) which connects via Thrift API.

## Requirements

- Node.js 14 or newer

## Installation

```bash
npm i @databricks/sql
```

## Usage

[examples/usage.js](examples/usage.js)

```javascript
const { DBSQLClient } = require('@databricks/sql');

const client = new DBSQLClient();

client
  .connect({
    host: '********.databricks.com',
    path: '/sql/2.0/warehouses/****************',
    token: 'dapi********************************',
  })
  .then(async (client) => {
    const session = await client.openSession();

    const queryOperation = await session.executeStatement('SELECT "Hello, World!"');
    const result = await queryOperation.fetchAll();
    await queryOperation.close();

    console.table(result);

    await session.close();
    await client.close();
  })
  .catch((error) => {
    console.log(error);
  });
```

## Telemetry

Starting with version 1.13, the driver collects telemetry — connection,
statement, and CloudFetch chunk metrics, plus error events with redacted
stack traces — to help Databricks improve driver performance and
reliability. **Telemetry is enabled by default and gated by a server-side
feature flag**: events are emitted only when the workspace's feature flag
is on. No SQL text, parameter values, or row data are ever included.

### What's collected

- Connection lifecycle (`CREATE_SESSION`, `DELETE_SESSION`) with latency.
- Statement lifecycle (`STATEMENT_START`, `STATEMENT_COMPLETE`) with
  execution latency, operation type, and result format.
- CloudFetch chunk timings and byte counts.
- Error events with redacted stack traces (Bearer/JWT tokens, OAuth
  secrets, home-directory paths, and Databricks PATs are stripped before
  emission).

See `TelemetryEvent` and `TelemetryMetric` in the package exports for the
exact payload shapes.

### Multi-tenant SaaS deployments — read this before enabling telemetry

The telemetry layer shares one per-host `TelemetryClient` across every
`DBSQLClient` connected to the same Databricks workspace host. The
authenticated export path uses the **first-registered** client's auth
provider, User-Agent, and `telemetryAuthenticatedExport` value — these
fields are snapshotted at the host singleton and are **not** per-tenant.

If you are operating a SaaS layer that fronts multiple tenants against the
same Databricks workspace host with a shared driver process, telemetry from
tenant B's queries can be POSTed under tenant A's auth headers, with
tenant A's `userAgentEntry`. A tenant B that explicitly set
`telemetryAuthenticatedExport: false` will still ride tenant A's
authenticated pipeline.

> **Recommendation for multi-tenant deployments**: set
> `telemetryEnabled: false` on all `DBSQLClient` instances, or partition
> by Databricks workspace host so each tenant owns its own
> `TelemetryClient`. Subsequent registrants with diverging auth/UA values
> emit a warn-level log so the leak is at least visible.

### Opting out

Three independent ways to disable telemetry, in order of precedence:

1. **Environment variable** — set `DATABRICKS_TELEMETRY_DISABLED` to one
   of `1`, `true`, `yes`, or `on` (case-insensitive). Other values
   (empty, `0`, `false`, `off`, `no`) are ignored, leaving the runtime
   config in charge.
2. **Programmatic** — pass `telemetryEnabled: false` to `connect()`:
   ```javascript
   await client.connect({
     host,
     path,
     token,
     telemetryEnabled: false,
   });
   ```
3. **Server-side** — Databricks-managed feature flag; if disabled for
   your workspace, the driver does not emit telemetry regardless of
   client config.

### Tuning

If you keep telemetry on, the following knobs are available on
`ConnectionOptions` (see JSDoc on `IDBSQLClient.ts` for defaults and
units):

- `telemetryAuthenticatedExport` — set to `false` to ship reduced
  payloads (no statement/session correlation IDs, generic User-Agent)
  via the unauthenticated endpoint.
- `telemetryBatchSize`, `telemetryFlushIntervalMs`, `telemetryMaxRetries`
  — batching and retry tuning.
- `telemetryCircuitBreakerThreshold`, `telemetryCircuitBreakerTimeout` —
  circuit-breaker tuning for the export endpoint.
- `telemetryCloseTimeoutMs` — bound on `await client.close()` waiting for
  the final flush.

> **Note for short-lived processes**: always `await client.close()`
> before `process.exit(0)` so the final batch is flushed. Without an
> explicit close, the periodic flush timer is `unref()`'d to avoid
> holding the event loop open, so any unflushed events are dropped.

## Run Tests

### Unit tests

You can run all unit tests, or specify a specific test to run:

```bash
npm test
npm test -- <path/to/file.test.js>
```

### e2e tests

Before running end-to-end tests, create a file named `tests/e2e/utils/config.local.js` and set the Databricks SQL connection info:

```javascript
{
    host: '***.databricks.com',
    path: '/sql/2.0/warehouses/***',
    token: 'dapi***',
    database: ['catalog', 'database'],
}
```

Then run

```bash
npm run e2e
npm run e2e -- <path/to/file.test.js>
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## Issues

If you find any issues, feel free to create an issue or send a pull request directly.

## License

[Apache License 2.0](LICENSE)
