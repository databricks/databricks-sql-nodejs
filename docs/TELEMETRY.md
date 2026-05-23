# Telemetry

The driver emits anonymous usage and performance metrics to Databricks to help track driver
adoption, identify performance regressions, and prioritize fixes. Telemetry is **enabled by
default** and is additionally gated by a per-workspace server-side feature flag, so events are
only exported when the workspace has telemetry turned on. No SQL text, parameter values, row
data, table/column names, credentials, or IP addresses are ever collected.

## What's collected

Events are batched per host and exported to the Databricks control plane over HTTPS using the
same auth as your queries.

- **Connection** (`connection.open`): driver version and name, Node.js version, OS platform/
  version, and boolean feature toggles (CloudFetch, LZ4, Arrow, direct results) plus numeric
  configs (socket timeout, retry max, CloudFetch concurrency).
- **Statement** (`statement.start` / `statement.complete`): randomly generated statement and
  session UUIDs, operation type (e.g. `SELECT`), latency, result format, poll count, chunk
  count, bytes downloaded.
- **CloudFetch chunk** (`cloudfetch.chunk`): chunk index, download latency, byte size,
  compressed flag.
- **Error**: error class name, sanitized message (no PII), HTTP status, terminal-vs-retryable
  flag. Stack traces are not transmitted.

Correlation IDs (session ID, statement ID) are random UUIDs and are not tied to user identity.
Workspace ID is included for aggregation.

## Configuration

Options are passed to `new DBSQLClient({...})` (and can be overridden per `connect()` call).
See the JSDoc on `IDBSQLClientConnectionOptions` in
[`lib/contracts/IDBSQLClient.ts`](../lib/contracts/IDBSQLClient.ts) for the authoritative
defaults and full descriptions.

| Option | Purpose |
| --- | --- |
| `telemetryEnabled` | Master switch. `false` is a hard opt-out; `true` requests telemetry (still subject to the server flag). |
| `telemetryAuthenticatedExport` | When `true`, exports go to the authenticated `/telemetry-ext` endpoint with full event context. When `false`, only error names go to the unauthenticated endpoint. |
| `telemetryBatchSize` | Events accumulated before a flush. |
| `telemetryFlushIntervalMs` | Periodic flush interval. |
| `telemetryMaxRetries` | Retries per failed export. |
| `telemetryCircuitBreakerThreshold` | Consecutive failures before the per-host breaker opens. |
| `telemetryCircuitBreakerTimeout` | How long the breaker stays open before re-probing. |
| `telemetryCloseTimeoutMs` | Upper bound on the final flush during `client.close()`. |

### Basic example

```javascript
const { DBSQLClient } = require('@databricks/sql');

const client = new DBSQLClient();
await client.connect({
  host: '********.databricks.com',
  path: '/sql/2.0/warehouses/****************',
  token: 'dapi********************************',
});
```

### Disabling telemetry

```javascript
const client = new DBSQLClient({ telemetryEnabled: false });
```

## Opt-out

Three independent ways to disable, in order of precedence (first match wins):

1. **Environment variable**: `DATABRICKS_TELEMETRY_DISABLED` set to `1`, `true`, `yes`, or
   `on` (case-insensitive) disables telemetry process-wide regardless of any other setting.
2. **Programmatic**: `telemetryEnabled: false` in `DBSQLClient` or `connect()` options is a
   hard opt-out for that client.
3. **Server feature flag**: If the workspace's server-side flag is off, no events are exported
   even when the client requests them.

## Multi-tenant / SaaS warning

The driver maintains a singleton telemetry client per host (shared across all `DBSQLClient`
instances pointing at the same workspace) to batch events and avoid rate limits. In a
multi-tenant process where multiple tenants connect to the same host with different
credentials, events buffered for tenant A may be flushed using whichever connection happens to
own the authenticated export at the time. Tenant B's auth headers could carry tenant A's
telemetry payload.

If you run a multi-tenant SaaS that proxies queries from distinct end-customers through one
Node process to the same Databricks host, set `telemetryEnabled: false` (or
`telemetryAuthenticatedExport: false`) to prevent cross-tenant attribution in telemetry.

## Troubleshooting

- **No events visible**: confirm `telemetryEnabled` is not `false`, `DATABRICKS_TELEMETRY_DISABLED`
  is unset, and the workspace feature flag is on. Look for the debug log
  `Telemetry disabled via feature flag`.
- **Events suddenly stop**: the per-host circuit breaker has likely opened after repeated
  export failures. Look for `Circuit breaker transitioned to OPEN`; it re-probes automatically
  after `telemetryCircuitBreakerTimeout` (default 60s).
- **Buffer pressure / dropped metrics**: check `client.getTelemetryStats().droppedMetrics`. If
  it climbs, increase `telemetryMaxPendingMetrics` or lower `telemetryFlushIntervalMs`.
- **Shutdown delay**: `client.close()` waits up to `telemetryCloseTimeoutMs` (default 2s) for
  the final flush. Lower it if shutdown latency matters more than the last batch.
- **Telemetry failures impacting the app**: they shouldn't. Exceptions are caught and logged
  at debug only; the driver continues regardless. File an issue if you see otherwise.

## FAQ

**Does telemetry affect query performance?** Event emission is non-blocking and exports are
batched on a background timer. Overhead is well under 1% of query time in typical workloads.

**Can I see what's being sent?** Yes, enable debug-level logging on the driver's logger.
Every export and circuit-breaker transition is logged.

**Where does the data go?** To `/api/2.0/sql/telemetry-ext` (authenticated) or
`/api/2.0/sql/telemetry-unauth` on the same Databricks host you're connected to. It stays in
the same regional control plane as your queries.

**Can I route telemetry to my own backend?** Not via configuration. Disable it and instrument
your application using your own logger/metrics.

**Can I disable telemetry for a single query?** No, the granularity is per-connection. Open a
separate `DBSQLClient` with `telemetryEnabled: false` for the queries you want excluded.

For implementation details (per-host management, circuit breaker state machine, exception
handling policy), see [`spec/telemetry-design.md`](../spec/telemetry-design.md).
