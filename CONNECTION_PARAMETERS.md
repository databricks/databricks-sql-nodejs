# Connection parameter reference

This document lists **every public connection / session parameter** the
Node.js connector accepts, and — because the driver ships two backends —
whether each parameter is honored on the **Thrift** backend (the default),
the **SEA / Kernel** backend (opt-in via `useKernel: true`), or both.

The goal is to make protocol gaps explicit: a parameter honored on one
backend but ignored (or rejected) on the other is called out in the **Note**
column.

> **Backend selection.** The connector defaults to Thrift. The SEA backend is
> selected by passing `useKernel: true`, an **internal, unstable (M0)** option
> that is intentionally absent from the published `.d.ts` and may be removed
> without notice (`lib/contracts/InternalConnectionOptions.ts`). Treat every
> "Kernel" column below as describing an experimental path.

## Legend

| Symbol | Meaning                                                             |
| ------ | ------------------------------------------------------------------- |
| ✅     | Honored — the option is read and forwarded to the backend.          |
| ❌     | Ignored or rejected — see the Note column.                          |
| ⚠️     | Partially supported or behaves differently from the other backend.  |
| —      | Not applicable / no public equivalent / no default on this backend. |

## Sources of truth

- Public option shape ← `lib/contracts/IDBSQLClient.ts` (`ConnectionOptions`,
  `AuthOptions`, `OpenSessionRequest`) and `lib/contracts/IDBSQLSession.ts`
  (`ExecuteStatementOptions`).
- Default values ← `DBSQLClientDefaults` (`lib/DBSQLClient.ts`) and
  `DEFAULT_TELEMETRY_CONFIG` (`lib/telemetry/types.ts`).
- Internal / kernel-only flags ← `lib/contracts/InternalConnectionOptions.ts`.
- Thrift wiring ← `lib/DBSQLClient.ts` (`getConnectionOptions`,
  `createAuthProvider`), `lib/thrift-backend/ThriftBackend.ts`,
  `lib/connection/connections/HttpConnection.ts`.
- Kernel wiring ← `lib/kernel/KernelAuth.ts` (`buildKernelConnectionOptions`,
  `buildKernelTlsOptions`, `buildKernelHttpOptions`, `buildKernelProxyOptions`,
  `buildKernelRetryOptions`), `lib/kernel/KernelBackend.ts`,
  `lib/kernel/KernelSessionBackend.ts`.
- Kernel-core parameter semantics ← databricks-sql-kernel
  [`docs/connection-parameters.md`](https://github.com/databricks/databricks-sql-kernel/pull/184).

---

## Connection identity

| Option           | Type     | Thrift | Kernel | Default Value | Note                                                                                                                                                   |
| ---------------- | -------- | :----: | :----: | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host`           | `string` |   ✅   |   ✅   | — (required)  | Required on both.                                                                                                                                      |
| `path`           | `string` |   ✅   |   ✅   | — (required)  | HTTP path; on the kernel path the org id is auto-parsed from a `?o=<id>` query param and sent as `x-databricks-org-id`.                                |
| `port`           | `number` |   ✅   |   ⚠️   | `443`         | Thrift defaults to `443` (`options.port \|\| 443`). The kernel derives host/port from `host` + `path`; a standalone `port` is not separately threaded. |
| `userAgentEntry` | `string` |   ✅   |   ✅   | —             | Folded into the composed `User-Agent` on both.                                                                                                         |

## Authentication

| Option                                                                       | Type                 |  Thrift  |  Kernel  | Default Value                                      | Note                                                                                                                                                                                                                                                                                                                                                  |
| ---------------------------------------------------------------------------- | -------------------- | :------: | :------: | -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `authType: 'access-token'` + `token` (PAT)                                   | `token: string`      |    ✅    |    ✅    | `authType` ⇒ `'access-token'`                      | Default auth mode when `authType` is omitted.                                                                                                                                                                                                                                                                                                         |
| `authType: 'databricks-oauth'` — M2M (`oauthClientId` + `oauthClientSecret`) | `string` + `string`  |    ✅    |    ✅    | —                                                  | Kernel runs OIDC discovery + client-credentials internally.                                                                                                                                                                                                                                                                                           |
| `authType: 'databricks-oauth'` — U2M (browser)                               | —                    |    ✅    |    ⚠️    | —                                                  | See U2M rows below.                                                                                                                                                                                                                                                                                                                                   |
| `oauthScopes`                                                                | `Array<string>`      |    ❌    |    ✅    | U2M `['sql','offline_access']`, M2M `['all-apis']` | **Thrift ignores `oauthScopes`** — `createAuthProvider` never threads it into `DatabricksOAuth`, so `authenticate()` always falls back to `defaultOAuthScopes` (`['sql','offline_access']`). Only the kernel honors a custom `oauthScopes`; its defaults happen to match Thrift's fallback.                                                           |
| `oauthClientId` (U2M)                                                        | `string`             |    ✅    |    ✅    | napi default `client_id` when absent               | The kernel adapter (`buildKernelConnectionOptions`) forwards a custom `oauthClientId` verbatim on the U2M arm; when it is absent the napi binding applies its own default `client_id`. Whether the native binding then honors or rejects a custom id is not observable from this repo — the TypeScript layer neither hardcodes an id nor rejects one. |
| `oauthClientId` + no secret                                                  | `string`             | ✅ (U2M) | ✅ (U2M) | —                                                  | **Parity.** The kernel keys flow selection off `oauthClientSecret` presence exactly like Thrift, so `oauthClientId` + no secret routes to **U2M** (with the id forwarded) — it does **not** throw an M2M "secret required" error.                                                                                                                     |
| `azureTenantId` / `useDatabricksOAuthInAzure`                                | `string` / `boolean` |    ✅    |    ❌    | —                                                  | **Thrift-only.** Kernel rejects Azure-direct (Entra) OAuth; workspace-OIDC discovery covers Azure workspaces without it.                                                                                                                                                                                                                              |
| `persistence` (custom OAuth token store)                                     | `OAuthPersistence`   |    ✅    |    ❌    | —                                                  | **Thrift-only.** Kernel throws; it auto-persists U2M tokens to `~/.config/databricks-sql-kernel/oauth/` and does not cache M2M.                                                                                                                                                                                                                       |
| `authType: 'custom'` (`provider`)                                            | `IAuthentication`    |    ✅    |    ❌    | —                                                  | **Thrift-only.** Kernel supports only `access-token` and `databricks-oauth`.                                                                                                                                                                                                                                                                          |
| `authType: 'token-provider'` (`tokenProvider`)                               | `ITokenProvider`     |    ✅    |    ❌    | —                                                  | **Thrift-only.**                                                                                                                                                                                                                                                                                                                                      |
| `authType: 'external-token'` (`getToken`)                                    | `TokenCallback`      |    ✅    |    ❌    | —                                                  | **Thrift-only.**                                                                                                                                                                                                                                                                                                                                      |
| `authType: 'static-token'` (`staticToken`)                                   | `string`             |    ✅    |    ❌    | —                                                  | **Thrift-only.**                                                                                                                                                                                                                                                                                                                                      |
| `enableTokenFederation` / `federationClientId`                               | `boolean` / `string` |    ✅    |    ❌    | `false` / —                                        | **Thrift-only** (available on the token-provider / external-token / static-token arms, none of which the kernel supports).                                                                                                                                                                                                                            |

## HTTP client, proxy, retries

| Option                       | Type                     | Thrift | Kernel | Default Value     | Note                                                                                                                                                                                                                        |
| ---------------------------- | ------------------------ | :----: | :----: | ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `socketTimeout`              | `number` (ms)            |   ✅   |   ⚠️   | `900000` (15 min) | Kernel maps it to the request timeout but forwards **only positive** values — `socketTimeout: 0` (Thrift "wait indefinitely") is omitted so the kernel keeps its large default.                                             |
| `proxy`                      | `ProxyOptions`           |   ✅   |   ⚠️   | —                 | `{protocol, host, port, auth}`. Kernel accepts **`http://` / `https://` only**; a SOCKS `protocol` surfaces a kernel connect error (Thrift supports SOCKS variants).                                                        |
| `noProxy` (internal)         | `string`                 |   ❌   |   ✅   | —                 | **Thrift ignores `noProxy`** — `getConnectionOptions` never threads it, and `getProxyForUrl: () => proxyUrl` returns the proxy for every URL (no bypass-list logic). Only the kernel honors it, forwarded as `bypassHosts`. |
| `customHeaders`              | `Record<string, string>` |   ✅   |   ✅   | —                 | Kernel drops reserved `Authorization` / `x-databricks-org-id`, rejects CR/LF/NUL, and appends the connector `User-Agent` last.                                                                                              |
| `retryMaxAttempts`           | `number`                 |   ✅   |   ✅   | `5`               | Total-attempt semantics on both; kernel converts to retries-after-first.                                                                                                                                                    |
| `retriesTimeout`             | `number` (ms)            |   ✅   |   ✅   | `900000` (15 min) | Kernel converts ms → whole seconds.                                                                                                                                                                                         |
| `retryDelayMin`              | `number` (ms)            |   ✅   |   ✅   | `1000` (1 s)      | Kernel converts ms → seconds.                                                                                                                                                                                               |
| `retryDelayMax`              | `number` (ms)            |   ✅   |   ✅   | `60000` (60 s)    | Kernel converts ms → seconds.                                                                                                                                                                                               |
| `maxConnections` (pool size) | `number` (internal)      |   ❌   |   ✅   | kernel default    | **Kernel-only** (`InternalConnectionOptions`). Thrift has no connection pool.                                                                                                                                               |

## TLS / SSL

> **Thrift TLS is public and secure-by-default.** `checkServerCertificate`,
> `customCaCert`, `clientCert`, and `clientKey` are declared on the public
> `ConnectionOptions` (`lib/contracts/IDBSQLClient.ts`) and honored on the
> Thrift (default) backend. `getConnectionOptions` (`lib/DBSQLClient.ts`) maps
> `customCaCert` → `ca` (**additively**, on top of `tls.rootCertificates` +
> `NODE_EXTRA_CA_CERTS`), `clientCert` → `cert`, and `clientKey` → `key`, with a
> both-or-neither mTLS guard and `normalizePemBytes` PEM validation. It sets
> `rejectUnauthorized: options.checkServerCertificate ?? true`, and
> `HttpConnection` reads `this.options.rejectUnauthorized ?? true` — i.e.
> **verification is on by default**; you must pass `checkServerCertificate:
false` to accept any certificate.

| Option                           | Type               | Thrift | Kernel | Default Value | Note                                                                                                                                    |
| -------------------------------- | ------------------ | :----: | :----: | ------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `checkServerCertificate`         | `boolean`          |   ✅   |   ✅   | `true`        | Master verify toggle on both, secure-by-default: Thrift uses `options.checkServerCertificate ?? true`. Set `false` to accept-anything.  |
| `checkServerCertificateHostname` | `boolean` (kernel) |   ❌   |   ✅   | `true`        | **Kernel-only.** Independent hostname-vs-SNI check; no-op when `checkServerCertificate: false`. No Thrift equivalent.                   |
| `customCaCert`                   | `Buffer \| string` |   ✅   |   ✅   | —             | PEM. Honored on both; added on top of system roots. Thrift maps it to `ca` additively (`tls.rootCertificates` + `NODE_EXTRA_CA_CERTS`). |
| `clientCert` (mTLS)              | `Buffer \| string` |   ✅   |   ✅   | —             | Honored on both; maps to `cert`. Must be paired with `clientKey`; supplying one alone throws a client-side error.                       |
| `clientKey` (mTLS)               | `Buffer \| string` |   ✅   |   ✅   | —             | Honored on both; maps to `key`. Must be paired with `clientCert`. PKCS#8 recommended.                                                   |

## Results & type rendering

| Option                        | Type      | Thrift | Kernel | Default Value | Note                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ----------------------------- | --------- | :----: | :----: | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `preserveBigNumericPrecision` | `boolean` |   ✅   |   ✅   | `false`       | DECIMAL → exact string, BIGINT → `bigint` on both.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `enableMetricViewMetadata`    | `boolean` |   ✅   |   ⚠️   | `false`       | **Auto-injected for both backends** in `DBSQLClient.openSession`, which sets `spark.sql.thriftserver.metadata.metricview.enabled=true` on `request.configuration` before dispatch. `KernelBackend` folds that into `sessionOptions.sessionConf`, so the conf **does** reach the kernel session config. (`ThriftBackend.ts` performs a second, redundant injection on the Thrift path.) The kernel-side gap is that the key is a non-allowlisted session conf, so it is likely dropped by the kernel's case-insensitive allowlist (see "Session defaults") — not that it is never injected. |

## Session defaults (`openSession(request)`)

| Option                          | Type                                          | Thrift | Kernel | Default Value | Note                                                                                                                                                                           |
| ------------------------------- | --------------------------------------------- | :----: | :----: | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `initialCatalog`                | `string`                                      |   ✅   |   ✅   | —             | Kernel → `DefaultOpts.catalog` on `CreateSession`.                                                                                                                             |
| `initialSchema`                 | `string`                                      |   ✅   |   ✅   | —             | Kernel → `DefaultOpts.schema`.                                                                                                                                                 |
| `configuration` (session confs) | `{ [key: string]: string }`                   |   ✅   |   ⚠️   | —             | Kernel matches keys **case-insensitively against an allowlist** and uppercases them; **non-allowlisted keys are dropped with a warning**. Thrift forwards the map more freely. |
| `queryTags`                     | `Record<string, string \| null \| undefined>` |   ✅   |   ✅   | —             | Both serialize into the reserved `QUERY_TAGS` conf; `queryTags` takes precedence over `configuration.QUERY_TAGS`.                                                              |

## Telemetry

All `telemetry*` options live in the driver-layer `ClientConfig`, not in either
backend, so they are read regardless of `useKernel`. Defaults are sourced from
`DEFAULT_TELEMETRY_CONFIG` (`lib/telemetry/types.ts`).

| Option                             | Type          | Thrift | Kernel | Default Value | Note                                                |
| ---------------------------------- | ------------- | :----: | :----: | ------------- | --------------------------------------------------- |
| `telemetryEnabled`                 | `boolean`     |   ✅   |   ✅   | `true`        | Enabled by default, gated by a server feature flag. |
| `telemetryBatchSize`               | `number`      |   ✅   |   ✅   | `100`         | Metrics per export batch.                           |
| `telemetryFlushIntervalMs`         | `number` (ms) |   ✅   |   ✅   | `5000`        | Periodic flush interval.                            |
| `telemetryMaxRetries`              | `number`      |   ✅   |   ✅   | `3`           | Export retry attempts.                              |
| `telemetryAuthenticatedExport`     | `boolean`     |   ✅   |   ✅   | `true`        | Export via the authenticated endpoint.              |
| `telemetryCircuitBreakerThreshold` | `number`      |   ✅   |   ✅   | `5`           | Consecutive failures before the breaker opens.      |
| `telemetryCircuitBreakerTimeout`   | `number` (ms) |   ✅   |   ✅   | `60000`       | Breaker open duration.                              |
| `telemetryCloseTimeoutMs`          | `number` (ms) |   ✅   |   ✅   | `2000`        | Caps `client.close()` shutdown latency.             |
| `telemetryMaxStatementMetrics`     | `number`      |   ✅   |   ✅   | `5000`        | Hard cap for the per-statement aggregation map.     |
| `telemetryMaxPendingMetrics`       | `number`      |   ✅   |   ✅   | `500`         | Cap on buffered, not-yet-exported metrics.          |

> **Telemetry _events_ differ by backend.** The config knobs above are
> backend-agnostic (driver layer), but the kernel owns result fetching
> internally, so it emits fewer per-statement / CloudFetch telemetry events than
> the Thrift path.

## Per-statement options (`session.executeStatement(sql, options)`)

| Option                                 | Type                                | Thrift | Kernel | Default Value | Note                                                                                                                                       |
| -------------------------------------- | ----------------------------------- | :----: | :----: | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `maxRows`                              | `number \| bigint \| Int64 \| null` |   ✅   |   ✅   | `100000`      | Kernel applies it at fetch time in the facade rather than on the request.                                                                  |
| `useCloudFetch`                        | `boolean`                           |   ✅   |   ❌   | `true`        | **Thrift-only.** Kernel ignores it (logs a no-op warning); CloudFetch is governed by the kernel's result configuration, not per-statement. |
| `useLZ4Compression`                    | `boolean`                           |   ✅   |   ❌   | `true`        | **Thrift-only.** Kernel ignores it (no-op warning); the kernel auto-detects and decompresses `LZ4_FRAME` from the server result manifest.  |
| `stagingAllowedLocalPath` (volume ops) | `string \| string[]`                |   ✅   |   ❌   | —             | **Thrift-only.** Not supported on the kernel path.                                                                                         |
| `runAsync`                             | `boolean`                           |   ✅   |   ⚠️   | `false`       | Deprecated; not threaded on the kernel path.                                                                                               |

---

## Summary of gaps

### Supported on Thrift, missing / ignored on Kernel

1. `enableMetricViewMetadata` — auto-injected for both backends in
   `DBSQLClient.openSession`, but the conf key is likely dropped by the
   kernel's session-conf allowlist, so it has no effect on the kernel path.
2. Auth types `custom`, `token-provider`, `external-token`, `static-token`,
   plus `enableTokenFederation` / `federationClientId`.
3. `azureTenantId` / `useDatabricksOAuthInAzure` (Azure-direct OAuth).
4. `persistence` (custom OAuth token store).
5. SOCKS proxies.
6. Per-statement `useCloudFetch`, `useLZ4Compression`,
   `stagingAllowedLocalPath`.

### Supported on Kernel, no Thrift public equivalent

1. `maxConnections` (connection-pool sizing).
2. `checkServerCertificateHostname` — the independent hostname-vs-SNI check has
   no public Thrift equivalent. (The other TLS controls —
   `checkServerCertificate`, `customCaCert`, `clientCert`, `clientKey` — **are**
   public and honored on the Thrift backend, which verifies certificates by
   default via `checkServerCertificate ?? true`; see the TLS / SSL section.)

### Behavioral divergences to watch

- **U2M flow selection** keys off `oauthClientSecret` presence on the kernel
  path, matching Thrift: no secret ⇒ U2M, secret present ⇒ M2M. A custom
  `oauthClientId` (with no secret) is forwarded on the U2M arm rather than
  triggering an M2M "secret required" error.
- **`socketTimeout: 0`** means "indefinite" on Thrift but is dropped on the
  kernel path (kernel default kept).
- **`configuration`** is allowlist-filtered on the kernel path but forwarded
  more freely on Thrift.

> All kernel-path behavior reflects the **M0 stub** and is subject to change.
