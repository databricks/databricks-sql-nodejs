# Connection parameter reference

This document lists **every public connection / session parameter** the
Node.js connector accepts, and — because the driver ships two backends —
whether each parameter is honored on the **Thrift** backend (the default),
the **SEA / Kernel** backend (opt-in via `useKernel: true`), or both.

The goal is to make protocol gaps explicit: a parameter honored on one
backend but ignored (or rejected) on the other is called out in the **Gap**
column.

> **Backend selection.** The connector defaults to Thrift. The SEA backend is
> selected by passing `useKernel: true`, an **internal, unstable (M0)** option
> that is intentionally absent from the published `.d.ts` and may be removed
> without notice (`lib/contracts/InternalConnectionOptions.ts`). Treat every
> "Kernel" column below as describing an experimental path.

## Legend

| Symbol | Meaning                                                            |
| ------ | ------------------------------------------------------------------ |
| ✅     | Honored — the option is read and forwarded to the backend.         |
| ❌     | Ignored or rejected — see the Gap column.                          |
| ⚠️     | Partially supported or behaves differently from the other backend. |
| —      | Not applicable / no public equivalent on this backend.             |

## Sources of truth

- Public option shape ← `lib/contracts/IDBSQLClient.ts` (`ConnectionOptions`,
  `AuthOptions`, `OpenSessionRequest`).
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

| Option           | Type     | Thrift | Kernel | Gap                                                                                                                          |
| ---------------- | -------- | :----: | :----: | ---------------------------------------------------------------------------------------------------------------------------- |
| `host`           | `string` |   ✅   |   ✅   | Required on both.                                                                                                            |
| `path`           | `string` |   ✅   |   ✅   | HTTP path; on the kernel path the org id is auto-parsed from a `?o=<id>` query param and sent as `x-databricks-org-id`.      |
| `port`           | `number` |   ✅   |   ⚠️   | Thrift defaults to `443`. The kernel derives host/port from `host` + `path`; a standalone `port` is not separately threaded. |
| `userAgentEntry` | `string` |   ✅   |   ✅   | Folded into the composed `User-Agent` on both.                                                                               |

## Authentication

| Option                                                                       |  Thrift  | Kernel | Gap                                                                                                                                                 |
| ---------------------------------------------------------------------------- | :------: | :----: | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `authType: 'access-token'` + `token` (PAT)                                   |    ✅    |   ✅   |                                                                                                                                                     |
| `authType: 'databricks-oauth'` — M2M (`oauthClientId` + `oauthClientSecret`) |    ✅    |   ✅   | Kernel runs OIDC discovery + client-credentials internally.                                                                                         |
| `authType: 'databricks-oauth'` — U2M (browser)                               |    ✅    |   ⚠️   | See U2M gaps below.                                                                                                                                 |
| `oauthScopes`                                                                |    ❌    |   ✅   | **Thrift ignores `oauthScopes`** — `createAuthProvider` never threads it into `DatabricksOAuth`, so `authenticate()` always falls back to `defaultOAuthScopes` (`['sql','offline_access']`). Only the kernel honors a custom `oauthScopes`; its defaults happen to match Thrift's fallback (U2M = `['sql','offline_access']`, M2M = `['all-apis']`). |
| `oauthClientId` (U2M)                                                        |    ✅    |   ✅   | The kernel adapter (`buildKernelConnectionOptions`) forwards a custom `oauthClientId` verbatim on the U2M arm; when it is absent the napi binding applies its own default `client_id`. Whether the native binding then honors or rejects a custom id is not observable from this repo — the TypeScript layer neither hardcodes an id nor rejects one. |
| `oauthClientId` + no secret                                                  | ✅ (U2M) | ✅ (U2M) | **Parity.** The kernel keys flow selection off `oauthClientSecret` presence exactly like Thrift, so `oauthClientId` + no secret routes to **U2M** (with the id forwarded) — it does **not** throw an M2M "secret required" error. |
| `azureTenantId` / `useDatabricksOAuthInAzure`                                |    ✅    |   ❌   | **Thrift-only.** Kernel rejects Azure-direct (Entra) OAuth; workspace-OIDC discovery covers Azure workspaces without it.                            |
| `persistence` (custom OAuth token store)                                     |    ✅    |   ❌   | **Thrift-only.** Kernel throws; it auto-persists U2M tokens to `~/.config/databricks-sql-kernel/oauth/` and does not cache M2M.                     |
| `authType: 'custom'` (`provider`)                                            |    ✅    |   ❌   | **Thrift-only.** Kernel supports only `access-token` and `databricks-oauth`.                                                                        |
| `authType: 'token-provider'` (`tokenProvider`)                               |    ✅    |   ❌   | **Thrift-only.**                                                                                                                                    |
| `authType: 'external-token'` (`getToken`)                                    |    ✅    |   ❌   | **Thrift-only.**                                                                                                                                    |
| `authType: 'static-token'` (`staticToken`)                                   |    ✅    |   ❌   | **Thrift-only.**                                                                                                                                    |
| `enableTokenFederation` / `federationClientId`                               |    ✅    |   ❌   | **Thrift-only** (available on the token-provider / external-token / static-token arms, none of which the kernel supports).                          |

## HTTP client, proxy, retries

| Option                                   | Thrift | Kernel | Gap                                                                                                                                                                             |
| ---------------------------------------- | :----: | :----: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `socketTimeout`                          |   ✅   |   ⚠️   | Kernel maps it to the request timeout but forwards **only positive** values — `socketTimeout: 0` (Thrift "wait indefinitely") is omitted so the kernel keeps its large default. |
| `proxy` (`{protocol, host, port, auth}`) |   ✅   |   ⚠️   | Kernel accepts **`http://` / `https://` only**; a SOCKS `protocol` surfaces a kernel connect error (Thrift supports SOCKS variants).                                            |
| `noProxy` (internal)                     |   ❌   |   ✅   | **Thrift ignores `noProxy`** — `getConnectionOptions` never threads it, and `createProxyAgent` installs `getProxyForUrl: () => proxyUrl` (the proxy is returned for every URL, so there is no bypass-list logic). Only the kernel honors it, forwarded as `bypassHosts`. |
| `customHeaders`                          |   ✅   |   ✅   | Kernel drops reserved `Authorization` / `x-databricks-org-id`, rejects CR/LF/NUL, and appends the connector `User-Agent` last.                                                  |
| `retryMaxAttempts`                       |   ✅   |   ✅   | Total-attempt semantics on both; kernel converts to retries-after-first.                                                                                                        |
| `retriesTimeout`                         |   ✅   |   ✅   | Kernel converts ms → whole seconds.                                                                                                                                             |
| `retryDelayMin`                          |   ✅   |   ✅   | Kernel converts ms → seconds.                                                                                                                                                   |
| `retryDelayMax`                          |   ✅   |   ✅   | Kernel converts ms → seconds.                                                                                                                                                   |
| `maxConnections` (pool size)             |   ❌   |   ✅   | **Kernel-only** (`InternalConnectionOptions`). Thrift has no connection pool.                                                                                                   |

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
> false` to accept any certificate.

| Option                                 | Thrift | Kernel | Gap                                                                                                                                   |
| -------------------------------------- | :----: | :----: | ------------------------------------------------------------------------------------------------------------------------------------- |
| `checkServerCertificate`               |   ✅   |   ✅   | Master verify toggle on both, secure-by-default: Thrift uses `options.checkServerCertificate ?? true`. Set `false` to accept-anything. |
| `checkServerCertificateHostname`       |   ❌   |   ✅   | **Kernel-only.** Independent hostname-vs-SNI check; no-op when `checkServerCertificate: false`. No Thrift equivalent.                  |
| `customCaCert` (PEM string / `Buffer`) |   ✅   |   ✅   | Honored on both; added on top of system roots. Thrift maps it to `ca` additively (`tls.rootCertificates` + `NODE_EXTRA_CA_CERTS`).     |
| `clientCert` (mTLS)                    |   ✅   |   ✅   | Honored on both; maps to `cert`. Must be paired with `clientKey`; supplying one alone throws a client-side error.                      |
| `clientKey` (mTLS)                     |   ✅   |   ✅   | Honored on both; maps to `key`. Must be paired with `clientCert`. PKCS#8 recommended.                                                  |

## Results & type rendering

| Option                        | Thrift | Kernel | Gap                                                                                                                                                                                                                                                                                                                            |
| ----------------------------- | :----: | :----: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `preserveBigNumericPrecision` |   ✅   |   ✅   | DECIMAL → exact string, BIGINT → `bigint` on both.                                                                                                                                                                                                                                                                             |
| `enableMetricViewMetadata`    |   ✅   |   ⚠️   | **Auto-injected for both backends** in `DBSQLClient.openSession`, which sets `spark.sql.thriftserver.metadata.metricview.enabled=true` on `request.configuration` before dispatch. `KernelBackend` folds that into `sessionOptions.sessionConf`, so the conf **does** reach the kernel session config. (`ThriftBackend.ts` performs a second, redundant injection on the Thrift path.) The kernel-side gap is that the key is a non-allowlisted session conf, so it is likely dropped by the kernel's case-insensitive allowlist (see "Session defaults") — not that it is never injected. |

## Session defaults (`openSession(request)`)

| Option                          | Thrift | Kernel | Gap                                                                                                                                                                            |
| ------------------------------- | :----: | :----: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `initialCatalog`                |   ✅   |   ✅   | Kernel → `DefaultOpts.catalog` on `CreateSession`.                                                                                                                             |
| `initialSchema`                 |   ✅   |   ✅   | Kernel → `DefaultOpts.schema`.                                                                                                                                                 |
| `configuration` (session confs) |   ✅   |   ⚠️   | Kernel matches keys **case-insensitively against an allowlist** and uppercases them; **non-allowlisted keys are dropped with a warning**. Thrift forwards the map more freely. |
| `queryTags`                     |   ✅   |   ✅   | Both serialize into the reserved `QUERY_TAGS` conf; `queryTags` takes precedence over `configuration.QUERY_TAGS`.                                                              |

## Telemetry

All `telemetry*` options (`telemetryEnabled`, `telemetryBatchSize`,
`telemetryFlushIntervalMs`, `telemetryMaxRetries`,
`telemetryAuthenticatedExport`, `telemetryCircuitBreakerThreshold`,
`telemetryCircuitBreakerTimeout`, `telemetryCloseTimeoutMs`,
`telemetryMaxStatementMetrics`, `telemetryMaxPendingMetrics`) live in the
driver-layer `ClientConfig`, not in either backend, so they are read
regardless of `useKernel`.

| Aspect                                    | Thrift | Kernel | Gap                                                                                                                    |
| ----------------------------------------- | :----: | :----: | ---------------------------------------------------------------------------------------------------------------------- |
| Telemetry config knobs                    |   ✅   |   ✅   | Backend-agnostic (driver layer).                                                                                       |
| Statement / CloudFetch telemetry _events_ |   ✅   |   ⚠️   | The kernel owns result fetching internally, so it emits fewer per-statement / cloud-fetch events than the Thrift path. |

## Per-statement options (`session.executeStatement(sql, options)`)

| Option                                 | Thrift | Kernel | Gap                                                                                                                                        |
| -------------------------------------- | :----: | :----: | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `maxRows`                              |   ✅   |   ✅   | Kernel applies it at fetch time in the facade rather than on the request.                                                                  |
| `useCloudFetch`                        |   ✅   |   ❌   | **Thrift-only.** Kernel ignores it (logs a no-op warning); CloudFetch is governed by the kernel's result configuration, not per-statement. |
| `useLZ4Compression`                    |   ✅   |   ❌   | **Thrift-only.** Kernel ignores it (no-op warning); the kernel auto-detects and decompresses `LZ4_FRAME` from the server result manifest.  |
| `stagingAllowedLocalPath` (volume ops) |   ✅   |   ❌   | **Thrift-only.** Not supported on the kernel path.                                                                                         |
| `runAsync`                             |   ✅   |   ⚠️   | Deprecated; not threaded on the kernel path.                                                                                               |

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
