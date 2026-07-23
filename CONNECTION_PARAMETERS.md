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

| Symbol | Meaning |
|--------|---------|
| ✅ | Honored — the option is read and forwarded to the backend. |
| ❌ | Ignored or rejected — see the Gap column. |
| ⚠️ | Partially supported or behaves differently from the other backend. |
| — | Not applicable / no public equivalent on this backend. |

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

| Option | Type | Thrift | Kernel | Gap |
|--------|------|:------:|:------:|-----|
| `host` | `string` | ✅ | ✅ | Required on both. |
| `path` | `string` | ✅ | ✅ | HTTP path; on the kernel path the org id is auto-parsed from a `?o=<id>` query param and sent as `x-databricks-org-id`. |
| `port` | `number` | ✅ | ⚠️ | Thrift defaults to `443`. The kernel derives host/port from `host` + `path`; a standalone `port` is not separately threaded. |
| `userAgentEntry` | `string` | ✅ | ✅ | Folded into the composed `User-Agent` on both. |

## Authentication

| Option | Thrift | Kernel | Gap |
|--------|:------:|:------:|-----|
| `authType: 'access-token'` + `token` (PAT) | ✅ | ✅ | |
| `authType: 'databricks-oauth'` — M2M (`oauthClientId` + `oauthClientSecret`) | ✅ | ✅ | Kernel runs OIDC discovery + client-credentials internally. |
| `authType: 'databricks-oauth'` — U2M (browser) | ✅ | ⚠️ | See U2M gaps below. |
| `oauthScopes` | ✅ | ✅ | Kernel default U2M scopes = `['sql','offline_access']` (Thrift parity); M2M = `['all-apis']`. |
| `oauthClientId` (U2M) | ✅ | ❌ | **Kernel-side gap.** Kernel U2M hardcodes `client_id` and **rejects** a custom `oauthClientId`; Thrift honors it. |
| `oauthClientId` + no secret | ✅ (U2M) | ❌ | **Divergence.** Thrift routes to U2M with that id; kernel keys the flow off `oauthClientSecret` presence and throws an M2M "secret required" error. |
| `azureTenantId` / `useDatabricksOAuthInAzure` | ✅ | ❌ | **Thrift-only.** Kernel rejects Azure-direct (Entra) OAuth; workspace-OIDC discovery covers Azure workspaces without it. |
| `persistence` (custom OAuth token store) | ✅ | ❌ | **Thrift-only.** Kernel throws; it auto-persists U2M tokens to `~/.config/databricks-sql-kernel/oauth/` and does not cache M2M. |
| `authType: 'custom'` (`provider`) | ✅ | ❌ | **Thrift-only.** Kernel supports only `access-token` and `databricks-oauth`. |
| `authType: 'token-provider'` (`tokenProvider`) | ✅ | ❌ | **Thrift-only.** |
| `authType: 'external-token'` (`getToken`) | ✅ | ❌ | **Thrift-only.** |
| `authType: 'static-token'` (`staticToken`) | ✅ | ❌ | **Thrift-only.** |
| `enableTokenFederation` / `federationClientId` | ✅ | ❌ | **Thrift-only** (available on the token-provider / external-token / static-token arms, none of which the kernel supports). |

## HTTP client, proxy, retries

| Option | Thrift | Kernel | Gap |
|--------|:------:|:------:|-----|
| `socketTimeout` | ✅ | ⚠️ | Kernel maps it to the request timeout but forwards **only positive** values — `socketTimeout: 0` (Thrift "wait indefinitely") is omitted so the kernel keeps its large default. |
| `proxy` (`{protocol, host, port, auth}`) | ✅ | ⚠️ | Kernel accepts **`http://` / `https://` only**; a SOCKS `protocol` surfaces a kernel connect error (Thrift supports SOCKS variants). |
| `noProxy` (internal) | ✅ | ✅ | Forwarded to the kernel as `bypassHosts`. |
| `customHeaders` | ✅ | ✅ | Kernel drops reserved `Authorization` / `x-databricks-org-id`, rejects CR/LF/NUL, and appends the connector `User-Agent` last. |
| `retryMaxAttempts` | ✅ | ✅ | Total-attempt semantics on both; kernel converts to retries-after-first. |
| `retriesTimeout` | ✅ | ✅ | Kernel converts ms → whole seconds. |
| `retryDelayMin` | ✅ | ✅ | Kernel converts ms → seconds. |
| `retryDelayMax` | ✅ | ✅ | Kernel converts ms → seconds. |
| `maxConnections` (pool size) | ❌ | ✅ | **Kernel-only** (`InternalConnectionOptions`). Thrift has no connection pool. |

## TLS / SSL

> **Important Thrift caveat.** The public `connect()` surface on the Thrift
> backend does **not** expose any TLS customization. `getConnectionOptions`
> (`lib/DBSQLClient.ts`) maps only `host` / `port` / `path` / `socketTimeout` /
> `proxy` / `User-Agent`; the internal `IConnectionOptions.ca/cert/key` fields
> are never populated from a public option, and `HttpConnection` hardcodes
> `rejectUnauthorized: false`. All TLS-verification and custom-CA / mTLS
> controls below are therefore **kernel-only** in practice.

| Option | Thrift | Kernel | Gap |
|--------|:------:|:------:|-----|
| `checkServerCertificate` | ❌ | ✅ | **Kernel-only.** Master verify toggle (secure-by-default; set `false` for accept-anything). |
| `checkServerCertificateHostname` | ❌ | ✅ | **Kernel-only.** Independent hostname-vs-SNI check; no-op when `checkServerCertificate: false`. |
| `customCaCert` (PEM string / `Buffer`) | ❌ | ✅ | **Kernel-only.** Added on top of system roots. |
| `clientCertPem` (mTLS) | ❌ | ✅ | **Kernel-only.** Must be paired with `clientKeyPem`; supplying one alone is rejected. |
| `clientKeyPem` (mTLS) | ❌ | ✅ | **Kernel-only.** PKCS#8 recommended. |

## Results & type rendering

| Option | Thrift | Kernel | Gap |
|--------|:------:|:------:|-----|
| `preserveBigNumericPrecision` | ✅ | ✅ | DECIMAL → exact string, BIGINT → `bigint` on both. |
| `enableMetricViewMetadata` | ✅ | ❌ | **Thrift-only in the connector.** Thrift auto-injects `spark.sql.thriftserver.metadata.metricview.enabled=true` (`ThriftBackend.ts`); `KernelBackend` does **not** auto-inject it. The kernel core *can* accept the raw conf key via `session_conf`, so a caller could pass it manually in `OpenSessionRequest.configuration`. |

## Session defaults (`openSession(request)`)

| Option | Thrift | Kernel | Gap |
|--------|:------:|:------:|-----|
| `initialCatalog` | ✅ | ✅ | Kernel → `DefaultOpts.catalog` on `CreateSession`. |
| `initialSchema` | ✅ | ✅ | Kernel → `DefaultOpts.schema`. |
| `configuration` (session confs) | ✅ | ⚠️ | Kernel matches keys **case-insensitively against an allowlist** and uppercases them; **non-allowlisted keys are dropped with a warning**. Thrift forwards the map more freely. |
| `queryTags` | ✅ | ✅ | Both serialize into the reserved `QUERY_TAGS` conf; `queryTags` takes precedence over `configuration.QUERY_TAGS`. |

## Telemetry

All `telemetry*` options (`telemetryEnabled`, `telemetryBatchSize`,
`telemetryFlushIntervalMs`, `telemetryMaxRetries`,
`telemetryAuthenticatedExport`, `telemetryCircuitBreakerThreshold`,
`telemetryCircuitBreakerTimeout`, `telemetryCloseTimeoutMs`,
`telemetryMaxStatementMetrics`, `telemetryMaxPendingMetrics`) live in the
driver-layer `ClientConfig`, not in either backend, so they are read
regardless of `useKernel`.

| Aspect | Thrift | Kernel | Gap |
|--------|:------:|:------:|-----|
| Telemetry config knobs | ✅ | ✅ | Backend-agnostic (driver layer). |
| Statement / CloudFetch telemetry *events* | ✅ | ⚠️ | The kernel owns result fetching internally, so it emits fewer per-statement / cloud-fetch events than the Thrift path. |

## Per-statement options (`session.executeStatement(sql, options)`)

| Option | Thrift | Kernel | Gap |
|--------|:------:|:------:|-----|
| `maxRows` | ✅ | ✅ | Kernel applies it at fetch time in the facade rather than on the request. |
| `useCloudFetch` | ✅ | ❌ | **Thrift-only.** Kernel ignores it (logs a no-op warning); CloudFetch is governed by the kernel's result configuration, not per-statement. |
| `useLZ4Compression` | ✅ | ❌ | **Thrift-only.** Kernel ignores it (no-op warning); the kernel auto-detects and decompresses `LZ4_FRAME` from the server result manifest. |
| `stagingAllowedLocalPath` (volume ops) | ✅ | ❌ | **Thrift-only.** Not supported on the kernel path. |
| `runAsync` | ✅ | ⚠️ | Deprecated; not threaded on the kernel path. |

---

## Summary of gaps

### Supported on Thrift, missing / ignored on Kernel

1. `enableMetricViewMetadata` — no auto-injection on the kernel path.
2. Auth types `custom`, `token-provider`, `external-token`, `static-token`,
   plus `enableTokenFederation` / `federationClientId`.
3. `azureTenantId` / `useDatabricksOAuthInAzure` (Azure-direct OAuth).
4. `persistence` (custom OAuth token store).
5. Custom `oauthClientId` on the U2M flow (and `oauthClientId` + no secret).
6. SOCKS proxies.
7. Per-statement `useCloudFetch`, `useLZ4Compression`,
   `stagingAllowedLocalPath`.

### Supported on Kernel, no Thrift public equivalent

1. `maxConnections` (connection-pool sizing).
2. TLS controls: `checkServerCertificate`, `checkServerCertificateHostname`,
   `customCaCert`, `clientCertPem`, `clientKeyPem`. The Thrift backend exposes
   **no** public TLS options and hardcodes `rejectUnauthorized: false`.

### Behavioral divergences to watch

- **U2M flow selection** keys off `oauthClientSecret` presence on the kernel
  path but is honored differently on Thrift.
- **`socketTimeout: 0`** means "indefinite" on Thrift but is dropped on the
  kernel path (kernel default kept).
- **`configuration`** is allowlist-filtered on the kernel path but forwarded
  more freely on Thrift.

> All kernel-path behavior reflects the **M0 stub** and is subject to change.
