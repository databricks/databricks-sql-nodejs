# Connection parameter reference

This document lists every typed public parameter accepted by
`DBSQLClient.connect`, `DBSQLClient.openSession`, and
`DBSQLSession.executeStatement`, plus the internal or deprecated connection
inputs that the runtime still recognizes. Because the driver ships two
backends, each table states whether the parameter is honored by the **Thrift**
backend (the default), the **SEA / Kernel** backend, or both.

> **Backend selection.** The connector defaults to Thrift. Pass the internal,
> unstable `useKernel: true` option to select Kernel. It is intentionally absent
> from the published `.d.ts` and may be removed without notice.

## Legend

| Symbol | Meaning                                                          |
| ------ | ---------------------------------------------------------------- |
| ✅     | Honored by the backend.                                          |
| ❌     | Ignored, rejected, or unsupported by the backend.                |
| ⚠️     | Honored only for some values, flows, or requests; read the note. |
| —      | Not applicable or no default on this backend.                    |

## Sources of truth

- Public option shapes: `lib/contracts/IDBSQLClient.ts` and
  `lib/contracts/IDBSQLSession.ts`.
- Internal Kernel options: `lib/contracts/InternalConnectionOptions.ts` and
  the runtime-only casts in `lib/kernel/KernelAuth.ts`.
- Defaults: `DBSQLClient.getDefaultConfig`, `DEFAULT_TELEMETRY_CONFIG`, and
  `native/kernel/index.d.ts`.
- Thrift wiring: `lib/DBSQLClient.ts`, `lib/thrift-backend/ThriftBackend.ts`,
  `lib/thrift-backend/ThriftSessionBackend.ts`, and
  `lib/connection/connections/HttpConnection.ts`.
- Kernel wiring: `lib/kernel/KernelAuth.ts`, `lib/kernel/KernelBackend.ts`, and
  `lib/kernel/KernelSessionBackend.ts`.

---

## Connection identity and backend selection

| Option                  | Type      | Thrift | Kernel | Default Value | Note                                                                                   |
| ----------------------- | --------- | :----: | :----: | ------------- | -------------------------------------------------------------------------------------- |
| `host`                  | `string`  |   ✅   |   ✅   | — (required)  | Required on both.                                                                      |
| `path`                  | `string`  |   ✅   |   ✅   | — (required)  | Kernel also derives `x-databricks-org-id` from a `?o=<id>` query parameter.            |
| `port`                  | `number`  |   ✅   |   ❌   | `443` / —     | Kernel ignores this field. Include a non-default port in `host` for Kernel.            |
| `userAgentEntry`        | `string`  |   ✅   |   ✅   | —             | Folded into the connector `User-Agent` on both.                                        |
| `useKernel` (internal)  | `boolean` |   —    |   —    | `false`       | Selects the backend: `false`/omitted → Thrift, `true` → Kernel.                        |
| `clientId` (deprecated) | `string`  |   ✅   |   ✅   | —             | Runtime-only alias for `userAgentEntry`. `userAgentEntry` wins when both are supplied. |

## Authentication

| Option                                        | Type                                                         | Thrift | Kernel | Default Value                 | Note                                                                                                                                                                                                                       |
| --------------------------------------------- | ------------------------------------------------------------ | :----: | :----: | ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `authType` — common modes                     | `'access-token'` \| `'databricks-oauth'` \| `'static-token'` |   ✅   |   ✅   | `'access-token'`              | `access-token` uses `token`; `static-token` uses `staticToken`. OAuth supports U2M and M2M, with the Kernel qualifications below.                                                                                          |
| `authType` — Thrift-only modes                | `'custom'` \| `'token-provider'` \| `'external-token'`       |   ✅   |   ❌   | —                             | Kernel rejects these auth modes.                                                                                                                                                                                           |
| `token`                                       | `string`                                                     |   ✅   |   ✅   | required for `access-token`   | Thrift sends the PAT as a Bearer token (`Authorization: Bearer <token>`); Kernel uses native PAT auth. Kernel rejects blank/reserved values client-side.                                                                   |
| `staticToken`                                 | `string`                                                     |   ✅   |   ✅   | required for `static-token`   | Thrift optionally wraps it in federation. Kernel uses native bearer-token auth with federation always enabled.                                                                                                             |
| `provider`                                    | `IAuthentication`                                            |   ✅   |   ❌   | required for `custom`         | Custom Thrift authentication provider.                                                                                                                                                                                     |
| `tokenProvider`                               | `ITokenProvider`                                             |   ✅   |   ❌   | required for `token-provider` | Thrift wraps it with caching and optional federation.                                                                                                                                                                      |
| `getToken`                                    | `TokenCallback`                                              |   ✅   |   ❌   | required for `external-token` | Callback used by the Thrift external-token provider.                                                                                                                                                                       |
| `oauthScopes`                                 | `Array<string>`                                              |   ❌   |   ⚠️   | flow-dependent                | Thrift does not pass caller-supplied scopes to `DatabricksOAuth`. Kernel honors them for U2M and workspace-OIDC M2M, but Azure Entra-direct M2M uses its fixed `<resource>/.default` scope.                                |
| `oauthClientId`                               | `string`                                                     |   ✅   |   ✅   | flow- and cloud-dependent     | In-house OAuth defaults to `databricks-sql-connector`. Thrift Entra-direct defaults to its Azure application id; Kernel Entra-direct M2M requires an explicit id.                                                          |
| `oauthClientSecret`                           | `string`                                                     |   ✅   |   ✅   | —                             | Presence selects M2M; absence selects U2M on both. Kernel forwards blank values for workspace-OIDC parity, but rejects them in the Azure Entra-direct arm.                                                                 |
| `azureTenantId` / `useDatabricksOAuthInAzure` | `string` / `boolean`                                         |   ✅   |   ⚠️   | —                             | Kernel ignores both for U2M. On Azure M2M, `useDatabricksOAuthInAzure: false`/omitted selects Entra-direct and `true` selects workspace OIDC; `azureTenantId` applies only to Entra-direct.                                |
| `persistence`                                 | `OAuthPersistence`                                           |   ✅   |   ❌   | in-memory store / —           | Custom OAuth persistence hook. Kernel rejects it; use `tokenCacheEnabled` for the Kernel U2M cache.                                                                                                                        |
| `tokenCacheEnabled`                           | `boolean`                                                    |   ❌   |   ✅   | — / `false`                   | Kernel U2M only. Enables its encrypted on-disk refresh-token cache; it has no effect on M2M or other auth types.                                                                                                           |
| `enableTokenFederation`                       | `boolean`                                                    |   ✅   |   ❌   | `false` / —                   | Thrift federation opt-in. Kernel ignores it because federation is always enabled for `static-token`.                                                                                                                       |
| `federationClientId`                          | `string`                                                     |   ✅   |   ⚠️   | —                             | Thrift uses it when federation is enabled for token-provider, external-token, or static-token auth. Kernel honors it only for `static-token`; a non-empty value selects SP-wide WIF and omission selects account-wide WIF. |
| `authProvider` second argument (deprecated)   | `IAuthentication`                                            |   ✅   |   ❌   | —                             | Deprecated second argument to `DBSQLClient.connect`. It overrides Thrift authentication; Kernel ignores it and authenticates from `ConnectionOptions`.                                                                     |

## HTTP client, proxy, and retries

| Option                      | Type                     | Thrift | Kernel | Default Value       | Note                                                                                                                                                                                                                                            |
| --------------------------- | ------------------------ | :----: | :----: | ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `socketTimeout`             | `number` (ms)            |   ✅   |   ⚠️   | `900000` / `120000` | Kernel forwards only positive values. `0` means no timeout on Thrift but is omitted on Kernel, which then keeps its 120-second default.                                                                                                         |
| `proxy`                     | `ProxyOptions`           |   ✅   |   ⚠️   | —                   | `{protocol, host, port, auth}`. Kernel supports HTTP/HTTPS proxies only; Thrift also supports the declared SOCKS variants.                                                                                                                      |
| `noProxy` (internal)        | `string`                 |   ❌   |   ⚠️   | —                   | Runtime-only Kernel bypass list, forwarded as `bypassHosts` only when an explicit `proxy` is also supplied.                                                                                                                                     |
| `customHeaders`             | `Record<string, string>` |   ⚠️   |   ✅   | —                   | Thrift applies these only to driver-owned telemetry and feature-flag requests, not the primary transport or OAuth. Kernel applies them after dropping reserved auth/org headers, validating control characters, and appending the connector UA. |
| `retryMaxAttempts`          | `number`                 |   ✅   |   ✅   | `5`                 | Total attempts, including the initial request. Kernel converts this to its retries-after-first representation internally.                                                                                                                       |
| `retriesTimeout`            | `number` (ms)            |   ✅   |   ✅   | `900000`            | Kernel converts milliseconds to whole seconds.                                                                                                                                                                                                  |
| `retryDelayMin`             | `number` (ms)            |   ✅   |   ✅   | `1000`              | Kernel converts milliseconds to whole seconds.                                                                                                                                                                                                  |
| `retryDelayMax`             | `number` (ms)            |   ✅   |   ✅   | `60000`             | Kernel converts milliseconds to whole seconds.                                                                                                                                                                                                  |
| `maxConnections` (internal) | `number`                 |   ❌   |   ✅   | — / Kernel default  | Kernel connection-pool size. Must be a positive integer within the napi `u32` range.                                                                                                                                                            |

## TLS / SSL

Both backends verify server certificates by default. Prefer `customCaCert` to
disabling verification.

| Option                                      | Type               | Thrift | Kernel | Default Value | Note                                                                                                                                          |
| ------------------------------------------- | ------------------ | :----: | :----: | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `checkServerCertificate`                    | `boolean`          |   ✅   |   ✅   | `true`        | Master verification toggle. `false` disables chain and hostname verification.                                                                 |
| `checkServerCertificateHostname` (internal) | `boolean`          |   ❌   |   ✅   | — / `true`    | Independent Kernel hostname-vs-SNI check; no-op when `checkServerCertificate` is `false`.                                                     |
| `customCaCert`                              | `Buffer \| string` |   ✅   |   ✅   | —             | Additional PEM CA. Thrift uses Node bundled roots plus `NODE_EXTRA_CA_CERTS`, then appends this CA; Kernel adds it to its normal trust roots. |
| `clientCert`                                | `Buffer \| string` |   ✅   |   ✅   | —             | Public mTLS client certificate. Must be paired with `clientKey`.                                                                              |
| `clientKey`                                 | `Buffer \| string` |   ✅   |   ✅   | —             | Public mTLS private key. Must be paired with `clientCert`; PKCS#8 is recommended for Kernel portability.                                      |
| `clientCertPem` / `clientKeyPem` (internal) | `Buffer \| string` |   ❌   |   ✅   | —             | Runtime-only Kernel aliases for the public mTLS pair. They take precedence over `clientCert`/`clientKey` when both pairs are supplied.        |

## Results and type rendering

| Option                        | Type      | Thrift | Kernel | Default Value | Note                                                                                                                                   |
| ----------------------------- | --------- | :----: | :----: | ------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `preserveBigNumericPrecision` | `boolean` |   ✅   |   ✅   | `false`       | Returns DECIMAL as an exact string and BIGINT as `bigint` on both.                                                                     |
| `disableRowMaterialization`   | `boolean` |   ✅   |   ✅   | `false`       | Fetches and parses Arrow batches but returns `null` row placeholders instead of converting cells. Intended for fetch-throughput tests. |
| `enableMetricViewMetadata`    | `boolean` |   ✅   |   ⚠️   | `false`       | Injected into session configuration on both paths. Kernel may drop its non-allowlisted configuration key.                              |

## Session defaults (`openSession(request)`)

| Option           | Type                                          | Thrift | Kernel | Default Value | Note                                                                                                                                            |
| ---------------- | --------------------------------------------- | :----: | :----: | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `initialCatalog` | `string`                                      |   ✅   |   ✅   | —             | Initial catalog for the session.                                                                                                                |
| `initialSchema`  | `string`                                      |   ✅   |   ✅   | —             | Initial schema for the session.                                                                                                                 |
| `configuration`  | `{ [key: string]: string }`                   |   ✅   |   ⚠️   | —             | Kernel matches keys case-insensitively against an allowlist and drops non-allowlisted keys with a warning. Thrift forwards the map more freely. |
| `queryTags`      | `Record<string, string \| null \| undefined>` |   ✅   |   ✅   | —             | Both serialize this into the reserved `QUERY_TAGS` session conf. It takes precedence over `configuration.QUERY_TAGS`.                           |

## Telemetry

Thrift uses the driver-layer telemetry implementation. Kernel uses its native
telemetry implementation, so only the options explicitly forwarded by
`buildKernelTelemetryOptions` apply to Kernel.

| Option                             | Type          | Thrift | Kernel | Default Value    | Note                                                                                                                                                         |
| ---------------------------------- | ------------- | :----: | :----: | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `telemetryEnabled`                 | `boolean`     |   ✅   |   ✅   | `true` / `false` | Thrift is enabled by default but gated by a workspace feature flag. Kernel is opt-in: pass `true` explicitly. `DATABRICKS_TELEMETRY_DISABLED` disables both. |
| `telemetryBatchSize`               | `number`      |   ✅   |   ✅   | `100`            | Events per export batch.                                                                                                                                     |
| `telemetryFlushIntervalMs`         | `number` (ms) |   ✅   |   ✅   | `5000`           | Periodic flush interval.                                                                                                                                     |
| `telemetryMaxRetries`              | `number`      |   ✅   |   ✅   | `3`              | Export retry attempts after the initial request.                                                                                                             |
| `telemetryAuthenticatedExport`     | `boolean`     |   ✅   |   ❌   | `true` / —       | Selects the authenticated endpoint only in the Thrift driver-layer exporter. It is not forwarded to Kernel.                                                  |
| `telemetryCircuitBreakerThreshold` | `number`      |   ✅   |   ⚠️   | `5`              | Forwarded to Kernel, but effective there only when the native telemetry circuit breaker is enabled.                                                          |
| `telemetryCircuitBreakerTimeout`   | `number` (ms) |   ✅   |   ⚠️   | `60000`          | Forwarded to Kernel, but effective there only when the native telemetry circuit breaker is enabled.                                                          |
| `telemetryCloseTimeoutMs`          | `number` (ms) |   ✅   |   ✅   | `2000`           | Maximum wait for the final telemetry flush.                                                                                                                  |
| `telemetryMaxStatementMetrics`     | `number`      |   ✅   |   ❌   | `5000` / —       | Thrift driver-layer aggregation-map cap. It is not forwarded to Kernel.                                                                                      |
| `telemetryMaxPendingMetrics`       | `number`      |   ✅   |   ❌   | `500` / —        | Thrift driver-layer pending-buffer cap. It is not forwarded to Kernel.                                                                                       |

> Telemetry events also differ by backend. Kernel owns execution and result
> fetching below the TypeScript layer, so its event set is not identical to the
> Thrift driver-layer event set.

## Per-statement options (`session.executeStatement(sql, options)`)

| Option                    | Type                                                    | Thrift | Kernel | Default Value | Note                                                                                                                                                                      |
| ------------------------- | ------------------------------------------------------- | :----: | :----: | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `queryTimeout`            | `number \| bigint \| Int64`                             |   ✅   |   ❌   | —             | Thrift sends this server-side timeout for Compute clusters. Kernel ignores it; SQL Warehouses use `STATEMENT_TIMEOUT`.                                                    |
| `runAsync`                | `boolean`                                               |   ❌   |   ✅   | — / `false`   | Thrift always sends asynchronously and ignores the option. Kernel `false`/omitted uses direct execution; `true` submits and polls. This execute option is not deprecated. |
| `maxRows`                 | `number \| bigint \| Int64 \| null`                     |   ✅   |   ❌   | `100000` / —  | Thrift uses it for the initial Direct Results row count; `null` disables Direct Results. Kernel does not retain or forward the supplied value.                            |
| `useCloudFetch`           | `boolean`                                               |   ✅   |   ❌   | `true` / —    | Kernel logs and ignores this per-statement hint; Kernel result fetching owns its CloudFetch behavior.                                                                     |
| `useLZ4Compression`       | `boolean`                                               |   ✅   |   ❌   | `true` / —    | Thrift uses it when supported and when the result is not CloudFetch. Kernel owns and auto-detects result compression.                                                     |
| `stagingAllowedLocalPath` | `string \| string[]`                                    |   ✅   |   ❌   | —             | Local allowlist for Thrift volume/staging operations. Kernel volume operations are unsupported.                                                                           |
| `namedParameters`         | `Record<string, DBSQLParameter \| DBSQLParameterValue>` |   ✅   |   ✅   | —             | Named SQL parameters. Cannot be combined with non-empty `ordinalParameters`. Thrift requires a protocol that supports parameterized queries.                              |
| `ordinalParameters`       | `Array<DBSQLParameter \| DBSQLParameterValue>`          |   ✅   |   ✅   | —             | Positional SQL parameters. Cannot be combined with non-empty `namedParameters`.                                                                                           |
| `queryTags`               | `Record<string, string \| null \| undefined>`           |   ✅   |   ✅   | —             | Serialized into the per-statement `query_tags` conf overlay on both.                                                                                                      |
| `rowLimit`                | `number`                                                |   ❌   |   ✅   | —             | Kernel-only server-side row cap. Thrift logs and ignores it.                                                                                                              |
| `statementConf`           | `Record<string, string>`                                |   ❌   |   ✅   | —             | Kernel-only per-statement Spark conf overlay. Structured `queryTags` overwrite its `query_tags` key when non-empty.                                                       |

## Metadata-operation request parameters

The public session metadata methods also accept request objects. These are not
connection options, but are included so the reference covers the complete
session parameter surface.

| Methods                         | Option(s)                                                                                                                 | Thrift | Kernel | Note                                                                                                                               |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | :----: | :----: | ---------------------------------------------------------------------------------------------------------------------------------- |
| Request-object metadata methods | `maxRows`                                                                                                                 |   ✅   |   ❌   | Thrift uses it for Direct Results. Kernel currently drops it rather than applying it at fetch time.                                |
| Request-object metadata methods | `runAsync` (deprecated)                                                                                                   |   ❌   |   ❌   | The caller value is ignored. Thrift derives wire async behavior from protocol support; Kernel metadata calls are already terminal. |
| `getSchemas`                    | `catalogName`, `schemaName`                                                                                               |   ✅   |   ✅   | Optional filters.                                                                                                                  |
| `getTables`                     | `catalogName`, `schemaName`, `tableName`, `tableTypes`                                                                    |   ✅   |   ✅   | Optional filters.                                                                                                                  |
| `getColumns`                    | `catalogName`, `schemaName`, `tableName`, `columnName`                                                                    |   ✅   |   ✅   | Optional filters.                                                                                                                  |
| `getFunctions`                  | `catalogName`, `schemaName`, `functionName`                                                                               |   ✅   |   ✅   | `functionName` is required.                                                                                                        |
| `getPrimaryKeys`                | `catalogName`, `schemaName`, `tableName`                                                                                  |   ✅   |   ⚠️   | Schema and table are required. Kernel additionally requires a non-empty `catalogName`; Thrift can resolve an omitted catalog.      |
| `getCrossReference`             | `parentCatalogName`, `parentSchemaName`, `parentTableName`, `foreignCatalogName`, `foreignSchemaName`, `foreignTableName` |   ✅   |   ✅   | All six fields are required.                                                                                                       |
| `getInfo`                       | `infoType`                                                                                                                |   ⚠️   |   ⚠️   | Both effectively support the three values the Databricks server answers: server name, DBMS name, and DBMS version.                 |

---

## Main backend gaps

Thrift-only capabilities include custom/token-provider authentication, custom
OAuth persistence, SOCKS proxies, authenticated/driver-buffer telemetry knobs,
query timeout, Direct Results `maxRows`, per-statement CloudFetch/LZ4 hints, and
volume operations.

Kernel-only capabilities include connection-pool sizing, proxy bypass hosts,
independent TLS hostname verification, optional U2M disk caching, and
per-statement `rowLimit`/`statementConf`.

The Kernel path remains internal and unstable; its behavior may change.
