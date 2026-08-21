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

import { ConnectionOptions } from '../contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../contracts/InternalConnectionOptions';
import AuthenticationError from '../errors/AuthenticationError';
import HiveDriverError from '../errors/HiveDriverError';
import { buildUserAgentString, normalizePemBytes } from '../utils';

/**
 * Default local listener port for the U2M authorization-code callback.
 * Hardcoded here so the override of the kernel default (8020) to the
 * thrift default (8030) is invariant for kernel callers — preserving parity
 * with the existing Node driver. Not exposed on the public
 * `ConnectionOptions` (thrift hides `callbackPorts` from its public
 * surface too — see nodejs-thrift-expert survey §B.2).
 */
const U2M_DEFAULT_REDIRECT_PORT = 8030;

// U2M OAuth scopes default. Matches the standalone Thrift driver's
// `defaultOAuthScopes` (lib/connection/auth/DatabricksOAuth/OAuthScope.ts):
// `['sql', 'offline_access']`. The kernel's bare default is
// `['all-apis', 'offline_access']`; the `databricks-sql-connector` OAuth app is
// registered for the `sql` scope, so we pass the Thrift-parity scopes explicitly
// unless the caller overrides via `oauthScopes`.
const U2M_DEFAULT_SCOPES = ['sql', 'offline_access'];

// M2M OAuth scopes default. Matches the standalone Thrift driver (`getScopes`
// forces `['all-apis']` for the client-credentials flow) and the kernel's own
// M2M default (`m2m.rs` → `['all-apis']`). Overridable via `oauthScopes`
// (parity with pyo3, which forwards `scopes` on M2M).
const M2M_DEFAULT_SCOPES = ['all-apis'];

// Default OAuth client id — identical to the Thrift driver's
// `DatabricksOAuthManager.defaultClientId` and the kernel napi's own U2M default.
// Used for `oauthClientId ?? default`, mirroring Thrift's `getClientId()`.
const DEFAULT_OAUTH_CLIENT_ID = 'databricks-sql-connector';

/**
 * Shape consumed by the napi-binding's `openSession()` (see
 * `native/kernel/index.d.ts`). Mirrors `ConnectionOptions` in the binding's
 * `.d.ts`; declared locally to avoid coupling the JS-side adapter to the
 * auto-generated TS file.
 *
 * Discriminated by `authMode`:
 * - `'Pat'`       → `token` is the PAT.
 * - `'OAuthM2m'`  → `oauthClientId` + `oauthClientSecret` drive a
 *                   kernel-side client_credentials exchange.
 * - `'OAuthU2m'`  → `oauthRedirectPort` overrides the kernel default;
 *                   everything else (client_id, scopes, callback timeout,
 *                   token_url_override) uses kernel defaults.
 *
 * `static-token` reuses the native PAT bearer-token mode, where federation is
 * always enabled. `enableTokenFederation` is ignored; a non-empty
 * `federationClientId` selects SP-wide WIF and omission selects account-wide.
 *
 * The `authMode` string literals MUST match the napi-emitted `AuthMode`
 * variant names verbatim (`'Pat'`, `'OAuthM2m'`, `'OAuthU2m'` — napi-rs's
 * `#[napi(string_enum)]` without an explicit case option emits the
 * Rust variant identifier as-is). We duplicate the values here instead
 * of importing `AuthMode` from `native/kernel/index.d.ts` because that
 * file declares `AuthMode` as `export const enum`, which is
 * incompatible with `isolatedModules` and a runtime-coupling hazard.
 * The Rust source of truth lives at `native/kernel/src/database.rs`.
 */
/**
 * Session-level defaults shared across all auth-mode variants.
 *
 * Mirrors `ConnectionOptions.catalog` / `.schema` / `.sessionConf` on
 * the napi binding (kernel `Session::builder().defaults(DefaultOpts)`
 * and `.session_conf(HashMap)` — the routes that actually populate kernel
 * `CreateSession.catalog` / `.schema` / `.session_confs`).
 *
 * Per-statement overrides do not exist on the kernel surface; both
 * pyo3 and napi expose catalog / schema / sessionConf only at session
 * creation. Mirror that here so the adapter doesn't promise a
 * capability the binding can't honour.
 */
export interface KernelSessionDefaults {
  catalog?: string;
  schema?: string;
  sessionConf?: Record<string, string>;
  /**
   * Render `INTERVAL` / `DURATION` result columns as strings
   * (kernel `ResultConfig.intervals_as_string`). The kernel default is
   * native Arrow `month_interval` / `duration[us]`, but the NodeJS
   * Thrift driver surfaces intervals as strings — so the kernel path sets
   * this `true` so its result shape is a byte-compatible drop-in for the
   * Thrift backend. Omitting it falls back to the kernel's native types.
   */
  intervalsAsString?: boolean;
  /**
   * Render complex (`ARRAY` / `MAP` / `STRUCT` / `VARIANT`) result
   * columns as JSON strings (kernel `ResultConfig.complex_types_as_json`).
   * Left unset on the kernel path: native Arrow nested types already decode
   * identically to the Thrift backend through the shared Arrow converter,
   * so forcing JSON here would *introduce* a divergence rather than
   * remove one.
   */
  complexTypesAsJson?: boolean;
  /**
   * Per-session kernel connection-pool size
   * (kernel `ConnectionOptions.max_connections`). Validated as a positive
   * integer within the napi `u32` range by `buildKernelConnectionOptions`.
   */
  maxConnections?: number;
  /**
   * Retry/backoff tuning forwarded to the kernel (which owns the retry loop
   * on the kernel path). These mirror the driver's `ClientConfig` retry knobs —
   * the same ones the Thrift `HttpRetryPolicy` uses — converted from the
   * connector's milliseconds to the kernel's whole seconds, so a single
   * retry config governs both backends. Unset ⇒ kernel default policy.
   * Map onto the napi `ConnectionOptions.retry{Min,Max}WaitSecs` /
   * `retryMaxAttempts` / `retryOverallTimeoutSecs` (see `buildKernelRetryOptions`).
   */
  retryMinWaitSecs?: number;
  retryMaxWaitSecs?: number;
  /** **Total** attempts (kernel converts to retries-after-first internally). */
  retryMaxAttempts?: number;
  retryOverallTimeoutSecs?: number;
}

/**
 * TLS options shared across all auth-mode variants. Mirror the napi
 * binding's `ConnectionOptions.checkServerCertificate` / `.customCaCert`
 * (kernel `Session::builder().tls(TlsConfig)`).
 *
 * The napi shape takes `customCaCert` as a `Buffer` only; the public
 * `ConnectionOptions` additionally accepts a PEM string, which
 * `buildKernelConnectionOptions` normalises to a `Buffer` before crossing
 * the FFI boundary.
 */
export interface KernelTlsOptions {
  /**
   * Verify the server's TLS certificate. The kernel backend is
   * **secure-by-default**: omitting this leaves the kernel default of
   * `true` (full chain + hostname verification). Set `false` only to opt
   * into the insecure, accept-anything mode (analogous to Thrift's
   * `rejectUnauthorized: false`); prefer pairing strict checking with
   * `customCaCert` over disabling verification entirely.
   */
  checkServerCertificate?: boolean;
  /**
   * Verify the server certificate's hostname (hostname-vs-SNI), independently
   * of chain validation. Omit ⇒ kernel default (on). `false` skips only the
   * hostname check. No-op when `checkServerCertificate` is `false`. Mirrors
   * the kernel napi `checkServerCertificateHostname` / Python
   * `tls_verify_hostname`.
   */
  checkServerCertificateHostname?: boolean;
  /** PEM-encoded CA bytes to add to the trust store. */
  customCaCert?: Buffer;
  /**
   * PEM-encoded client certificate for mutual TLS (kernel
   * `TlsConfig::client_cert_pem`). Paired with {@link clientKeyPem} —
   * `buildKernelTlsOptions` rejects supplying only one before the FFI hop.
   * The napi shape takes a `Buffer`; the public surface also accepts a
   * PEM string, normalised here.
   */
  clientCertPem?: Buffer;
  /**
   * PEM-encoded private key for the mTLS client certificate (kernel
   * `TlsConfig::client_key_pem`). Paired with {@link clientCertPem}.
   */
  clientKeyPem?: Buffer;
}

/**
 * HTTP options shared across all auth-mode variants. Mirrors the napi
 * binding's `ConnectionOptions.customHeaders` (kernel
 * `HttpConfig::custom_headers`).
 *
 * Carries the extra request headers the kernel path sends on every request:
 * the caller's `customHeaders` plus the composed `User-Agent` (the kernel
 * appends a `User-Agent` entry to its base UA rather than replacing it).
 *
 * An **ordered list** of `{ name, value }` pairs — the napi shape
 * (`Array<HeaderEntry>`), which mirrors the kernel core's
 * `Vec<(String, String)>` and the Python connector's `http_headers`
 * `List[Tuple[str, str]]`. Order is preserved and duplicate names are
 * allowed (e.g. a caller `User-Agent` followed by the connector's, which
 * the kernel folds last-wins).
 */
export interface KernelHttpOptions {
  customHeaders?: Array<{ name: string; value: string }>;
  socketTimeoutMs?: number;
}

/**
 * HTTP(S) proxy forwarded to the napi binding's `ConnectionOptions.proxy`
 * (kernel `ProxyConfig`). The public `ConnectionOptions.proxy` is the
 * Thrift-shaped `{protocol, host, port, auth}`; `buildKernelProxyOptions`
 * maps it onto the kernel's structured proxy input — `url` composed from
 * `protocol://host:port`, with `auth.{username,password}` forwarded as
 * separate basic-auth fields (NOT embedded in the URL, so no percent-encoding
 * footgun) and the `noProxy` host list forwarded as `bypassHosts`. The same
 * connection option therefore works identically on both backends.
 */
export interface KernelProxyOptions {
  proxy?: {
    url: string;
    username?: string;
    password?: string;
    bypassHosts?: string;
  };
}

export interface KernelFederationOptions {
  /**
   * SP-wide Workload Identity Federation client id. Omitted selects BYOT /
   * account-wide WIF.
   */
  identityFederationClientId?: string;
}

export type KernelNativeConnectionOptions = KernelSessionDefaults &
  KernelTlsOptions &
  KernelHttpOptions &
  KernelProxyOptions &
  KernelFederationOptions &
  (
    | {
        hostName: string;
        httpPath: string;
        authMode: 'Pat';
        token: string;
      }
    | {
        hostName: string;
        httpPath: string;
        authMode: 'OAuthM2m';
        oauthClientId: string;
        oauthClientSecret: string;
        oauthScopes?: Array<string>;
      }
    | {
        hostName: string;
        httpPath: string;
        authMode: 'OAuthU2m';
        oauthRedirectPort: number;
        oauthScopes?: Array<string>;
        oauthClientId?: string;
      }
    | {
        hostName: string;
        httpPath: string;
        authMode: 'AzureSpM2m';
        azureClientId: string;
        azureClientSecret: string;
        azureTenantId?: string;
      }
  );

function prependSlash(str: string): string {
  if (str.length > 0 && str.charAt(0) !== '/') {
    return `/${str}`;
  }
  return str;
}

/**
 * Azure Databricks host suffixes — the superset the Thrift driver's
 * `OAuthManager.getManager` recognises (`.azuredatabricks.net`,
 * `.databricks.azure.us`, `.databricks.azure.cn`). Used to decide whether an
 * OAuth connection is on Azure and therefore subject to the in-house-vs-
 * Entra-direct split.
 */
const AZURE_HOST_SUFFIXES = ['.azuredatabricks.net', '.databricks.azure.us', '.databricks.azure.cn'];

/**
 * True when `host` is an Azure Databricks workspace host. Normalises the input
 * the same way `getManager` does (trim surrounding whitespace, lowercase, strip
 * scheme, then drop any path and explicit `:port`) so a caller passing a bare
 * host, a padded string, or a full URL with a port is treated identically.
 */
function isAzureHost(host: string): boolean {
  const normalized = host
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .split('/')[0]
    .split(':')[0];
  return AZURE_HOST_SUFFIXES.some((suffix) => normalized.endsWith(suffix));
}

/**
 * Reject inputs that pass `typeof === 'string' && length > 0` but are
 * structurally useless as credentials: whitespace-only strings, and the
 * literal strings `'undefined'` / `'null'` (case-insensitive) that buggy
 * shell exports (e.g. `export FOO="$UNSET_VAR"`) produce. Surfacing
 * these here means an OAuth flow's `invalid_client` from the workspace
 * is always a real credential mismatch, never a malformed-input passthrough.
 *
 * Exported so the integration-test env-gate can reuse the same predicate
 * and stay in lockstep with production (B-3 fix).
 */
export function isBlankOrReserved(s: string): boolean {
  const normalized = s.trim().toLowerCase();
  return normalized.length === 0 || normalized === 'undefined' || normalized === 'null';
}

/** napi-rs marshals `maxConnections` as a `u32`; reject values it can't hold. */
const MAX_U32 = 0xffffffff;

/**
 * Normalise the public TLS options into the napi shape.
 *
 * - `checkServerCertificate` passes through verbatim (only when set; an
 *   absent value leaves the kernel default, which is secure — verify on).
 * - `checkServerCertificateHostname` passes through verbatim — the
 *   independent hostname-vs-SNI toggle (kernel applies it only when the
 *   master verify toggle is on). Mirrors Python's `tls_verify_hostname`.
 * - `customCaCert` accepts a PEM string or `Buffer`; normalised to a
 *   `Buffer` via {@link normalizePemBytes}.
 * - `clientCertPem` / `clientKeyPem` (or their public aliases
 *   `clientCert` / `clientKey`) carry the mutual-TLS client identity. The
 *   internal `*Pem` names win when both are present. They must be supplied
 *   **together** — supplying only one is rejected here with an actionable
 *   error (rather than waiting for the kernel's `InvalidArgument` at
 *   `openSession`). Each accepts a PEM string or `Buffer`, normalised the
 *   same way.
 *
 * Throws `HiveDriverError` when a cert/key is empty, mis-typed, lacks the
 * expected PEM header, or when only one half of the mTLS pair is set.
 */
export function buildKernelTlsOptions(options: ConnectionOptions): KernelTlsOptions {
  // Read the kernel-only fields through the purpose-built internal options type
  // rather than an ad-hoc inline cast, so the shape can't silently drift from
  // its declaration and a typo'd key fails to compile.
  const merged = options as ConnectionOptions & InternalConnectionOptions;
  const { checkServerCertificate, checkServerCertificateHostname, customCaCert } = merged;

  // The public mTLS options are `clientCert`/`clientKey` (see `ConnectionOptions`);
  // the internal kernel-only aliases are `clientCertPem`/`clientKeyPem`. Accept
  // both here — preferring the explicit internal alias when present — so a caller
  // who sets the public `clientCert`/`clientKey` and runs on the kernel backend
  // still gets mTLS configured instead of having their client identity silently
  // dropped.
  const clientCertPem = merged.clientCertPem ?? merged.clientCert;
  const clientKeyPem = merged.clientKeyPem ?? merged.clientKey;

  const tls: KernelTlsOptions = {};

  if (checkServerCertificate !== undefined) {
    tls.checkServerCertificate = checkServerCertificate;
  }

  if (checkServerCertificateHostname !== undefined) {
    tls.checkServerCertificateHostname = checkServerCertificateHostname;
  }

  if (customCaCert !== undefined) {
    tls.customCaCert = normalizePemBytes(customCaCert, 'customCaCert', 'certificate', 'kernel backend');
  }

  // mTLS client identity. Enforce both-or-neither up front so a caller who
  // sets only one gets a clear message naming the missing half, instead of
  // the kernel's generic `InvalidArgument` after the FFI hop.
  const hasCert = clientCertPem !== undefined;
  const hasKey = clientKeyPem !== undefined;
  if (hasCert !== hasKey) {
    throw new HiveDriverError(
      'kernel backend: mutual TLS requires both `clientCertPem` and `clientKeyPem`; only ' +
        `\`${hasCert ? 'clientCertPem' : 'clientKeyPem'}\` was supplied. ` +
        `Provide the matching ${hasCert ? 'private key (`clientKeyPem`)' : 'certificate (`clientCertPem`)'}, ` +
        'or omit both.',
    );
  }
  if (hasCert && hasKey) {
    tls.clientCertPem = normalizePemBytes(
      clientCertPem as Buffer | string,
      'clientCertPem',
      'certificate',
      'kernel backend',
    );
    tls.clientKeyPem = normalizePemBytes(
      clientKeyPem as Buffer | string,
      'clientKeyPem',
      'private key',
      'kernel backend',
    );
  }

  return tls;
}

/**
 * Build the napi HTTP options (`customHeaders`) from the public
 * `customHeaders` map and `userAgentEntry`.
 *
 * Mirrors the Python connector's `use_kernel` path (`session.py` +
 * `backend/kernel/client.py`), which:
 *   1. composes a single connector `User-Agent` and **unconditionally**
 *      appends it last —
 *      `all_headers = (http_headers or []) + [("User-Agent", useragent_header)]`;
 *   2. before forwarding to the kernel, **drops** the kernel-managed
 *      reserved names `Authorization` / `x-databricks-org-id`
 *      (case-insensitive) — the kernel applies the auth token itself and
 *      re-derives the org id from the `?o=` in the http path, and would
 *      otherwise skip-and-warn on every request.
 *
 * The result is an ordered list (the napi `Array<HeaderEntry>` shape,
 * matching the kernel core `Vec<(String, String)>`): the caller's
 * `customHeaders` first (minus reserved names), then the connector's
 * `User-Agent` last. The connector UA is always present and, being last,
 * is authoritative (the kernel folds the last `User-Agent` into its base
 * UA — `DatabricksJDBCDriverOSS/...` — preserving the result-disposition
 * gating token). The value is composed via the same `buildUserAgentString`
 * the Thrift path uses, so the kernel UA carries the identical
 * `NodejsDatabricksSqlConnector/...` identity (with `userAgentEntry`
 * folded in). A caller `User-Agent` in `customHeaders` is forwarded too
 * (mirroring Python, which doesn't dedupe it); the kernel's last-wins fold
 * means the connector UA still wins.
 */
const KERNEL_MANAGED_HEADERS = new Set(['authorization', 'x-databricks-org-id']);

// CR / LF / NUL in a header name or value enable request-splitting / header
// injection. The kernel's HTTP client (reqwest) does reject these, but only at
// connect time and with an opaque "Failed to construct HTTP client:
// InvalidArgument: failed to parse header value" error that names neither the
// offending header nor the cause. Reject them here, before the FFI hop, with a
// clear error so a caller gets actionable signal at the point they set the
// header (verified against pecotesting: the kernel otherwise surfaces the
// opaque construction error).
const FORBIDDEN_HEADER_CHARS = /[\r\n\0]/;

function validateHeaderToken(kind: 'name' | 'value', headerName: string, token: string): void {
  if (FORBIDDEN_HEADER_CHARS.test(token)) {
    throw new HiveDriverError(
      `kernel backend: customHeaders ${kind} for \`${headerName}\` contains a forbidden control character ` +
        `(CR, LF, or NUL). Such characters enable HTTP header injection and are rejected.`,
    );
  }
}

export function buildKernelHttpOptions(options: ConnectionOptions): KernelHttpOptions {
  const { customHeaders, userAgentEntry, socketTimeout } = options;

  const headers: Array<{ name: string; value: string }> = [];
  if (customHeaders) {
    for (const [name, value] of Object.entries(customHeaders)) {
      // Reject CR/LF/NUL in either the name or the value before forwarding —
      // a clear, early error instead of the kernel's opaque connect-time throw.
      validateHeaderToken('name', name, name);
      validateHeaderToken('value', name, value);
      // Drop kernel-managed reserved names before the FFI hop — same
      // double-wall as the Python connector's `_KERNEL_MANAGED_HEADERS`.
      if (KERNEL_MANAGED_HEADERS.has(name.toLowerCase())) {
        continue;
      }
      headers.push({ name, value });
    }
  }

  // Always append the connector's composed User-Agent last — exactly the
  // Python connector's unconditional `base_headers` append.
  headers.push({ name: 'User-Agent', value: buildUserAgentString(userAgentEntry) });

  const http: KernelHttpOptions = { customHeaders: headers };
  // Per-connection socket read timeout (ms). The public `socketTimeout`
  // ConnectionOption maps onto the kernel napi `socketTimeoutMs`
  // (kernel `HttpConfig::request_timeout` / reqwest `Client::timeout`).
  // Only forward a POSITIVE value: `socketTimeout: 0` means "disabled / wait
  // indefinitely" on the Thrift path, but forwarding `0` would make reqwest
  // time out immediately, so we omit it and let the kernel keep its (large)
  // default — preserving the "effectively no idle timeout" semantics.
  if (typeof socketTimeout === 'number' && socketTimeout > 0) {
    http.socketTimeoutMs = socketTimeout;
  }
  return http;
}

/**
 * Validate the user-supplied `ConnectionOptions` and build the
 * napi-binding's connection-options shape.
 *
 * Supported auth modes:
 *   - PAT: `authType: 'access-token'` (or undefined, which already means
 *     PAT throughout the existing driver — see
 *     `DBSQLClient.createAuthProvider`).
 *   - Static token: `authType: 'static-token'` + `staticToken`. The token is
 *     forwarded through the native PAT bearer-token mode, where federation is
 *     always enabled. `federationClientId` selects SP-wide WIF; omission
 *     selects account-wide WIF. `enableTokenFederation` is ignored.
 *   - OAuth M2M: `authType: 'databricks-oauth'` + `oauthClientId` +
 *     `oauthClientSecret`. Kernel handles OIDC discovery, client_credentials
 *     exchange, and re-auth on expiry internally.
 *   - OAuth U2M: `authType: 'databricks-oauth'` + NO `oauthClientId` and
 *     NO `oauthClientSecret`. Kernel runs the PKCE auth-code dance (opens
 *     a browser, listens on localhost:8030, exchanges the code, persists
 *     to `~/.config/databricks-sql-kernel/oauth/{sha256}.json`).
 *
 *     **Flow selection — MIRRORS THRIFT.** Thrift's
 *     `DBSQLClient.createAuthProvider` (`DBSQLClient.ts:216`) keys off the
 *     *secret* (`oauthClientSecret === undefined ? U2M : M2M`), so a custom
 *     `oauthClientId` with no secret runs U2M with that id. This adapter keys
 *     off the same signal: `oauthClientSecret === undefined` ⇒ U2M, else M2M
 *     (see `buildKernelConnectionOptions` below). A custom `oauthClientId` is
 *     forwarded verbatim on the U2M arm; when absent the napi binding applies
 *     its own default `client_id`. The adapter therefore does NOT throw an M2M
 *     "secret required" error for `oauthClientId` + no secret, and does NOT
 *     reject a custom `oauthClientId` on U2M — those decisions, if the native
 *     binding makes them, happen below the TypeScript layer and are not
 *     observable from this repo.
 *
 * Azure (Entra) on the OAuth path. The kernel runs a single, cloud-blind
 * in-house U2M flow and workspace-OIDC M2M; only Entra-direct **M2M** gets a
 * dedicated kernel mode:
 *   - **U2M (no secret), any cloud, any `useDatabricksOAuthInAzure`** →
 *     `OAuthU2m`. The kernel uses the workspace's OIDC-discovered authorize
 *     endpoint (`{host}/oidc/v1/authorize`) verbatim; that in-house
 *     workspace-federated flow works against Azure workspaces too (they federate
 *     the browser login to Entra server-side — verified E2E). So Azure U2M is
 *     NOT special-cased and NOT rejected — it forwards the in-house app
 *     (`databricks-sql-connector`) + `sql offline_access`, exactly like AWS/GCP.
 *   - **M2M (secret) with `useDatabricksOAuthInAzure: true`** (or non-Azure) →
 *     `OAuthM2m` (workspace-OIDC client-credentials).
 *   - **M2M (secret) on an Azure host with `useDatabricksOAuthInAzure` absent/
 *     `false`** (Entra-direct) → Azure service-principal M2M (`AzureSpM2m`); the
 *     Entra SP creds ride `oauthClientId`/`oauthClientSecret`, `azureTenantId`
 *     optional (kernel auto-discovers).
 *   - On a non-Azure host `useDatabricksOAuthInAzure` is inert.
 *
 * Out of scope on the OAuth paths (rejected with a clear error):
 *   - `persistence` on M2M → M2M tokens are not cached (re-issuing is
 *     cheap; no refresh token).
 *   - `persistence` on U2M → custom token store is a parity gap;
 *     requires kernel-side `AuthConfig::External` plumbing. The kernel's
 *     auto-disk-cache works for the standard flow today.
 *
 * Ambiguity:
 *   - PAT path: rejects when OAuth fields (`oauthClientId` /
 *     `oauthClientSecret`) are simultaneously set.
 *   - OAuth path: rejects when `token` is set alongside OAuth fields.
 *
 * Throws:
 *   - `AuthenticationError` for missing/blank required credentials.
 *   - `HiveDriverError` for unsupported auth modes /
 *     custom persistence / ambiguous combinations.
 */
/**
 * Convert the driver's `ClientConfig` retry knobs (milliseconds, total-attempt
 * count) into the kernel's `ConnectionOptions` retry kwargs (whole seconds).
 * The kernel owns the retry loop on the kernel path, so forwarding these keeps kernel
 * and Thrift governed by one retry config. `retryMaxAttempts` is a TOTAL attempt
 * count on both sides (the kernel converts to retries-after-first internally),
 * so it passes through directly. Sub-second delays round to the nearest second
 * (the kernel's granularity); all values are clamped into the napi `u32` range.
 */
export function buildKernelRetryOptions(config: {
  retryMaxAttempts?: number;
  retriesTimeout?: number;
  retryDelayMin?: number;
  retryDelayMax?: number;
}): Pick<
  KernelSessionDefaults,
  'retryMinWaitSecs' | 'retryMaxWaitSecs' | 'retryMaxAttempts' | 'retryOverallTimeoutSecs'
> {
  const msToSecs = (ms: number): number => Math.min(MAX_U32, Math.max(0, Math.round(ms / 1000)));
  const clampU32 = (n: number): number => Math.min(MAX_U32, Math.max(0, Math.trunc(n)));
  // Only forward a knob the connector actually set to a finite number; an
  // absent/garbage value is OMITTED so the kernel keeps its built-in default
  // (rather than emitting NaN across the FFI). A finite-but-negative value is
  // still forwarded and clamped to 0 by the helpers above.
  const out: Pick<
    KernelSessionDefaults,
    'retryMinWaitSecs' | 'retryMaxWaitSecs' | 'retryMaxAttempts' | 'retryOverallTimeoutSecs'
  > = {};
  if (Number.isFinite(config.retryDelayMin)) out.retryMinWaitSecs = msToSecs(config.retryDelayMin as number);
  if (Number.isFinite(config.retryDelayMax)) out.retryMaxWaitSecs = msToSecs(config.retryDelayMax as number);
  if (Number.isFinite(config.retryMaxAttempts)) out.retryMaxAttempts = clampU32(config.retryMaxAttempts as number);
  if (Number.isFinite(config.retriesTimeout)) out.retryOverallTimeoutSecs = msToSecs(config.retriesTimeout as number);
  return out;
}

/**
 * Map the public `ConnectionOptions.proxy` (`{protocol, host, port, auth}` —
 * the same shape the Thrift backend accepts) onto the kernel's structured napi
 * proxy input. The `url` is composed from `protocol://host:port` (no embedded
 * credentials); `auth.{username,password}` are forwarded as separate
 * basic-auth fields (the kernel applies them via reqwest `Proxy::basic_auth`),
 * avoiding any URL percent-encoding footgun. The `noProxy` host list (a driver
 * option, not on the published `.d.ts`) is forwarded as `bypassHosts`. The
 * kernel accepts only `http://` / `https://`; a SOCKS protocol surfaces a clear
 * kernel error at connect (reqwest SOCKS support is not compiled in).
 */
export function buildKernelProxyOptions(options: ConnectionOptions): KernelProxyOptions {
  const { proxy } = options;
  if (!proxy) {
    return {};
  }
  const { noProxy } = options as ConnectionOptions & { noProxy?: string };
  const out: NonNullable<KernelProxyOptions['proxy']> = {
    url: `${proxy.protocol}://${proxy.host}:${proxy.port}`,
  };
  if (proxy.auth?.username !== undefined) out.username = proxy.auth.username;
  if (proxy.auth?.password !== undefined) out.password = proxy.auth.password;
  if (typeof noProxy === 'string' && noProxy.length > 0) out.bypassHosts = noProxy;
  return { proxy: out };
}

export function buildKernelConnectionOptions(options: ConnectionOptions): KernelNativeConnectionOptions {
  const { authType } = options as { authType?: string };

  const base: {
    hostName: string;
    httpPath: string;
    intervalsAsString: boolean;
    maxConnections?: number;
  } & KernelTlsOptions &
    KernelHttpOptions &
    KernelProxyOptions &
    KernelFederationOptions = {
    hostName: options.host,
    httpPath: prependSlash(options.path),
    // Match the NodeJS Thrift driver, which surfaces INTERVAL columns as
    // strings. The kernel defaults to native Arrow interval/duration types;
    // forcing the string rendering here keeps the kernel path a byte-compatible
    // drop-in. Complex types are intentionally left at the kernel default
    // (native Arrow) — they already decode identically to Thrift via the
    // shared Arrow converter, so `complexTypesAsJson` is not forced on.
    intervalsAsString: true,
    // TLS knobs (server-cert verification toggle + custom CA + mTLS client
    // identity). Validated and normalised (string PEM → Buffer) here so the
    // napi shape only sees a Buffer.
    ...buildKernelTlsOptions(options),
    // HTTP headers (caller `customHeaders` + composed `User-Agent`).
    ...buildKernelHttpOptions(options),
    // HTTP(S) proxy — the same `ConnectionOptions.proxy` the Thrift path uses.
    ...buildKernelProxyOptions(options),
  };

  // kernel-only pool sizing; read via cast to match how this function reads the
  // other kernel-specific options (TLS) — they live on the internal options
  // surface, not the published public `ConnectionOptions` `.d.ts`.
  const { maxConnections } = options as ConnectionOptions & InternalConnectionOptions;
  if (maxConnections !== undefined) {
    if (!Number.isInteger(maxConnections) || maxConnections < 1) {
      throw new HiveDriverError(
        `kernel backend: \`maxConnections\` must be a positive integer; got ${maxConnections}.`,
      );
    }
    if (maxConnections > MAX_U32) {
      throw new HiveDriverError(
        `kernel backend: \`maxConnections\` exceeds the napi u32 limit (${MAX_U32}); got ${maxConnections}. ` +
          'Typical pool sizes are 10-500.',
      );
    }
    base.maxConnections = maxConnections;
  }

  const oauth = options as {
    oauthClientId?: string;
    oauthClientSecret?: string;
    oauthScopes?: Array<string>;
    azureTenantId?: string;
    useDatabricksOAuthInAzure?: boolean;
    persistence?: unknown;
  };

  if (authType === undefined || authType === 'access-token') {
    const { token } = options as { token?: string };
    if (typeof token !== 'string' || isBlankOrReserved(token)) {
      throw new AuthenticationError(
        "kernel backend: a non-empty PAT must be supplied via `token` when using `authType: 'access-token'`.",
      );
    }
    if (oauth.oauthClientId !== undefined || oauth.oauthClientSecret !== undefined) {
      throw new HiveDriverError(
        'kernel backend: cannot supply both `token` and `oauthClientId`/`oauthClientSecret` ' +
          "on the same connection. Pick one: 'access-token' (PAT) uses `token`; " +
          "'databricks-oauth' uses the OAuth fields.",
      );
    }
    return { ...base, authMode: 'Pat', token };
  }

  if (authType === 'static-token') {
    const { staticToken, federationClientId } = options as {
      staticToken?: string;
      federationClientId?: string;
    };
    if (typeof staticToken !== 'string' || isBlankOrReserved(staticToken)) {
      throw new AuthenticationError(
        "kernel backend: a non-empty token must be supplied via `staticToken` when using `authType: 'static-token'`.",
      );
    }
    if (oauth.oauthClientId !== undefined || oauth.oauthClientSecret !== undefined) {
      throw new HiveDriverError(
        'kernel backend: cannot supply `staticToken` alongside `oauthClientId`/`oauthClientSecret` ' +
          'on the same connection. Pick one auth mode.',
      );
    }
    base.identityFederationClientId = federationClientId || undefined;
    return { ...base, authMode: 'Pat', token: staticToken };
  }

  if (authType === 'databricks-oauth') {
    if ((options as { token?: string }).token !== undefined) {
      throw new HiveDriverError(
        "kernel backend: cannot supply `token` alongside `authType: 'databricks-oauth'`. " +
          "Use `authType: 'access-token'` for PAT, or omit `token` to use OAuth.",
      );
    }

    // Azure Entra-direct **M2M** → the kernel's dedicated azure-sp-m2m. Closely
    // mirroring the Thrift driver's `OAuthManager.getManager`, an Azure host with
    // `useDatabricksOAuthInAzure` NOT set to true (the Entra-direct default) plus a
    // secret is an Entra service-principal client-credentials flow: the Entra SP
    // credentials ride the generic `oauthClientId` / `oauthClientSecret` (Thrift
    // convention); forward them as `azureClientId` / `azureClientSecret`.
    // `azureTenantId` is optional — the kernel auto-discovers it from the workspace
    // `/aad/auth` redirect when omitted.
    //
    // Azure **U2M** is deliberately NOT special-cased and NOT rejected. The kernel
    // runs a single, cloud-blind in-house U2M flow: it uses the workspace's
    // OIDC-discovered authorize endpoint (`{host}/oidc/v1/authorize`) verbatim, and
    // that in-house workspace-federated flow works against Azure workspaces (the
    // workspace federates the browser login to Entra server-side; verified E2E). So
    // ALL U2M — including Azure, with or without `useDatabricksOAuthInAzure` — falls
    // through to the standard `OAuthU2m` path below, which forwards the in-house app
    // (`databricks-sql-connector`) + `sql offline_access` scopes, exactly like
    // AWS/GCP. Handing the kernel the Thrift Azure Entra-direct app / scope instead
    // would derail its in-house flow to a broken AAD authorize URL.
    //
    // One deliberate divergence from Thrift: `isAzureHost` uses the full suffix
    // superset (incl. `.databricks.azure.us`) for every branch, whereas Thrift's
    // `useDatabricksOAuthInAzure`-true arm omits `.databricks.azure.us` and so
    // throws `OAuth is not supported` for a US-gov host in that mode. Here such a
    // host falls through to the in-house flow (accepted) instead — intentional,
    // since the kernel's in-house flow is cloud-blind and reachable everywhere.
    // The `oauthClientSecret !== undefined` check is inline so TypeScript narrows
    // the field to `string` inside the branch (for the AzureSpM2m literal below).
    if (
      isAzureHost(options.host) &&
      oauth.useDatabricksOAuthInAzure !== true &&
      oauth.oauthClientSecret !== undefined
    ) {
      // Entra-direct SP M2M is a client-credentials flow (no refresh token), so
      // `persistence` is rejected here for parity with the workspace-OIDC M2M and
      // U2M arms below (and matching the contract docblock's "persistence on M2M
      // → rejected" note). Otherwise a caller's hook would be silently dropped.
      if (oauth.persistence !== undefined) {
        throw new HiveDriverError(
          'kernel backend: `persistence` is not supported on Azure service-principal M2M ' +
            '(M2M tokens have no refresh token; the kernel re-issues on expiry).',
        );
      }
      // Reject a present-but-degenerate secret (`''`, whitespace, or the reserved
      // `'undefined'`/`'null'` shell-export strings) up front. Unlike the generic
      // `OAuthM2m` arm below — which forwards such values verbatim for byte-for-byte
      // Thrift parity — this Azure arm has no parity contract (it already rejects a
      // missing id outright), so a blank credential is as unusable as a missing one
      // and would only surface an opaque Entra `invalid_client` downstream.
      const azureClientSecret = oauth.oauthClientSecret;
      if (isBlankOrReserved(azureClientSecret)) {
        throw new HiveDriverError(
          'kernel backend: Azure service-principal M2M requires a non-blank `oauthClientSecret` ' +
            '(the Entra app-registration client secret).',
        );
      }
      const azureClientId = oauth.oauthClientId;
      if (typeof azureClientId !== 'string' || isBlankOrReserved(azureClientId)) {
        throw new HiveDriverError(
          'kernel backend: Azure service-principal M2M requires `oauthClientId` (the Entra ' +
            'app-registration client id) alongside `oauthClientSecret`.',
        );
      }
      const azure = {
        ...base,
        authMode: 'AzureSpM2m' as const,
        azureClientId,
        azureClientSecret,
      };
      // Forward `azureTenantId` only when it's a real value. A blank/reserved
      // string (`''`, whitespace, `'undefined'`/`'null'` shell-export artifacts)
      // is treated as omitted so the kernel auto-discovers the tenant from the
      // workspace `/aad/auth` redirect, rather than being handed a degenerate
      // tenant that suppresses discovery and yields a malformed AAD URL. Matches
      // this arm's `oauthClientId`/`oauthClientSecret` hygiene above and the
      // Thrift `AzureOAuthManager` empty-tenant fallback.
      return oauth.azureTenantId !== undefined && !isBlankOrReserved(oauth.azureTenantId)
        ? { ...azure, azureTenantId: oauth.azureTenantId }
        : azure;
    }

    // Flow selector + client-id resolution mirror the Thrift driver EXACTLY
    // (`DBSQLClient.createAuthProvider`, DBSQLClient.ts:220):
    //   flow     = oauthClientSecret === undefined ? U2M : M2M   (strict undefined)
    //   clientId = oauthClientId ?? defaultClientId              (`??` guards null/undefined only)
    // No blank/reserved normalization on the OAuth fields — a present-but-
    // degenerate value (`""`, `"undefined"`, whitespace) is forwarded verbatim,
    // exactly as Thrift forwards it, so the kernel path does not diverge from the
    // Thrift backend. (This intentionally re-imports Thrift's env-stringification
    // behaviour: a secret that resolved to `""`/`"undefined"` counts as a real
    // secret ⇒ M2M, just like Thrift.)
    if (oauth.oauthClientSecret === undefined) {
      // U2M (browser) — no secret, exactly like Thrift.
      if (oauth.persistence !== undefined) {
        throw new HiveDriverError(
          'kernel backend: `persistence` (custom OAuth token store) is not yet wired through ' +
            'to the kernel — requires `AuthConfig::External` plumbing. ' +
            'Today the kernel auto-persists U2M tokens to ' +
            '`~/.config/databricks-sql-kernel/oauth/` which works for the standard flow; ' +
            "the JS-supplied hook (matching thrift's `OAuthPersistence` interface) lands " +
            'when the kernel exposes it.',
        );
      }
      const u2m = {
        ...base,
        authMode: 'OAuthU2m' as const,
        oauthRedirectPort: U2M_DEFAULT_REDIRECT_PORT,
        // Scopes default to Thrift parity (`sql offline_access`); overridable.
        oauthScopes:
          Array.isArray(oauth.oauthScopes) && oauth.oauthScopes.length > 0 ? oauth.oauthScopes : U2M_DEFAULT_SCOPES,
      };
      // clientId: Thrift uses `oauthClientId ?? default`. Forward it verbatim
      // when set; when absent the napi applies the same default
      // (`databricks-sql-connector`), so omitting it is identical to Thrift.
      return oauth.oauthClientId !== undefined ? { ...u2m, oauthClientId: oauth.oauthClientId } : u2m;
    }

    // M2M (client credentials) — a secret is present, exactly like Thrift.
    if (oauth.persistence !== undefined) {
      throw new HiveDriverError(
        'kernel backend: `persistence` is not supported on OAuth M2M ' +
          '(M2M tokens have no refresh token; the kernel re-issues on expiry).',
      );
    }
    return {
      ...base,
      authMode: 'OAuthM2m',
      // Thrift: `getClientId()` = `oauthClientId ?? defaultClientId`.
      oauthClientId: oauth.oauthClientId ?? DEFAULT_OAUTH_CLIENT_ID,
      oauthClientSecret: oauth.oauthClientSecret,
      // Configurable (parity with pyo3); defaults to `['all-apis']`.
      oauthScopes:
        Array.isArray(oauth.oauthScopes) && oauth.oauthScopes.length > 0 ? oauth.oauthScopes : M2M_DEFAULT_SCOPES,
    };
  }

  throw new HiveDriverError(
    `kernel backend: unsupported auth mode '${authType}'. ` +
      "Supported modes on the kernel backend today: 'access-token' (PAT), 'static-token', and 'databricks-oauth' " +
      '(M2M with oauthClientId+oauthClientSecret, or U2M with neither).',
  );
}
