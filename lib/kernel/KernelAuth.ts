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
import { buildUserAgentString } from '../utils';

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
}

/**
 * HTTP(S) proxy forwarded to the napi binding's `ConnectionOptions.proxy`
 * (kernel `ProxyConfig.url`). The public `ConnectionOptions.proxy` is the
 * Thrift-shaped `{protocol, host, port, auth}`; `buildKernelProxyOptions`
 * composes a single proxy URL string (with any basic-auth credentials
 * percent-encoded into the `userinfo`) so the SAME connection option works
 * on both backends. The napi contract takes a flat `proxy?: string`.
 */
export interface KernelProxyOptions {
  proxy?: string;
}

export type KernelNativeConnectionOptions = KernelSessionDefaults &
  KernelTlsOptions &
  KernelHttpOptions &
  KernelProxyOptions &
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
  );

function prependSlash(str: string): string {
  if (str.length > 0 && str.charAt(0) !== '/') {
    return `/${str}`;
  }
  return str;
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
 * Normalise a PEM input (`string` or `Buffer`) accepted on the public
 * surface into the `Buffer` the napi shape requires. Does a light,
 * ordered BEGIN…END sanity check so a truncated/headerless blob (or a
 * stray page that merely contains the literals out of order, e.g. a
 * proxy-intercept page) is rejected here rather than surfacing as an
 * opaque kernel TLS error. The bytes are NOT fully parsed in JS — that
 * is deferred to the kernel, which returns a meaningful error on a
 * malformed PEM/key.
 *
 * `kind` selects the expected block: `'certificate'` matches a
 * `CERTIFICATE` block; `'private key'` matches any `… PRIVATE KEY` block
 * (PKCS#8 `PRIVATE KEY`, PKCS#1 `RSA PRIVATE KEY`, SEC1 `EC PRIVATE KEY`).
 *
 * Throws `HiveDriverError` when the value is empty or (for strings)
 * lacks the expected PEM header.
 */
function normalizePemBytes(value: Buffer | string, optionName: string, kind: 'certificate' | 'private key'): Buffer {
  if (typeof value === 'string') {
    const re =
      kind === 'certificate'
        ? /-----BEGIN CERTIFICATE-----[\s\S]+?-----END CERTIFICATE-----/
        : /-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]+?-----END [A-Z0-9 ]*PRIVATE KEY-----/;
    if (!re.test(value)) {
      const expected =
        kind === 'certificate'
          ? "a '-----BEGIN CERTIFICATE-----' … '-----END CERTIFICATE-----' block"
          : "a 'BEGIN … PRIVATE KEY' / 'END … PRIVATE KEY' PEM block (PKCS#8, PKCS#1, or SEC1)";
      throw new HiveDriverError(
        `kernel backend: \`${optionName}\` string does not look like a PEM ${kind} (expected ${expected}). ` +
          'Pass PEM text or a Buffer of PEM bytes.',
      );
    }
    return Buffer.from(value, 'utf8');
  }
  if (Buffer.isBuffer(value)) {
    if (value.length === 0) {
      throw new HiveDriverError(`kernel backend: \`${optionName}\` Buffer is empty.`);
    }
    return value;
  }
  throw new HiveDriverError(`kernel backend: \`${optionName}\` must be a PEM string or a Buffer.`);
}

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
 * - `clientCertPem` / `clientKeyPem` carry the mutual-TLS client identity.
 *   They must be supplied **together** — supplying only one is rejected
 *   here with an actionable error (rather than waiting for the kernel's
 *   `InvalidArgument` at `openSession`). Each accepts a PEM string or
 *   `Buffer`, normalised the same way.
 *
 * Throws `HiveDriverError` when a cert/key is empty, mis-typed, lacks the
 * expected PEM header, or when only one half of the mTLS pair is set.
 */
export function buildKernelTlsOptions(options: ConnectionOptions): KernelTlsOptions {
  // Read the kernel-only fields through the purpose-built internal options type
  // rather than an ad-hoc inline cast, so the shape can't silently drift from
  // its declaration and a typo'd key fails to compile.
  const { checkServerCertificate, checkServerCertificateHostname, customCaCert, clientCertPem, clientKeyPem } =
    options as ConnectionOptions & InternalConnectionOptions;

  const tls: KernelTlsOptions = {};

  if (checkServerCertificate !== undefined) {
    tls.checkServerCertificate = checkServerCertificate;
  }

  if (checkServerCertificateHostname !== undefined) {
    tls.checkServerCertificateHostname = checkServerCertificateHostname;
  }

  if (customCaCert !== undefined) {
    tls.customCaCert = normalizePemBytes(customCaCert, 'customCaCert', 'certificate');
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
    tls.clientCertPem = normalizePemBytes(clientCertPem as Buffer | string, 'clientCertPem', 'certificate');
    tls.clientKeyPem = normalizePemBytes(clientKeyPem as Buffer | string, 'clientKeyPem', 'private key');
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
  const { customHeaders, userAgentEntry } = options;

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

  return { customHeaders: headers };
}

/**
 * Validate the user-supplied `ConnectionOptions` and build the
 * napi-binding's connection-options shape.
 *
 * Supported auth modes:
 *   - PAT: `authType: 'access-token'` (or undefined, which already means
 *     PAT throughout the existing driver — see
 *     `DBSQLClient.createAuthProvider`).
 *   - OAuth M2M: `authType: 'databricks-oauth'` + `oauthClientId` +
 *     `oauthClientSecret`. Kernel handles OIDC discovery, client_credentials
 *     exchange, and re-auth on expiry internally.
 *   - OAuth U2M: `authType: 'databricks-oauth'` + NO `oauthClientId` and
 *     NO `oauthClientSecret`. Kernel runs the PKCE auth-code dance (opens
 *     a browser, listens on localhost:8030, exchanges the code, persists
 *     to `~/.config/databricks-sql-kernel/oauth/{sha256}.json`).
 *
 *     **Flow selection — DELIBERATE DIVERGENCE FROM THRIFT.** Thrift's
 *     `DBSQLClient.createAuthProvider` (`DBSQLClient.ts:216`) keys off the
 *     *secret* (`oauthClientSecret === undefined ? U2M : M2M`), so a custom
 *     `oauthClientId` with no secret runs U2M with that id. kernel instead keys
 *     off `oauthClientId` *presence* (id present → M2M, absent → U2M). The
 *     trade-off: keying off the id means a caller who set an id but
 *     typoed/forgot the secret gets the actionable M2M "secret is required"
 *     error instead of being silently routed to U2M (which would hide their
 *     intent). The cost is two real behavioural gaps vs Thrift:
 *       1. `oauthClientId` + no secret → Thrift runs U2M; kernel throws
 *          `AuthenticationError` (M2M secret required).
 *       2. kernel U2M has NO custom-client-id support — the kernel hardcodes
 *          `client_id = "databricks-cli"`, and kernel rejects any `oauthClientId`
 *          on the U2M arm. Thrift U2M honours a custom `clientId`.
 *     Both are documented limitations of the M0 kernel OAuth surface, not bugs.
 *
 * Out of scope on the OAuth paths (rejected with a clear error):
 *   - `azureTenantId` / `useDatabricksOAuthInAzure` → Microsoft Entra
 *     direct flow. The kernel uses workspace-OIDC discovery (which works
 *     against Azure workspaces too — they serve `/oidc/.well-known/...`)
 *     and does not implement the Entra-direct scope-rewrite path.
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
 *   - `HiveDriverError` for unsupported auth modes / Azure-direct /
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
 * the same shape the Thrift backend accepts) onto the kernel's napi
 * `proxy?: string`. Composes `protocol://[user:pass@]host:port`, percent-
 * encoding any `auth.{username,password}` into the URL `userinfo` so
 * credentials containing reserved characters (`@`, `:`, `/`) survive intact —
 * the kernel parses the userinfo off and applies it as basic-auth. The kernel
 * accepts only `http://` / `https://`; a SOCKS protocol surfaces a clear
 * kernel error at connect (reqwest SOCKS support is not compiled in).
 */
export function buildKernelProxyOptions(options: ConnectionOptions): KernelProxyOptions {
  const { proxy } = options;
  if (!proxy) {
    return {};
  }
  const { username, password } = proxy.auth ?? {};
  const userinfo =
    username !== undefined ? `${encodeURIComponent(username)}:${encodeURIComponent(password ?? '')}@` : '';
  return {
    proxy: `${proxy.protocol}://${userinfo}${proxy.host}:${proxy.port}`,
  };
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
    KernelProxyOptions = {
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

  if (authType === 'databricks-oauth') {
    if ((options as { token?: string }).token !== undefined) {
      throw new HiveDriverError(
        "kernel backend: cannot supply `token` alongside `authType: 'databricks-oauth'`. " +
          "Use `authType: 'access-token'` for PAT, or omit `token` to use OAuth.",
      );
    }

    if (oauth.azureTenantId !== undefined || oauth.useDatabricksOAuthInAzure === true) {
      throw new HiveDriverError(
        'kernel backend: Azure-direct OAuth (azureTenantId / useDatabricksOAuthInAzure) ' +
          'is not supported. The workspace-OIDC discovery path handles Azure workspaces ' +
          'today without these options.',
      );
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
      "Supported modes on the kernel backend today: 'access-token' (PAT) and 'databricks-oauth' " +
      '(M2M with oauthClientId+oauthClientSecret, or U2M with neither).',
  );
}
