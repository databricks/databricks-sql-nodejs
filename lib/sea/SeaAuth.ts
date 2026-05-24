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
import AuthenticationError from '../errors/AuthenticationError';
import HiveDriverError from '../errors/HiveDriverError';

/**
 * Default local listener port for the U2M authorization-code callback.
 * Hardcoded here so the override of the kernel default (8020) to the
 * thrift default (8030) is invariant for SEA callers — preserving parity
 * with the existing Node driver. Not exposed on the public
 * `ConnectionOptions` (thrift hides `callbackPorts` from its public
 * surface too — see nodejs-thrift-expert survey §B.2).
 */
const U2M_DEFAULT_REDIRECT_PORT = 8030;

/**
 * Shape consumed by the napi-binding's `openSession()` (see
 * `native/sea/index.d.ts`). Mirrors `ConnectionOptions` in the binding's
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
 * of importing `AuthMode` from `native/sea/index.d.ts` because that
 * file declares `AuthMode` as `export const enum`, which is
 * incompatible with `isolatedModules` and a runtime-coupling hazard.
 * The Rust source of truth lives at `native/sea/src/database.rs`.
 */
/**
 * Session-level defaults shared across all auth-mode variants.
 *
 * Mirrors `ConnectionOptions.catalog` / `.schema` / `.sessionConf` on
 * the napi binding (kernel `Session::builder().defaults(DefaultOpts)`
 * and `.session_conf(HashMap)` — the routes that actually populate SEA
 * `CreateSession.catalog` / `.schema` / `.session_confs`).
 *
 * Per-statement overrides do not exist on the kernel surface; both
 * pyo3 and napi expose catalog / schema / sessionConf only at session
 * creation. Mirror that here so the adapter doesn't promise a
 * capability the binding can't honour.
 */
export interface SeaSessionDefaults {
  catalog?: string;
  schema?: string;
  sessionConf?: Record<string, string>;
}

export type SeaNativeConnectionOptions = SeaSessionDefaults &
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
      }
    | {
        hostName: string;
        httpPath: string;
        authMode: 'OAuthU2m';
        oauthRedirectPort: number;
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
 *     to `~/.config/databricks-sql-kernel/oauth/{sha256}.json`). The flow
 *     selector keys off `oauthClientId` presence: present → M2M, absent →
 *     U2M. (Round-4 NF3-2 fix; previously secret-keyed — that variant
 *     routed a typo'd-secret M2M call to the U2M arm and swallowed the
 *     actionable error.) Mirrors thrift's intent at `DBSQLClient.ts:143`.
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
export function buildSeaConnectionOptions(options: ConnectionOptions): SeaNativeConnectionOptions {
  const { authType } = options as { authType?: string };

  const base = {
    hostName: options.host,
    httpPath: prependSlash(options.path),
  };

  const oauth = options as {
    oauthClientId?: string;
    oauthClientSecret?: string;
    azureTenantId?: string;
    useDatabricksOAuthInAzure?: boolean;
    persistence?: unknown;
  };

  if (authType === undefined || authType === 'access-token') {
    const { token } = options as { token?: string };
    if (typeof token !== 'string' || isBlankOrReserved(token)) {
      throw new AuthenticationError(
        'SEA backend: a non-empty PAT must be supplied via `token` when using `authType: \'access-token\'`.',
      );
    }
    if (oauth.oauthClientId !== undefined || oauth.oauthClientSecret !== undefined) {
      throw new HiveDriverError(
        'SEA backend: cannot supply both `token` and `oauthClientId`/`oauthClientSecret` ' +
          "on the same connection. Pick one: 'access-token' (PAT) uses `token`; " +
          "'databricks-oauth' uses the OAuth fields.",
      );
    }
    return { ...base, authMode: 'Pat', token };
  }

  if (authType === 'databricks-oauth') {
    if ((options as { token?: string }).token !== undefined) {
      throw new HiveDriverError(
        "SEA backend: cannot supply `token` alongside `authType: 'databricks-oauth'`. " +
          "Use `authType: 'access-token'` for PAT, or omit `token` to use OAuth.",
      );
    }

    if (oauth.azureTenantId !== undefined || oauth.useDatabricksOAuthInAzure === true) {
      throw new HiveDriverError(
        'SEA backend: Azure-direct OAuth (azureTenantId / useDatabricksOAuthInAzure) ' +
          'is not supported. The workspace-OIDC discovery path handles Azure workspaces ' +
          'today without these options.',
      );
    }

    // Flow selector mirrors thrift's `DBSQLClient.createAuthProvider`
    // (`DBSQLClient.ts:143`): presence of `oauthClientId` indicates M2M
    // intent, otherwise U2M. Routing decision is based on `oauthClientId`
    // (the "do I have an id?" signal) rather than the secret, so a
    // user who set an id but typoed/forgot the secret gets the M2M
    // "secret is required" error instead of a U2M error that hides
    // their actual intent. The U2M arm still defends against an id
    // sneaking through: fires only when `oauthClientId` is provided as
    // a blank-reserved literal (e.g., whitespace, `"null"`, `"undefined"`)
    // alongside an absent/blank secret — both `idIsBlank` and
    // `secretIsBlank` are true so U2M wins routing, but the caller's
    // intent to use U2M with a partially-set id is ambiguous and
    // rejected explicitly.
    const idIsBlank =
      oauth.oauthClientId === undefined ||
      (typeof oauth.oauthClientId === 'string' && isBlankOrReserved(oauth.oauthClientId));
    const secretIsBlank =
      oauth.oauthClientSecret === undefined ||
      (typeof oauth.oauthClientSecret === 'string' && isBlankOrReserved(oauth.oauthClientSecret));

    if (idIsBlank && secretIsBlank) {
      // U2M — neither id nor secret supplied.
      if (oauth.oauthClientId !== undefined) {
        // Defense-in-depth: id was set but blank/reserved literal.
        // The kernel hardcodes `client_id = "databricks-cli"` for U2M;
        // there's no JS-side override knob.
        throw new HiveDriverError(
          'SEA backend: `oauthClientId` is not supported on the OAuth U2M flow; ' +
            "the kernel uses the built-in 'databricks-cli' client. " +
            'Omit `oauthClientId` for U2M, or supply `oauthClientSecret` for the M2M flow.',
        );
      }
      if (oauth.persistence !== undefined) {
        throw new HiveDriverError(
          'SEA backend: `persistence` (custom OAuth token store) is not yet wired through ' +
            'to the kernel — requires `AuthConfig::External` plumbing. ' +
            'Today the kernel auto-persists U2M tokens to ' +
            '`~/.config/databricks-sql-kernel/oauth/` which works for the standard flow; ' +
            "the JS-supplied hook (matching thrift's `OAuthPersistence` interface) lands " +
            'when the kernel exposes it.',
        );
      }
      return {
        ...base,
        authMode: 'OAuthU2m',
        oauthRedirectPort: U2M_DEFAULT_REDIRECT_PORT,
      };
    }

    // M2M.
    if (typeof oauth.oauthClientId !== 'string' || isBlankOrReserved(oauth.oauthClientId)) {
      throw new AuthenticationError(
        'SEA backend: `oauthClientId` is required (non-empty, non-whitespace) for OAuth M2M.',
      );
    }
    if (typeof oauth.oauthClientSecret !== 'string' || isBlankOrReserved(oauth.oauthClientSecret)) {
      throw new AuthenticationError(
        'SEA backend: `oauthClientSecret` must be a non-empty non-whitespace string for OAuth M2M.',
      );
    }
    if (oauth.persistence !== undefined) {
      throw new HiveDriverError(
        'SEA backend: `persistence` is not supported on OAuth M2M ' +
          '(M2M tokens have no refresh token; the kernel re-issues on expiry).',
      );
    }
    return {
      ...base,
      authMode: 'OAuthM2m',
      oauthClientId: oauth.oauthClientId,
      oauthClientSecret: oauth.oauthClientSecret,
    };
  }

  throw new HiveDriverError(
    `SEA backend: unsupported auth mode '${authType}'. ` +
      "Supported modes on the SEA backend today: 'access-token' (PAT) and 'databricks-oauth' " +
      '(M2M with oauthClientId+oauthClientSecret, or U2M with neither).',
  );
}
