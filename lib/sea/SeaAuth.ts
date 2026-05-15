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
export type SeaNativeConnectionOptions =
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
    };

function prependSlash(str: string): string {
  if (str.length > 0 && str.charAt(0) !== '/') {
    return `/${str}`;
  }
  return str;
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
 *     exchange, and re-auth on expiry internally (no caching needed — M2M
 *     never has a refresh token; see `auth/oauth/m2m.rs` and the thrift
 *     parity note at `OAuthManager.ts:178-181`).
 *   - OAuth U2M: `authType: 'databricks-oauth'` + NO `oauthClientSecret`.
 *     Kernel runs the PKCE auth-code dance (opens a browser, listens on
 *     localhost:8030, exchanges the code, persists to
 *     `~/.config/databricks-sql-kernel/oauth/{sha256}.json`). The flow
 *     selector matches thrift at `DBSQLClient.ts:143` —
 *     `oauthClientSecret defined ? M2M : U2M`.
 *
 * Out of scope on the OAuth paths (rejected with a clear error):
 *   - `azureTenantId` / `useDatabricksOAuthInAzure` → Microsoft Entra
 *     direct flow with `<tenantId>/.default` scope rewrite. The kernel
 *     uses workspace-OIDC discovery (which works against Azure workspaces
 *     too — they serve `/oidc/.well-known/...`); Entra-direct is a
 *     follow-on M1 Phase 2 task.
 *   - `persistence` on either flavor — for M2M the kernel doesn't cache
 *     (re-issuing is cheap; M2M has no refresh token). For U2M, custom
 *     persistence requires the kernel to expose `AuthConfig::External`
 *     (M1 Phase 2 task). The kernel-internal disk cache works for the
 *     standard flow today.
 *
 * Throws:
 *   - `AuthenticationError` for missing required credentials.
 *   - `HiveDriverError` for unsupported auth modes / Azure-direct /
 *     custom persistence.
 */
export function buildSeaConnectionOptions(options: ConnectionOptions): SeaNativeConnectionOptions {
  const { authType } = options as { authType?: string };

  const base = {
    hostName: options.host,
    httpPath: prependSlash(options.path),
  };

  if (authType === undefined || authType === 'access-token') {
    const { token } = options as { token?: string };
    if (typeof token !== 'string' || token.length === 0) {
      throw new AuthenticationError(
        'SEA backend: a non-empty PAT must be supplied via `token` when using `authType: \'access-token\'`.',
      );
    }
    return { ...base, authMode: 'Pat', token };
  }

  if (authType === 'databricks-oauth') {
    const oauth = options as {
      oauthClientId?: string;
      oauthClientSecret?: string;
      azureTenantId?: string;
      useDatabricksOAuthInAzure?: boolean;
      persistence?: unknown;
    };

    if (oauth.azureTenantId !== undefined || oauth.useDatabricksOAuthInAzure === true) {
      throw new HiveDriverError(
        'SEA backend: Azure-direct OAuth (azureTenantId / useDatabricksOAuthInAzure) ' +
          'is a later M1 task; the kernel uses workspace-OIDC discovery today, ' +
          'which works against Azure workspaces with no extra options.',
      );
    }

    // Flow selector mirrors thrift's `DBSQLClient.createAuthProvider`
    // (`DBSQLClient.ts:143`): `oauthClientSecret defined ? M2M : U2M`.
    if (oauth.oauthClientSecret === undefined) {
      // U2M.
      if (oauth.persistence !== undefined) {
        throw new HiveDriverError(
          'SEA backend: `persistence` (custom OAuth token store) is not yet wired through ' +
            'to the kernel — requires `AuthConfig::External` plumbing planned for M1 Phase 2. ' +
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
    if (typeof oauth.oauthClientId !== 'string' || oauth.oauthClientId.length === 0) {
      throw new AuthenticationError('SEA backend: `oauthClientId` is required for OAuth M2M.');
    }
    if (typeof oauth.oauthClientSecret !== 'string' || oauth.oauthClientSecret.length === 0) {
      throw new AuthenticationError(
        'SEA backend: `oauthClientSecret` must be a non-empty string for OAuth M2M.',
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
      `Supported modes today: 'access-token' (PAT), 'databricks-oauth' (M2M + U2M). ` +
      `Other modes (token-provider, external-token, static-token, custom) are M1+ follow-ups.`,
  );
}
