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
 * Shape consumed by the napi-binding's `openSession()` (see
 * `native/sea/index.d.ts`). M0 supports PAT only — `token` is required.
 *
 * Mirrors `ConnectionOptions` in the binding's `.d.ts`; declared locally
 * to avoid coupling the JS-side adapter to the auto-generated TS file.
 */
export interface SeaNativeConnectionOptions {
  hostName: string;
  httpPath: string;
  token: string;
}

function prependSlash(str: string): string {
  if (str.length > 0 && str.charAt(0) !== '/') {
    return `/${str}`;
  }
  return str;
}

/**
 * Validate that the user-supplied `ConnectionOptions` describe a PAT auth
 * configuration and build the napi-binding's connection-options shape.
 *
 * M0 SCOPE: PAT only.
 *   - Accepts `authType: 'access-token'` and the undefined-authType default
 *     (which already means PAT throughout the existing driver — see
 *     `DBSQLClient.createAuthProvider`).
 *   - Rejects every other `authType` discriminant with a clear
 *     "M0 supports only PAT" message so callers know OAuth / Federation /
 *     custom providers land in M1.
 *
 * Throws:
 *   - `AuthenticationError` when the auth mode is PAT but `token` is missing
 *     or empty.
 *   - `HiveDriverError` when the auth mode is anything other than PAT.
 */
export function buildSeaConnectionOptions(options: ConnectionOptions): SeaNativeConnectionOptions {
  const { authType } = options as { authType?: string };

  if (authType !== undefined && authType !== 'access-token') {
    throw new HiveDriverError(
      `SEA backend (M0) supports only PAT auth (authType: 'access-token'); ` +
        `got authType: '${authType}'. Other auth modes (databricks-oauth, ` +
        `token-provider, external-token, static-token, custom) will land in M1.`,
    );
  }

  // PAT path — at this point `options` is structurally the access-token branch
  // of `AuthOptions`, which guarantees a `token` field at the type level. We
  // still defensively re-check because the public ConnectionOptions type
  // permits `authType: undefined` with no token at runtime.
  const { token } = options as { token?: string };
  if (typeof token !== 'string' || token.length === 0) {
    throw new AuthenticationError(
      'SEA backend: a non-empty PAT must be supplied via `token` when using `authType: \'access-token\'`.',
    );
  }

  return {
    hostName: options.host,
    httpPath: prependSlash(options.path),
    token,
  };
}
