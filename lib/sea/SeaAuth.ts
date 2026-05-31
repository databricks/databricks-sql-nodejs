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
import prependSlash from '../utils/prependSlash';
import { SeaConnectionOptions } from './SeaNativeLoader';

/**
 * Shape consumed by the napi-binding's `openSession()`. M0 sends the PAT
 * triple plus optional session defaults, so we `Pick` those fields off the
 * binding's generated `ConnectionOptions` (re-exported as
 * `SeaConnectionOptions`) rather than re-declaring them — if the kernel renames
 * one of these fields this stops compiling instead of silently drifting.
 */
export type SeaNativeConnectionOptions = Pick<
  SeaConnectionOptions,
  'hostName' | 'httpPath' | 'token' | 'catalog' | 'schema' | 'sessionConf'
>;

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
  // Reject whitespace / control characters in the PAT. The kernel's
  // reqwest `HeaderValue` already hard-rejects CR/LF/NUL at build time so
  // this isn't a header-injection fix — it's parity with the Python
  // driver (auth_bridge.py rejects `[\x00-\x20\x7f]`) and catches
  // copy-paste whitespace before a confusing downstream failure.
  // eslint-disable-next-line no-control-regex
  if (/[\x00-\x20\x7f]/.test(token)) {
    throw new AuthenticationError(
      'SEA backend: the PAT supplied via `token` must not contain whitespace or control characters.',
    );
  }

  return {
    hostName: options.host,
    httpPath: prependSlash(options.path),
    token,
  };
}
