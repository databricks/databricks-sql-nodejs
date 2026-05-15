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

/**
 * Loader for the SEA (Statement Execution API) native binding.
 *
 * Round 1b: minimal pass-through to the napi-rs auto-generated
 * `index.js` shim in `native/sea/`. The shim itself picks the right
 * per-platform `.node` artifact (linux-x64-gnu today; more triples in
 * the bundling feature).
 *
 * Round 2+ will extend this with: lazy require to defer the `.node`
 * load until the first SEA call, structured load-error diagnostics
 * (which platform/arch was attempted, whether the package was
 * installed at all), and a JS-side `DBSQLLogger` install path that
 * forwards to the binding's `installLogger()` once that surface lands.
 */

// The path is relative to this file at runtime (`dist/sea/SeaNativeLoader.js`)
// resolving to `dist/sea/../../native/sea/index.js` once `tsc` has emitted
// to `dist/`. We use a require-time path resolution because the napi
// shim is plain CommonJS and not part of the TS source tree.
//
// eslint-disable-next-line @typescript-eslint/no-var-requires, import/no-dynamic-require, global-require
const native = require('../../native/sea/index.js');

/**
 * Public surface of the native binding exposed to the rest of the
 * NodeJS driver. Round 2 lands `openSession` + opaque `Connection` /
 * `Statement` classes (the binding-generated `.d.ts` is the source of
 * truth for their method signatures — see `native/sea/index.d.ts`).
 *
 * We deliberately keep this typed loosely (`unknown` for the class
 * shapes) so the loader layer doesn't have to import the binding's
 * generated types and the JS adapter layer can introduce its own
 * higher-level wrappers without conflicting with the binding's TS
 * declarations.
 */
export interface SeaNativeBinding {
  /** Returns the native crate version (smoke test for the binding's load path). */
  version(): string;
  /** Open a session over PAT auth. Returns an opaque Connection. */
  openSession(opts: {
    hostName: string;
    httpPath: string;
    token: string;
  }): Promise<unknown>;
  /** Opaque Connection class — instance methods on the binding-generated d.ts. */
  Connection: Function;
  /** Opaque Statement class — instance methods on the binding-generated d.ts. */
  Statement: Function;
}

/**
 * Returns the loaded native binding. Throws if the platform-specific
 * `.node` artifact cannot be found (napi-rs's auto-generated shim
 * surfaces a descriptive error in that case).
 */
export function getSeaNative(): SeaNativeBinding {
  return native as SeaNativeBinding;
}

/**
 * Convenience accessor for the smoke-test path. Equivalent to
 * `getSeaNative().version()` but reads more naturally in tests and
 * REPLs.
 */
export function version(): string {
  return getSeaNative().version();
}
