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
 * The napi shim is required LAZILY on first `getSeaNative()` call so
 * importing this module is free for callers that never reach a SEA
 * code path. Concretely: a test or build that exercises only the
 * Thrift backend, or a Mocha file evaluation that only checks an
 * env-var skip-gate, does not need the platform-specific `.node`
 * artifact at module-load time. The eager `require()` we previously
 * had at the top of this file crashed test discovery with
 * `MODULE_NOT_FOUND` before `before()` skip-gates could fire — a
 * defect that propagated forward into every e2e file that imported
 * anything from this module (DA round-1 H1 against F2 and F4).
 *
 * The require result is memoised on first call so subsequent
 * `getSeaNative()` calls are O(1). Failures are surfaced verbatim:
 * the napi-rs auto-generated shim already produces a descriptive
 * `MODULE_NOT_FOUND` listing the platform-arch tuple it tried, which
 * is the actionable diagnostic for "run `yarn build:native` first."
 * The structured platform/arch diagnostics planned in the file's
 * earlier doc-comment remain a follow-on; the lazy-load addresses
 * the immediate defect.
 */

import type { SeaNativeConnectionOptions } from './SeaAuth';

// Memoised require slot. `undefined` = not yet loaded; on first
// `getSeaNative()` we resolve and cache. `null` is not used —
// failures throw out of the require call rather than yielding null.
//
// The path is relative to this file at runtime
// (`dist/sea/SeaNativeLoader.js`) resolving to
// `dist/sea/../../native/sea/index.js` once `tsc` has emitted to
// `dist/`. The napi shim is plain CommonJS, not part of the TS source
// tree, so the path is require-time resolved.
let nativeBindingCache: SeaNativeBinding | undefined;

/**
 * Arrow IPC payload returned by `Statement.fetchNextBatch()`. Carries a
 * complete Arrow IPC stream (schema header + 1 record-batch message).
 */
export interface SeaArrowBatch {
  ipcBytes: Buffer;
}

/**
 * Arrow IPC payload returned by `Statement.schema()` (schema header only).
 */
export interface SeaArrowSchema {
  ipcBytes: Buffer;
}

/**
 * Typed surface for the opaque napi `Statement` handle. Method signatures
 * match `native/sea/index.d.ts` exactly so the JS-side wrappers can
 * `await` them without `any` casts.
 */
export interface SeaNativeStatement {
  fetchNextBatch(): Promise<SeaArrowBatch | null>;
  schema(): Promise<SeaArrowSchema>;
  cancel(): Promise<void>;
  close(): Promise<void>;
}

/**
 * Typed surface for the opaque napi `Connection` handle.
 */
export interface SeaNativeConnection {
  /**
   * Execute a SQL statement. Catalog / schema / sessionConf are
   * session-level — set on `openSession`, applied to every statement
   * executed on the resulting `Connection`. No per-statement options.
   */
  executeStatement(sql: string): Promise<SeaNativeStatement>;
  close(): Promise<void>;
}

/**
 * Public surface of the native binding exposed to the rest of the
 * NodeJS driver. Round 2 lands `openSession` + opaque `Connection` /
 * `Statement` classes (the binding-generated `.d.ts` is the source of
 * truth for their method signatures — see `native/sea/index.d.ts`).
 */
export interface SeaNativeBinding {
  /** Returns the native crate version (smoke test for the binding's load path). */
  version(): string;
  /**
   * Open a session over PAT, OAuth M2M, or OAuth U2M auth. Returns an
   * opaque Connection. The discriminated `SeaNativeConnectionOptions`
   * union from `SeaAuth` is the source of truth for the per-mode
   * required fields, so the loader-seam enforces the same compile-time
   * check the adapter applies — a caller can't bypass
   * `buildSeaConnectionOptions` and pass, say, `{ authMode: 'Pat' }`
   * with no token.
   */
  openSession(opts: SeaNativeConnectionOptions): Promise<SeaNativeConnection>;
  /** Opaque Connection class — instance methods on the binding-generated d.ts. */
  Connection: Function;
  /** Opaque Statement class — instance methods on the binding-generated d.ts. */
  Statement: Function;
}

/**
 * Returns the loaded native binding. Throws if the platform-specific
 * `.node` artifact cannot be found (napi-rs's auto-generated shim
 * surfaces a descriptive error in that case).
 *
 * Lazy + memoised: the first call resolves
 * `../../native/sea/index.js` and caches the result; subsequent calls
 * are O(1). Importing this module no longer eagerly loads the .node
 * artifact, so callers that never reach a SEA code path don't pay the
 * load cost or crash if the artifact is absent (DA round-1 H1 fixup).
 */
export function getSeaNative(): SeaNativeBinding {
  if (nativeBindingCache === undefined) {
    // The `.js` extension is required: the napi-rs shim is plain
    // CommonJS, not a TS source file, so the extension cannot be
    // resolved away. The full eslint disable comment covers the
    // file-extension, dynamic-require, var-requires, and global-require
    // rules that all flag this otherwise-necessary pattern.
    // eslint-disable-next-line @typescript-eslint/no-var-requires, import/no-dynamic-require, global-require, import/extensions
    nativeBindingCache = require('../../native/sea/index.js') as SeaNativeBinding;
  }
  return nativeBindingCache;
}

/**
 * Convenience accessor for the smoke-test path. Equivalent to
 * `getSeaNative().version()` but reads more naturally in tests and
 * REPLs.
 */
export function version(): string {
  return getSeaNative().version();
}
