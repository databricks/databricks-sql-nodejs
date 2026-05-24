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
 * Lazy loader for the SEA (Statement Execution API) native binding.
 *
 * Mirrors the load-failure-tolerant pattern of `lib/utils/lz4.ts`: the
 * `.node` artifact ships via per-platform optional dependencies
 * (`@databricks/sea-native-<triple>`), so its absence must not crash
 * a Thrift-only consumer of the driver. Callers that actually need
 * SEA invoke `getSeaNative()`, which throws a structured error if
 * the binding could not be loaded.
 */

import type {
  Connection as NativeConnection,
  Statement as NativeStatement,
  ConnectionOptions,
  ExecuteOptions,
  ArrowBatch,
  ArrowSchema,
} from '@sea-native';

export type { ConnectionOptions, ExecuteOptions, ArrowBatch, ArrowSchema };
export type Connection = NativeConnection;
export type Statement = NativeStatement;

export interface SeaNativeBinding {
  version(): string;
  openSession(options: ConnectionOptions): Promise<NativeConnection>;
  Connection: typeof NativeConnection;
  Statement: typeof NativeStatement;
}

const MIN_NODE_MAJOR = 18;

function detectNodeMajor(): number {
  // `process.version` is `vX.Y.Z`; parseInt stops at the first non-digit.
  return parseInt(process.version.slice(1), 10);
}

function loadFailureHint(err: NodeJS.ErrnoException): string {
  const platform = `${process.platform}-${process.arch}`;
  const installHint = `Install the matching optional dependency (e.g. @databricks/sea-native-${platform}).`;
  if (err.code === 'MODULE_NOT_FOUND') {
    return `SEA native binding not installed for platform ${platform} on Node ${process.version}. ${installHint}`;
  }
  if (err.code === 'ERR_DLOPEN_FAILED') {
    return `SEA native binding present but failed to dlopen on platform ${platform} / Node ${process.version} — likely a libc or Node ABI mismatch. The binding requires Node >=${MIN_NODE_MAJOR}.`;
  }
  return `SEA native binding failed to load on platform ${platform} / Node ${process.version}: ${err.message}`;
}

let cached: SeaNativeBinding | null | undefined;
let cachedError: Error | undefined;

function tryLoad(): SeaNativeBinding | undefined {
  const nodeMajor = detectNodeMajor();
  if (Number.isFinite(nodeMajor) && nodeMajor < MIN_NODE_MAJOR) {
    cachedError = new Error(
      `SEA native binding requires Node >=${MIN_NODE_MAJOR}; running Node ${process.version}. Continue using the Thrift backend on this runtime.`,
    );
    return undefined;
  }

  try {
    // The require path resolves to `native/sea/index.js` (the napi-rs
    // router). `.js` is omitted so eslint's `import/extensions` rule
    // accepts the call.
    // eslint-disable-next-line @typescript-eslint/no-var-requires, global-require
    return require('../../native/sea') as SeaNativeBinding;
  } catch (err) {
    if (err instanceof Error && 'code' in err) {
      cachedError = new Error(loadFailureHint(err as NodeJS.ErrnoException));
      return undefined;
    }
    cachedError = new Error(`SEA native binding failed to load with non-standard error: ${String(err)}`);
    return undefined;
  }
}

/**
 * Returns the loaded native binding. Throws a structured error if
 * the binding is unavailable on this platform / Node version.
 */
export function getSeaNative(): SeaNativeBinding {
  if (cached === undefined) {
    cached = tryLoad() ?? null;
  }
  if (cached === null) {
    throw cachedError ?? new Error('SEA native binding unavailable');
  }
  return cached;
}

/**
 * Returns the loaded binding or `undefined` if it could not be
 * loaded. Use this for capability-detection at startup; use
 * `getSeaNative()` at the point where SEA is actually required.
 */
export function tryGetSeaNative(): SeaNativeBinding | undefined {
  if (cached === undefined) {
    cached = tryLoad() ?? null;
  }
  return cached ?? undefined;
}
