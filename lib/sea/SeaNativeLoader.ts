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
 * (`@databricks/sql-kernel-<triple>`), so its absence must not crash
 * a Thrift-only consumer of the driver. Callers that actually need
 * SEA construct a {@link SeaNativeLoader} (or use the process-global
 * {@link getSeaNative}) which throws a structured error if the binding
 * could not be loaded.
 *
 * M0 publishes a single triple (`linux-x64-gnu`); see
 * `native/sea/README.md` for the supported-platform policy.
 */

import type {
  Connection as NativeConnection,
  Statement as NativeStatement,
  ConnectionOptions as NativeConnectionOptions,
  ArrowBatch as NativeArrowBatch,
  ArrowSchema as NativeArrowSchema,
} from '../../native/sea';

// SEA-prefixed re-exports. The kernel-generated `.d.ts` keeps the
// napi-rs default names (`ConnectionOptions`, `ArrowBatch`, …); we
// disambiguate on the TS-wrapper side so these never collide with the
// Thrift-side `ConnectionOptions` (lib/contracts/IDBSQLClient.ts) or
// `ArrowBatch` (lib/result/utils.ts) when imported elsewhere.
export type SeaConnectionOptions = NativeConnectionOptions;
export type SeaArrowBatch = NativeArrowBatch;
export type SeaArrowSchema = NativeArrowSchema;
export type SeaConnection = NativeConnection;
export type SeaStatement = NativeStatement;

// Back-compat aliases for the downstream SEA stack branches that landed
// against the pre-rename loader. The merged kernel (@databricks/sql-kernel)
// moved per-statement catalog/schema/sessionConfig to session-level
// `openSession`, so `ExecuteOptions` no longer exists on the binding;
// `SeaExecuteOptions` is kept as a deprecated shim describing the old
// per-statement shape so the stack keeps compiling. Per-statement options
// are now applied at session creation — see native/sea/README.md.
export type SeaNativeConnection = NativeConnection;
export type SeaNativeStatement = NativeStatement;
export type SeaNativeConnectionOptions = NativeConnectionOptions;
/** @deprecated per-statement options moved to session-level `openSession`. */
export interface SeaExecuteOptions {
  initialCatalog?: string;
  initialSchema?: string;
  sessionConfig?: Record<string, string>;
}

/**
 * The full native binding surface, derived from the generated module
 * so it can never drift from the `.d.ts` contract: when the kernel
 * adds or renames a free function / class, this type follows
 * automatically and `defaultRequire`'s cast stays correct.
 */
export type SeaNativeBinding = typeof import('../../native/sea');

const MIN_NODE_MAJOR = 18;

function detectNodeMajor(): number {
  // `process.version` is `vX.Y.Z`; parseInt stops at the first non-digit.
  return parseInt(process.version.slice(1), 10);
}

function platformLabel(): string {
  return `${process.platform}-${process.arch}`;
}

function loadFailureHint(err: NodeJS.ErrnoException): string {
  const platform = platformLabel();
  // Do not name a concrete package: the published name uses the napi-rs
  // triple (e.g. `-linux-x64-gnu` / `-linux-x64-musl` / `-win32-x64-msvc`),
  // not the bare `${platform}` shown here, so a literal example would
  // 404. Point at the README's supported-triple list instead.
  const installHint =
    'Install the matching @databricks/sql-kernel-* optional dependency for your platform ' +
    '(see native/sea/README.md for the supported triples; M0 ships linux-x64-gnu only).';
  if (err.code === 'MODULE_NOT_FOUND') {
    return `SEA native binding not installed for platform ${platform} on Node ${process.version}. ${installHint}`;
  }
  if (err.code === 'ERR_DLOPEN_FAILED') {
    // Surface the underlying dlerror string (e.g. `GLIBC_2.32 not found`)
    // plus concrete remediation — without it the cause is invisible.
    return (
      `SEA native binding present but failed to dlopen on platform ${platform} / Node ${process.version}: ` +
      `${err.message}. Common causes: glibc/musl mismatch (e.g. Alpine Linux — install the -musl variant), ` +
      `Node ABI mismatch (try \`rm -rf node_modules && npm install\`), or CPU-architecture mismatch. ` +
      `The binding requires Node >=${MIN_NODE_MAJOR}.`
    );
  }
  return `SEA native binding failed to load on platform ${platform} / Node ${process.version}: ${err.message}`;
}

/**
 * Default loader: resolves `native/sea/index.js` (the napi-rs router),
 * which selects the per-platform `.node`. `.js` is omitted so eslint's
 * `import/extensions` rule accepts the call.
 */
function defaultRequire(): SeaNativeBinding {
  // eslint-disable-next-line @typescript-eslint/no-var-requires, global-require
  return require('../../native/sea') as SeaNativeBinding;
}

/**
 * Verify the loaded module exposes the surface the driver depends on.
 * Catches kernel-side renames at load time rather than letting them
 * surface as `undefined is not a function` deep in a call path.
 */
function assertBindingShape(binding: SeaNativeBinding): void {
  const missing: string[] = [];
  if (typeof binding.version !== 'function') missing.push('version');
  if (typeof binding.openSession !== 'function') missing.push('openSession');
  if (typeof binding.Connection !== 'function') missing.push('Connection');
  if (typeof binding.Statement !== 'function') missing.push('Statement');
  if (missing.length > 0) {
    throw new Error(
      `SEA native binding loaded but is missing expected export(s): ${missing.join(', ')}. ` +
        `The kernel-generated binding and the JS loader are out of sync.`,
    );
  }
}

/**
 * Loads and caches the SEA native binding. Exposed as a class with an
 * injectable `load` seam so consumers (e.g. `SeaBackend`) can be unit
 * tested with a stub binding instead of requiring a real `.node` on the
 * test machine. Most production code uses the process-global default
 * via {@link getSeaNative} / {@link tryGetSeaNative}.
 */
export class SeaNativeLoader {
  private cached: SeaNativeBinding | null | undefined;

  private cachedError: Error | undefined;

  constructor(private readonly load: () => SeaNativeBinding = defaultRequire) {}

  private tryLoad(): SeaNativeBinding | undefined {
    const nodeMajor = detectNodeMajor();
    // Fail closed: if we cannot determine the Node major (NaN) or it is
    // below the floor, refuse the load and fall back to Thrift.
    if (!Number.isFinite(nodeMajor) || nodeMajor < MIN_NODE_MAJOR) {
      this.cachedError = new Error(
        `SEA native binding requires Node >=${MIN_NODE_MAJOR}; running Node ${process.version}. ` +
          `Continue using the Thrift backend on this runtime.`,
      );
      return undefined;
    }

    try {
      const binding = this.load();
      assertBindingShape(binding);
      return binding;
    } catch (err) {
      if (err instanceof Error && 'code' in err) {
        this.cachedError = new Error(loadFailureHint(err as NodeJS.ErrnoException));
      } else if (err instanceof Error) {
        // Shape-check failure or any other Error — preserve its message.
        this.cachedError = err;
      } else {
        this.cachedError = new Error(`SEA native binding failed to load with non-standard error: ${String(err)}`);
      }
      return undefined;
    }
  }

  /**
   * Returns the loaded native binding. Throws a structured error if the
   * binding is unavailable on this platform / Node version.
   */
  get(): SeaNativeBinding {
    if (this.cached === undefined) {
      this.cached = this.tryLoad() ?? null;
    }
    if (this.cached === null) {
      throw this.cachedError ?? new Error('SEA native binding unavailable');
    }
    return this.cached;
  }

  /**
   * Returns the loaded binding or `undefined` if it could not be
   * loaded. Use this for capability-detection at startup; use
   * {@link get} at the point where SEA is actually required.
   */
  tryGet(): SeaNativeBinding | undefined {
    if (this.cached === undefined) {
      this.cached = this.tryLoad() ?? null;
    }
    return this.cached ?? undefined;
  }
}

// Process-global default instance + thin convenience wrappers.
const defaultLoader = new SeaNativeLoader();

/**
 * Returns the loaded native binding from the process-global loader.
 * Throws a structured error if the binding is unavailable.
 */
export function getSeaNative(): SeaNativeBinding {
  return defaultLoader.get();
}

/**
 * Returns the loaded binding from the process-global loader, or
 * `undefined` if it could not be loaded.
 */
export function tryGetSeaNative(): SeaNativeBinding | undefined {
  return defaultLoader.tryGet();
}
