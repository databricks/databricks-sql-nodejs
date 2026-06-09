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
 * (`@databricks/databricks-sql-kernel-<triple>`), so its absence must not crash
 * a Thrift-only consumer of the driver. Callers that actually need
 * kernel construct a {@link KernelNativeLoader} (or use the process-global
 * {@link getKernelNative}) which throws a structured error if the binding
 * could not be loaded.
 *
 * M0 publishes a single triple (`linux-x64-gnu`); see
 * `native/kernel/README.md` for the supported-platform policy.
 */

import type {
  Connection as NativeConnection,
  Statement as NativeStatement,
  ConnectionOptions as NativeConnectionOptions,
  ArrowBatch as NativeArrowBatch,
  ArrowSchema as NativeArrowSchema,
  ExecuteOptions as NativeExecuteOptions,
  TypedValueInput as NativeTypedValueInput,
  NamedTypedValueInput as NativeNamedTypedValueInput,
  AsyncStatement as NativeAsyncStatement,
  AsyncResultHandle as NativeAsyncResultHandle,
  CancellableExecution as NativeCancellableExecution,
  LogRecord as NativeLogRecord,
} from '../../native/kernel';

// kernel-prefixed re-exports. The kernel-generated `.d.ts` keeps the
// napi-rs default names (`ConnectionOptions`, `ArrowBatch`, …); we
// disambiguate on the TS-wrapper side so these never collide with the
// Thrift-side `ConnectionOptions` (lib/contracts/IDBSQLClient.ts) or
// `ArrowBatch` (lib/result/utils.ts) when imported elsewhere.
export type KernelConnectionOptions = NativeConnectionOptions;
export type KernelArrowBatch = NativeArrowBatch;
export type KernelArrowSchema = NativeArrowSchema;
export type KernelConnection = NativeConnection;
export type KernelStatement = NativeStatement;

// Per-statement execution options and bound-parameter inputs are kernel
// concerns: the napi binding generates the canonical shapes (`positionalParams`
// / `namedParams` as `TypedValueInput` / `NamedTypedValueInput`, plus
// `rowLimit`, `statementConf`, `queryTags`). We re-export
// rather than re-declare so the driver-side param codec can never drift from
// the kernel contract.
export type KernelNativeExecuteOptions = NativeExecuteOptions;
export type KernelNativeTypedValueInput = NativeTypedValueInput;
export type KernelNativeNamedTypedValueInput = NativeNamedTypedValueInput;

// Async-submit surface: `Connection.submitStatement` returns an
// `AsyncStatement` (status / awaitResult / cancel / close); `awaitResult`
// yields an `AsyncResultHandle` whose `fetchNextBatch` / `schema` match the
// blocking `Statement`'s fetch surface, so the results pipeline consumes
// either interchangeably.
export type KernelNativeAsyncStatement = NativeAsyncStatement;
export type KernelNativeAsyncResultHandle = NativeAsyncResultHandle;

// Cancellable sync-execute surface: `Connection.executeStatementCancellable`
// returns a `CancellableExecution` that captures a detached StatementCanceller
// BEFORE dispatching the blocking `execute()`, so a concurrent `cancel()`
// interrupts a still-running query mid-compute. `result()` drives the blocking
// execute and resolves to the same terminal `Statement` `executeStatement`
// returns.
export type KernelNativeCancellableExecution = NativeCancellableExecution;

// One kernel log event forwarded over the napi log bridge (see KernelLogging.ts):
// `{ level, target, message }`. Re-exported so the bridge can name the shape
// without re-declaring it (stays in lock-step with the kernel contract).
export type KernelNativeLogRecord = NativeLogRecord;

/**
 * The full native binding surface, derived from the generated module
 * so it can never drift from the `.d.ts` contract: when the kernel
 * adds or renames a free function / class, this type follows
 * automatically and `defaultRequire`'s cast stays correct.
 */
export type KernelNativeBinding = typeof import('../../native/kernel');

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
    'Install the matching @databricks/databricks-sql-kernel-* optional dependency for your platform ' +
    '(see native/kernel/README.md for the supported triples; M0 ships linux-x64-gnu only).';
  if (err.code === 'MODULE_NOT_FOUND') {
    return `kernel native binding not installed for platform ${platform} on Node ${process.version}. ${installHint}`;
  }
  if (err.code === 'ERR_DLOPEN_FAILED') {
    // Surface the underlying dlerror string (e.g. `GLIBC_2.32 not found`)
    // plus concrete remediation — without it the cause is invisible.
    return (
      `kernel native binding present but failed to dlopen on platform ${platform} / Node ${process.version}: ` +
      `${err.message}. Common causes: glibc/musl mismatch (e.g. Alpine Linux — install the -musl variant), ` +
      `Node ABI mismatch (try \`rm -rf node_modules && npm install\`), or CPU-architecture mismatch. ` +
      `The binding requires Node >=${MIN_NODE_MAJOR}.`
    );
  }
  return `kernel native binding failed to load on platform ${platform} / Node ${process.version}: ${err.message}`;
}

/**
 * Default loader: resolves `native/kernel/index.js` (the napi-rs router),
 * which selects the per-platform `.node`. `.js` is omitted so eslint's
 * `import/extensions` rule accepts the call.
 */
function defaultRequire(): KernelNativeBinding {
  // eslint-disable-next-line @typescript-eslint/no-var-requires, global-require
  return require('../../native/kernel') as KernelNativeBinding;
}

/**
 * Verify the loaded module exposes the surface the driver depends on.
 * Catches kernel-side renames at load time rather than letting them
 * surface as `undefined is not a function` deep in a call path.
 */
function assertBindingShape(binding: KernelNativeBinding): void {
  const missing: string[] = [];
  if (typeof binding.version !== 'function') missing.push('version');
  if (typeof binding.openSession !== 'function') missing.push('openSession');
  if (typeof binding.Connection !== 'function') missing.push('Connection');
  if (typeof binding.Statement !== 'function') missing.push('Statement');
  // Classes the async (submit/poll) and cancellable-sync execution paths depend
  // on. Checking them here turns a stale/older cached `.node` into a clear
  // load-time error instead of an `X is not a function` mid-query (e.g.
  // `Connection.submitStatement` / `executeStatementCancellable`).
  if (typeof binding.AsyncStatement !== 'function') missing.push('AsyncStatement');
  if (typeof binding.AsyncResultHandle !== 'function') missing.push('AsyncResultHandle');
  if (typeof binding.CancellableExecution !== 'function') missing.push('CancellableExecution');
  if (missing.length > 0) {
    throw new Error(
      `kernel native binding loaded but is missing expected export(s): ${missing.join(', ')}. ` +
        `The kernel-generated binding and the JS loader are out of sync.`,
    );
  }
}

/**
 * Loads and caches the kernel native binding. Exposed as a class with an
 * injectable `load` seam so consumers (e.g. `KernelBackend`) can be unit
 * tested with a stub binding instead of requiring a real `.node` on the
 * test machine. Most production code uses the process-global default
 * via {@link getKernelNative} / {@link tryGetKernelNative}.
 */
export class KernelNativeLoader {
  private cached: KernelNativeBinding | null | undefined;

  private cachedError: Error | undefined;

  /**
   * @param load        injectable module-require seam (stub a binding in tests)
   * @param nodeMajor   injectable Node-major detector. Defaults to reading the
   *                    live `process.version`; injected in unit tests so the
   *                    load/shape branches are exercised independently of the
   *                    runner's actual Node version (the matrix spans 14–20).
   */
  constructor(
    private readonly load: () => KernelNativeBinding = defaultRequire,
    private readonly nodeMajor: () => number = detectNodeMajor,
  ) {}

  private tryLoad(): KernelNativeBinding | undefined {
    const nodeMajor = this.nodeMajor();
    // Fail closed: if we cannot determine the Node major (NaN) or it is
    // below the floor, refuse the load and fall back to Thrift.
    if (!Number.isFinite(nodeMajor) || nodeMajor < MIN_NODE_MAJOR) {
      this.cachedError = new Error(
        `kernel native binding requires Node >=${MIN_NODE_MAJOR}; running Node ${process.version}. ` +
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
        this.cachedError = new Error(`kernel native binding failed to load with non-standard error: ${String(err)}`);
      }
      return undefined;
    }
  }

  /**
   * Returns the loaded native binding. Throws a structured error if the
   * binding is unavailable on this platform / Node version.
   */
  get(): KernelNativeBinding {
    if (this.cached === undefined) {
      this.cached = this.tryLoad() ?? null;
    }
    if (this.cached === null) {
      throw this.cachedError ?? new Error('kernel native binding unavailable');
    }
    return this.cached;
  }

  /**
   * Returns the loaded binding or `undefined` if it could not be
   * loaded. Use this for capability-detection at startup; use
   * {@link get} at the point where kernel is actually required.
   */
  tryGet(): KernelNativeBinding | undefined {
    if (this.cached === undefined) {
      this.cached = this.tryLoad() ?? null;
    }
    return this.cached ?? undefined;
  }
}

// Process-global default instance + thin convenience wrappers.
const defaultLoader = new KernelNativeLoader();

/**
 * Returns the loaded native binding from the process-global loader.
 * Throws a structured error if the binding is unavailable.
 */
export function getKernelNative(): KernelNativeBinding {
  return defaultLoader.get();
}

/**
 * Returns the loaded binding from the process-global loader, or
 * `undefined` if it could not be loaded.
 */
export function tryGetKernelNative(): KernelNativeBinding | undefined {
  return defaultLoader.tryGet();
}
