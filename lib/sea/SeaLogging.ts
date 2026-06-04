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
 * Kernel → driver log bridge.
 *
 * The Rust kernel emits its diagnostics via `tracing`. In a Node process those
 * events have no subscriber and are dropped — so by default the driver's
 * `DBSQLLogger` only ever saw JS-side lines. The napi binding's
 * `initKernelLogging` installs a process-global subscriber that forwards
 * kernel events (batched) to a JS callback; this module wires that callback
 * into the **same** `IDBSQLLogger` the driver logs through, so logs from all
 * three layers (driver, napi shim, kernel) land in one place — and one file
 * when the logger has a file transport.
 *
 * Verbosity follows the driver's logger level (see `installKernelLogBridge`),
 * filtered kernel-side so we don't pay the channel/bridge cost for events the
 * sink would discard anyway.
 */

import IDBSQLLogger, { LogLevel } from '../contracts/IDBSQLLogger';
import { SeaNativeBinding, SeaNativeLogRecord } from './SeaNativeLoader';

/**
 * Map a kernel level string (`error`/`warn`/`info`/`debug`/`trace`) onto the
 * driver's `LogLevel`. The kernel's `trace` has no `LogLevel` analogue, so it
 * folds into `debug` (the most verbose driver level).
 */
export function kernelLevelToLogLevel(level: string): LogLevel {
  switch (level) {
    case 'error':
      return LogLevel.error;
    case 'warn':
      return LogLevel.warn;
    case 'info':
      return LogLevel.info;
    case 'debug':
    case 'trace':
      return LogLevel.debug;
    default:
      // Unknown kernel level — surface it rather than drop it; debug is the
      // least surprising bucket for an unrecognised severity.
      return LogLevel.debug;
  }
}

/**
 * Map a driver `LogLevel` onto the kernel level string the napi bridge expects.
 * The `LogLevel` enum values are already the kernel-compatible lower-case
 * strings, so this is the identity at runtime — kept as an explicit function so
 * the boundary is named and a future divergence has one place to live.
 */
export function logLevelToKernelLevel(level: LogLevel): string {
  return level;
}

/**
 * Format one kernel log record into a single driver log line, tagged with its
 * origin so kernel lines are distinguishable from driver lines in a shared
 * sink/file.
 */
export function formatKernelLine(record: SeaNativeLogRecord): string {
  return `[kernel ${record.target}] ${record.message}`;
}

/**
 * Install the kernel→driver log bridge: forward kernel `tracing` events into
 * `logger` at `level`.
 *
 * - **Verbosity** is set kernel-side to `level` so events the sink would drop
 *   never cross the bridge.
 * - **Process-global, last-writer-wins:** the napi binding holds a single
 *   process-global subscriber + sink (a `tracing` global subscriber installs
 *   once). Each call retargets the sink to `logger`, so in a multi-client
 *   process the most recently connected client's logger receives kernel logs —
 *   mirroring the Python connector's `pyo3_log` model. Single-client apps, the
 *   common case, are unaffected.
 * - **Graceful on older bindings:** if the loaded `.node` predates
 *   `initKernelLogging`, this is a no-op (kernel logs simply stay unbridged)
 *   rather than a hard failure — logging is advisory.
 */
export function installKernelLogBridge(binding: SeaNativeBinding, logger: IDBSQLLogger, level: LogLevel): void {
  // Defensive: a stale/older binding without the bridge export must not break
  // connect() — logging is non-critical.
  if (typeof binding.initKernelLogging !== 'function') {
    return;
  }

  const callback = (err: Error | null, records: Array<SeaNativeLogRecord>): void => {
    if (err || !records) {
      return;
    }
    for (const record of records) {
      logger.log(kernelLevelToLogLevel(record.level), formatKernelLine(record));
    }
  };

  binding.initKernelLogging(callback, logLevelToKernelLevel(level));
}
