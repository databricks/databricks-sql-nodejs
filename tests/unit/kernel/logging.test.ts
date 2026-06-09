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

import { expect } from 'chai';
import {
  kernelLevelToLogLevel,
  logLevelToKernelLevel,
  formatKernelLine,
  installKernelLogBridge,
} from '../../../lib/kernel/KernelLogging';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import { KernelNativeBinding, KernelNativeLogRecord } from '../../../lib/kernel/KernelNativeLoader';

// Minimal recording logger. With `withLevelChange` (default) it exposes
// `onLevelChange` + an `emitLevelChange` test helper to simulate a runtime
// `setLevel(...)`; pass `false` to model a logger that can't notify.
function recordingLogger(opts: { withLevelChange?: boolean } = {}) {
  const lines: Array<{ level: LogLevel; message: string }> = [];
  const listeners: Array<(level: LogLevel) => void> = [];
  const logger = {
    lines,
    log(level: LogLevel, message: string) {
      lines.push({ level, message });
    },
    // test-only: simulate the driver's `setLevel(...)` firing its subscribers
    emitLevelChange(level: LogLevel) {
      listeners.forEach((l) => l(level));
    },
  } as {
    lines: Array<{ level: LogLevel; message: string }>;
    log(level: LogLevel, message: string): void;
    emitLevelChange(level: LogLevel): void;
    onLevelChange?(listener: (level: LogLevel) => void): () => void;
  };
  if (opts.withLevelChange !== false) {
    logger.onLevelChange = (listener: (level: LogLevel) => void) => {
      listeners.push(listener);
      return () => {
        const i = listeners.indexOf(listener);
        if (i >= 0) listeners.splice(i, 1);
      };
    };
  }
  return logger;
}

// A binding stub that captures the registered bridge callback + initial level,
// and records each `setKernelLogLevel` retarget.
function captureBinding(overrides: Partial<Record<keyof KernelNativeBinding, unknown>> = {}) {
  const captured: {
    cb?: (err: Error | null, records: Array<KernelNativeLogRecord>) => void;
    level?: string;
    levelChanges: string[];
  } = { levelChanges: [] };
  const binding = {
    initKernelLogging: (cb: (err: Error | null, records: Array<KernelNativeLogRecord>) => void, level: string) => {
      captured.cb = cb;
      captured.level = level;
    },
    setKernelLogLevel: (level: string) => {
      captured.levelChanges.push(level);
    },
    ...overrides,
  } as unknown as KernelNativeBinding;
  return { binding, captured };
}

describe('KernelLogging', () => {
  describe('kernelLevelToLogLevel', () => {
    it('maps the kernel levels onto driver LogLevels', () => {
      expect(kernelLevelToLogLevel('error')).to.equal(LogLevel.error);
      expect(kernelLevelToLogLevel('warn')).to.equal(LogLevel.warn);
      expect(kernelLevelToLogLevel('info')).to.equal(LogLevel.info);
      expect(kernelLevelToLogLevel('debug')).to.equal(LogLevel.debug);
    });

    it('folds kernel `trace` (no driver analogue) into debug', () => {
      expect(kernelLevelToLogLevel('trace')).to.equal(LogLevel.debug);
    });

    it('buckets an unknown level into debug rather than dropping it', () => {
      expect(kernelLevelToLogLevel('weird')).to.equal(LogLevel.debug);
    });
  });

  describe('logLevelToKernelLevel', () => {
    it('passes the lower-case level string through to the kernel', () => {
      expect(logLevelToKernelLevel(LogLevel.error)).to.equal('error');
      expect(logLevelToKernelLevel(LogLevel.debug)).to.equal('debug');
    });
  });

  describe('formatKernelLine', () => {
    it('tags the line with the kernel origin + target', () => {
      const line = formatKernelLine({ level: 'info', target: 'databricks::sql::kernel', message: 'hi' });
      expect(line).to.equal('[kernel databricks::sql::kernel] hi');
    });
  });

  describe('installKernelLogBridge', () => {
    it('registers with the kernel at the requested level', () => {
      const { binding, captured } = captureBinding();
      installKernelLogBridge(binding, recordingLogger(), LogLevel.debug);
      expect(captured.level).to.equal('debug');
      expect(captured.cb).to.be.a('function');
    });

    it('forwards a batch of kernel records into the logger, level-mapped + tagged', () => {
      const { binding, captured } = captureBinding();
      const logger = recordingLogger();
      installKernelLogBridge(binding, logger, LogLevel.debug);

      captured.cb!(null, [
        { level: 'warn', target: 'databricks::sql::kernel', message: 'retrying' },
        { level: 'trace', target: 'databricks_sql_kernel::session', message: 'span enter' },
      ]);

      expect(logger.lines).to.have.length(2);
      expect(logger.lines[0]).to.deep.equal({
        level: LogLevel.warn,
        message: '[kernel databricks::sql::kernel] retrying',
      });
      // trace → debug
      expect(logger.lines[1].level).to.equal(LogLevel.debug);
      expect(logger.lines[1].message).to.contain('span enter');
    });

    it('drops a batch that arrives with an error and never calls the logger', () => {
      const { binding, captured } = captureBinding();
      const logger = recordingLogger();
      installKernelLogBridge(binding, logger, LogLevel.info);
      captured.cb!(new Error('tsfn failure'), []);
      expect(logger.lines).to.have.length(0);
    });

    it('is a no-op (no throw) on an older binding without the bridge export', () => {
      const binding = {} as unknown as KernelNativeBinding;
      expect(() => installKernelLogBridge(binding, recordingLogger(), LogLevel.info)).to.not.throw();
    });
  });

  describe('runtime level retargeting', () => {
    it('retargets the kernel level when the logger level changes at runtime', () => {
      const { binding, captured } = captureBinding();
      const logger = recordingLogger();
      installKernelLogBridge(binding, logger, LogLevel.warn);
      expect(captured.level).to.equal('warn'); // connect-time level

      logger.emitLevelChange(LogLevel.debug);
      logger.emitLevelChange(LogLevel.error);
      expect(captured.levelChanges).to.deep.equal(['debug', 'error']);
    });

    it('stops retargeting once the returned unsubscribe is called', () => {
      const { binding, captured } = captureBinding();
      const logger = recordingLogger();
      const unsubscribe = installKernelLogBridge(binding, logger, LogLevel.info);

      logger.emitLevelChange(LogLevel.debug);
      unsubscribe();
      logger.emitLevelChange(LogLevel.error); // after unsubscribe → ignored

      expect(captured.levelChanges).to.deep.equal(['debug']);
    });

    it('does not subscribe when the logger cannot notify (no onLevelChange)', () => {
      const { binding, captured } = captureBinding();
      const logger = recordingLogger({ withLevelChange: false });
      const unsubscribe = installKernelLogBridge(binding, logger, LogLevel.info);
      logger.emitLevelChange(LogLevel.debug);
      expect(captured.levelChanges).to.have.length(0);
      expect(unsubscribe).to.be.a('function'); // safe no-op
      expect(() => unsubscribe()).to.not.throw();
    });

    it('does not subscribe when the binding cannot retarget (no setKernelLogLevel)', () => {
      // initKernelLogging present, setKernelLogLevel absent (partial/older binding).
      const captured: { installed: boolean } = { installed: false };
      const binding = {
        initKernelLogging: () => {
          captured.installed = true;
        },
      } as unknown as KernelNativeBinding;
      const logger = recordingLogger();
      const unsubscribe = installKernelLogBridge(binding, logger, LogLevel.info);
      expect(captured.installed).to.equal(true);
      expect(() => {
        logger.emitLevelChange(LogLevel.debug);
        unsubscribe();
      }).to.not.throw();
    });
  });
});
