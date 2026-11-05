import fs from 'fs';
import path from 'path';
import winston, { Logger } from 'winston';
import IDBSQLLogger, { LoggerOptions, LogLevel } from './contracts/IDBSQLLogger';

const DEFAULT_LOG_FILE_NAME = 'databricks-sql-nodejs.log';

// An existing directory resolves to a default file inside it. Anything else is
// returned as the path winston will open (which drops a trailing separator).
function resolveLogFilePath(filepath: string): string {
  try {
    if (fs.statSync(filepath).isDirectory()) {
      return path.join(filepath, DEFAULT_LOG_FILE_NAME);
    }
  } catch {
    // doesn't exist or isn't accessible: treat it as a file path
  }
  return path.join(path.dirname(filepath), path.basename(filepath));
}

// winston's File transport swallows errors opening its stream (EISDIR, EACCES,
// ENOTDIR, ...), after which the logger stalls for all transports once its
// buffer fills. Check up front that the file can be opened for appending.
// Note: this probe is intentionally not side-effect free. It creates missing
// parent directories and an empty log file (as winston itself would when the
// transport opens), so the check exercises the exact same open path.
// Limitations: this is a one-time, synchronous check (statSync/mkdirSync/
// openSync) run each time a DBSQLLogger is constructed with a `filepath`.
// Failures that start later (file/dir deleted, permissions changed, disk
// full, a Windows file lock) are not detected here and can still stall the
// logger. Follow-up (separate change): attach a runtime `error` handler to the
// File transport that removes it and falls back to console-only logging.
function checkLogFileWritable(filename: string): Error | undefined {
  try {
    fs.mkdirSync(path.dirname(filename), { recursive: true });
    fs.closeSync(fs.openSync(filename, 'a'));
    return undefined;
  } catch (error) {
    return error instanceof Error ? error : new Error(String(error));
  }
}

export default class DBSQLLogger implements IDBSQLLogger {
  logger: Logger;

  transports: {
    console: winston.transports.ConsoleTransportInstance;
    file?: winston.transports.FileTransportInstance;
  };

  // Subscribers notified on `setLevel(...)` — used by the kernel backend to
  // keep the kernel-side log bridge's verbosity in lock-step with this logger.
  private levelListeners: Array<(level: LogLevel) => void> = [];

  constructor({ level = LogLevel.info, filepath }: LoggerOptions = {}) {
    this.transports = {
      console: new winston.transports.Console({ handleExceptions: true, level }),
    };
    this.logger = winston.createLogger({
      transports: [this.transports.console],
    });
    if (filepath) {
      const filename = resolveLogFilePath(filepath);
      const error = checkLogFileWritable(filename);
      if (error) {
        // Logged at `error` so the console transport's level filter can never
        // drop it (e.g. when the logger is configured with `level: error`).
        // Trade-off: consumers filtering on the winston `level` field (e.g.
        // error-level alerting) WILL see this as an error; the "[warn] ...
        // non-fatal" wording only helps human readers tell this config
        // fallback apart from a driver failure.
        this.logger.log({
          level: LogLevel.error,
          message: `[warn] Cannot write to log file ${filename}, logging to console only (non-fatal, driver continues normally): ${error.message}`,
        });
      } else {
        this.transports.file = new winston.transports.File({ filename, handleExceptions: true, level });
        this.logger.add(this.transports.file);
      }
    }
  }

  async log(level: LogLevel, message: string) {
    this.logger.log({ level, message });
  }

  getLevel(): LogLevel {
    return (this.transports.console.level as LogLevel) ?? LogLevel.info;
  }

  onLevelChange(listener: (level: LogLevel) => void): () => void {
    this.levelListeners.push(listener);
    return () => {
      const index = this.levelListeners.indexOf(listener);
      if (index >= 0) {
        this.levelListeners.splice(index, 1);
      }
    };
  }

  setLevel(level: LogLevel) {
    this.transports.console.level = level;
    if (this.transports.file) {
      this.transports.file.level = level;
    }
    for (const listener of this.levelListeners) {
      // A subscriber must never break level setting for the rest.
      try {
        listener(level);
      } catch {
        // swallow — level-change notification is advisory
      }
    }
  }
}
