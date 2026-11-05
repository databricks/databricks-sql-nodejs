import fs from 'fs';
import path from 'path';
import winston, { Logger } from 'winston';
import IDBSQLLogger, { LoggerOptions, LogLevel } from './contracts/IDBSQLLogger';

const DEFAULT_LOG_FILE_NAME = 'databricks-sql-nodejs.log';

export const LOG_FILE_UNAVAILABLE_WARNING_CODE = 'DBSQL_LOG_FILE_UNAVAILABLE';

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

// winston's File transport only `debug`-logs errors opening its write stream
// (EISDIR, EACCES, ...), and the logger then stalls for every transport once
// its buffer fills. Its `stat` errors (e.g. ENOTDIR) are emitted instead and,
// with no listener, crash the process. So probe the file once up front,
// creating it and its directory as winston would. Later failures (e.g. disk
// full) aren't covered. Paths that exist but aren't regular files or
// directories are left to winston: opening a FIFO blocks until it has a reader.
function checkLogFileWritable(filename: string): Error | undefined {
  try {
    const stat = fs.statSync(filename, { throwIfNoEntry: false });
    if (stat && !stat.isFile() && !stat.isDirectory()) {
      return undefined;
    }
    const dir = path.dirname(filename);
    // Like winston, only create a missing directory: on Windows, mkdir of a
    // drive root throws EPERM even with `recursive`.
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
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
        process.emitWarning(`Cannot write to log file ${filename}, logging to console only: ${error.message}`, {
          code: LOG_FILE_UNAVAILABLE_WARNING_CODE,
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
