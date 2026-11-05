import fs from 'fs';
import path from 'path';
import winston, { Logger } from 'winston';
import IDBSQLLogger, { LoggerOptions, LogLevel } from './contracts/IDBSQLLogger';

const DEFAULT_LOG_FILE_NAME = 'databricks-sql-nodejs.log';

// If `filepath` names an existing directory, write logs to a file inside it
// instead of trying to open the directory itself as a file (which fails with an
// asynchronous EISDIR on the transport stream). Any other value is used as-is.
function resolveLogFilePath(filepath: string): string {
  try {
    if (fs.statSync(filepath).isDirectory()) {
      return path.join(filepath, DEFAULT_LOG_FILE_NAME);
    }
  } catch {
    // path does not exist (or is inaccessible) — treat it as a file path
  }
  return filepath;
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
      this.transports.file = new winston.transports.File({
        filename: resolveLogFilePath(filepath),
        handleExceptions: true,
        level,
      });
      this.logger.add(this.transports.file);
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
