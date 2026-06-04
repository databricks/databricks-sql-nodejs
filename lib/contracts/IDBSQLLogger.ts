export interface LoggerOptions {
  filepath?: string;
  level?: LogLevel;
}

export default interface IDBSQLLogger {
  log(level: LogLevel, message: string): void;

  /**
   * Optional: the logger's current level. When implemented, the SEA/kernel
   * backend uses it to set the verbosity of the kernel-side (Rust) log bridge,
   * so kernel logs are filtered at the same level as the driver's own logs and
   * land in the same sink. Loggers that don't implement it leave the kernel
   * bridge at its `info` default.
   */
  getLevel?(): LogLevel;
}

export enum LogLevel {
  error = 'error',
  warn = 'warn',
  info = 'info',
  debug = 'debug',
}
