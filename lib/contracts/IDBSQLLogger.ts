export interface LoggerOptions {
  filepath?: string;
  level?: LogLevel;
}

export default interface IDBSQLLogger {
  log(level: LogLevel, message: string): void;

  /**
   * Optional: the logger's current level. When implemented, the kernel
   * backend uses it to set the verbosity of the kernel-side (Rust) log bridge,
   * so kernel logs are filtered at the same level as the driver's own logs and
   * land in the same sink. Loggers that don't implement it leave the kernel
   * bridge at its `info` default.
   */
  getLevel?(): LogLevel;

  /**
   * Optional: subscribe to runtime level changes. When implemented, the
   * kernel backend subscribes so a runtime `setLevel(...)` retargets the
   * kernel-side log bridge too (not just the driver's own transports) — keeping
   * kernel verbosity in lock-step with the driver's. Returns an unsubscribe
   * function. Loggers that don't implement it still get the connect-time level;
   * only *runtime* retargeting of the kernel is unavailable.
   */
  onLevelChange?(listener: (level: LogLevel) => void): () => void;
}

export enum LogLevel {
  error = 'error',
  warn = 'warn',
  info = 'info',
  debug = 'debug',
}
