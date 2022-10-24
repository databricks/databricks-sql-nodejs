export default interface IDBSQLLogger {
  log(level: LogLevel, message: string): void;
}

export enum LogLevel {
  error = 'error',
  warn = 'warn',
  info = 'info',
  debug = 'debug',
}
