export default interface IDBSQLLogger {
  log(level: LogLevel, message: string): void;
}

export enum LogLevel {
  error = 'error',
  warn = 'warn',
  info = 'info',
  http = 'http',
  verbose = 'verbose',
  debug = 'debug',
  silly = 'silly',
}
