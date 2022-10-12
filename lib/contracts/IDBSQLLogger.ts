export default interface IDBSQLLogger {
  log(message: string, level: string): void;
}

export enum LOGLEVELS {
  error = 'error',
  warn = 'warn',
  info = 'info',
  http = 'http',
  verbose = 'verbose',
  debug = 'debug',
  silly = 'silly',
}
