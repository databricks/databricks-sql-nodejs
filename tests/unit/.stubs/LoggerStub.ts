import IDBSQLLogger, { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

export default class LoggerStub implements IDBSQLLogger {
  public log(level: LogLevel, message: string) {
    // do nothing
  }
}
