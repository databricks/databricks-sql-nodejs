import winston, { Logger } from 'winston';
import IDBSQLLogger from './contracts/IDBSQLLogger';

export default class DBSQLLogger implements IDBSQLLogger {
  logger: Logger;

  transports: any;

  constructor(filepath?: string) {
    this.transports = {
      console: new winston.transports.Console({ handleExceptions: true }),
    };
    this.logger = winston.createLogger({
      transports: [this.transports.console],
    });
    if (filepath) {
      this.transports.file = new winston.transports.File({ filename: filepath, handleExceptions: true });
      this.logger.add(this.transports.file);
    }
  }

  async log(level: string, message: string) {
    this.logger.log({ level, message });
  }

  setLoggingLevel(level: string) {
    for (const key of Object.keys(this.transports)) {
      this.transports[key].level = level;
    }
  }
}
