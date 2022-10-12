import winston, { Logger } from 'winston';
import IDBSQLLogger, { LOGLEVELS } from './contracts/IDBSQLLogger';

export default class DBSQLLogger implements IDBSQLLogger {
  logger: Logger;

  transports: any;

  constructor(filepath?: string, level = LOGLEVELS.info) {
    this.transports = {
      console: new winston.transports.Console({ handleExceptions: true, level }),
    };
    this.logger = winston.createLogger({
      transports: [this.transports.console],
    });
    if (filepath) {
      this.transports.file = new winston.transports.File({ filename: filepath, handleExceptions: true, level });
      this.logger.add(this.transports.file);
    }
  }

  async log(level: LOGLEVELS, message: string) {
    this.logger.log({ level, message });
  }

  setLevel(level: LOGLEVELS) {
    for (const key of Object.keys(this.transports)) {
      this.transports[key].level = level;
    }
  }
}
