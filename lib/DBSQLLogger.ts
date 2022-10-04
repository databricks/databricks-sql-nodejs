import IDBSQLLogger from "./contracts/IDBSQLLogger";
const winston = require('winston');

export default class DBSQLLogger implements IDBSQLLogger {
    logger: any;
    transports: any;
    constructor(filepath?: string) {
        this.transports = {
            console: new winston.transports.Console()
        };
        this.logger = winston.createLogger({
            transports: [
              this.transports.console
            ]
        });
        if(filepath) {
            this.transports.file = new winston.transports.File({ filename: filepath })
            this.logger.add(this.transports.file)
        }
    }
    async log(level: string, message: string) {
        this.logger.log({level: level, message: message})
    }

    setLoggingLevel(level: string) {
        for(let key in this.transports) {
            this.transports[key].level = level;
        }
    }
}