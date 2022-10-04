export default interface IDBSQLLogger {
  log(message: string, level: string): Promise<void>;
}
