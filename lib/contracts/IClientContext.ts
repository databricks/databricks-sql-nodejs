import IDBSQLLogger from './IDBSQLLogger';
import IDriver from './IDriver';
import IConnectionProvider from '../connection/contracts/IConnectionProvider';
import TCLIService from '../../thrift/TCLIService';

export default interface IClientContext {
  getLogger(): IDBSQLLogger;

  getConnectionProvider(): Promise<IConnectionProvider>;

  getClient(): Promise<TCLIService.Client>;

  getDriver(): Promise<IDriver>;
}
