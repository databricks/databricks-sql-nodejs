import { TOpenSessionReq } from '../../thrift/TCLIService_types';
import IHiveSession from './IHiveSession';

export interface IDBSQLConnectionOptions {
  host: string;
  port?: number;
  path: string;
  token: string;
  clientId?: string;
}

export default interface IDBSQLClient {
  connect(options: IDBSQLConnectionOptions): Promise<IDBSQLClient>;

  openSession(request?: TOpenSessionReq): Promise<IHiveSession>;

  close(): Promise<void>;
}
