import IDBSQLLogger from './IDBSQLLogger';
import IDBSQLSession from './IDBSQLSession';
import IAuthentication from '../connection/contracts/IAuthentication';
import OAuthPersistence from '../connection/auth/DatabricksOAuth/OAuthPersistence';

export interface ClientOptions {
  logger?: IDBSQLLogger;
  stagingAllowedLocalPath?: string[]
}

type AuthOptions =
  | {
      authType?: 'access-token';
      token: string;
    }
  | {
      authType: 'databricks-oauth';
      persistence?: OAuthPersistence;
      azureTenantId?: string;
    }
  | {
      authType: 'custom';
      provider: IAuthentication;
    };

export type ConnectionOptions = {
  host: string;
  port?: number;
  path: string;
  clientId?: string;
  socketTimeout?: number;
} & AuthOptions;

export interface OpenSessionRequest {
  initialCatalog?: string;
  initialSchema?: string;
}

export default interface IDBSQLClient {
  connect(options: ConnectionOptions): Promise<IDBSQLClient>;

  openSession(request?: OpenSessionRequest): Promise<IDBSQLSession>;

  close(): Promise<void>;
}
