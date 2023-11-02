import IDBSQLLogger from './IDBSQLLogger';
import IDBSQLSession from './IDBSQLSession';
import IAuthentication from '../connection/contracts/IAuthentication';
import IConnectionOptions, { ProxyOptions } from '../connection/contracts/IConnectionOptions';
import OAuthPersistence from '../connection/auth/DatabricksOAuth/OAuthPersistence';
import IConnectionProvider from '../connection/contracts/IConnectionProvider';

export interface ClientOptions {
  logger?: IDBSQLLogger;

  connectionProvider: new (o: IConnectionOptions) => IConnectionProvider;
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
      oauthClientId?: string;
      oauthClientSecret?: string;
    }
  | {
      authType: 'custom';
      provider: IAuthentication;
    };

export type ConnectionOptions = {
  clientId?: string;
} & AuthOptions & IConnectionOptions;

export interface OpenSessionRequest {
  initialCatalog?: string;
  initialSchema?: string;
}

export default interface IDBSQLClient {
  connect(options: ConnectionOptions): Promise<IDBSQLClient>;

  openSession(request?: OpenSessionRequest): Promise<IDBSQLSession>;

  close(): Promise<void>;
}
