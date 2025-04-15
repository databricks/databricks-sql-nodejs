import IDBSQLLogger from './IDBSQLLogger';
import IDBSQLSession from './IDBSQLSession';
import IAuthentication from '../connection/contracts/IAuthentication';
import { ProxyOptions } from '../connection/contracts/IConnectionOptions';
import OAuthPersistence from '../connection/auth/DatabricksOAuth/OAuthPersistence';
import { TProtocolVersion, TSparkRowSetType } from '../../thrift/TCLIService_types';

export interface ClientOptions {
  logger?: IDBSQLLogger;
  useAsSparkThriftServerClient?: boolean;
  /**
   * Optionally set the expected result set type in case the server does not provide it.
   * This is useful for Spark Thrift Server, which does not provide the result set type.
   */
  resultFormat?: TSparkRowSetType;
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
      useDatabricksOAuthInAzure?: boolean;
    }
  | {
      authType: 'custom';
      provider: IAuthentication;
    };

export type ConnectionOptions = {
  host: string;
  port?: number;
  path: string;
  userAgentEntry?: string;
  socketTimeout?: number;
  proxy?: ProxyOptions;
} & AuthOptions;

export interface OpenSessionRequest {
  initialCatalog?: string;
  initialSchema?: string;
  protocol?: TProtocolVersion;
}

export default interface IDBSQLClient {
  connect(options: ConnectionOptions): Promise<IDBSQLClient>;

  openSession(request?: OpenSessionRequest): Promise<IDBSQLSession>;

  close(): Promise<void>;
}
