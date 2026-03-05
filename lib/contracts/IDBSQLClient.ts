import IDBSQLLogger from './IDBSQLLogger';
import IDBSQLSession from './IDBSQLSession';
import IAuthentication from '../connection/contracts/IAuthentication';
import { ProxyOptions } from '../connection/contracts/IConnectionOptions';
import OAuthPersistence from '../connection/auth/DatabricksOAuth/OAuthPersistence';
import ITokenProvider from '../connection/auth/tokenProvider/ITokenProvider';
import { TokenCallback } from '../connection/auth/tokenProvider/ExternalTokenProvider';

export interface ClientOptions {
  logger?: IDBSQLLogger;
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
    }
  | {
      authType: 'token-provider';
      tokenProvider: ITokenProvider;
      enableTokenFederation?: boolean;
      federationClientId?: string;
    }
  | {
      authType: 'external-token';
      getToken: TokenCallback;
      enableTokenFederation?: boolean;
      federationClientId?: string;
    }
  | {
      authType: 'static-token';
      staticToken: string;
      enableTokenFederation?: boolean;
      federationClientId?: string;
    };

export type ConnectionOptions = {
  host: string;
  port?: number;
  path: string;
  userAgentEntry?: string;
  socketTimeout?: number;
  proxy?: ProxyOptions;
  enableMetricViewMetadata?: boolean;
} & AuthOptions;

export interface OpenSessionRequest {
  initialCatalog?: string;
  initialSchema?: string;
  configuration?: { [key: string]: string };
}

export default interface IDBSQLClient {
  connect(options: ConnectionOptions): Promise<IDBSQLClient>;

  openSession(request?: OpenSessionRequest): Promise<IDBSQLSession>;

  close(): Promise<void>;
}
