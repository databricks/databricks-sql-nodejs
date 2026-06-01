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
  /**
   * Opt-in flag to dispatch through the Statement Execution API (SEA) backend
   * instead of the default Thrift backend. Defaults to `false`.
   * @internal Not stable; M0 stub only.
   */
  useSEA?: boolean;
  /**
   * Whether to verify the server's TLS certificate (SEA backend only).
   *
   * Defaults to `true` — **secure by default**: strict validation against
   * the system trust store (full chain + expiry + hostname), matching the
   * JDBC/ODBC drivers and every modern HTTPS client.
   *
   * Set to `false` to disable verification: self-signed, untrusted, and
   * expired certificates are accepted and the hostname-vs-certificate check
   * is skipped. This is **insecure** — it provides no protection against
   * active man-in-the-middle attacks — and exists only as an opt-out for
   * parity with the legacy NodeJS Thrift driver, which hard-codes
   * `rejectUnauthorized: false`.
   *
   * For corporate TLS-inspecting proxies or on-prem deployments with an
   * internal CA, prefer the default `checkServerCertificate: true` together
   * with `customCaCert` over disabling verification entirely.
   */
  checkServerCertificate?: boolean;
  /**
   * PEM-encoded CA certificate to add to the trust store on top of the
   * system roots (SEA backend only). Accepts a PEM string or its raw
   * `Buffer` bytes. Use this for a corporate proxy that re-signs TLS or an
   * on-prem Databricks deployment that uses an internal CA. Honoured
   * regardless of `checkServerCertificate`.
   */
  customCaCert?: Buffer | string;
} & AuthOptions;

export interface OpenSessionRequest {
  initialCatalog?: string;
  initialSchema?: string;
  configuration?: { [key: string]: string };
  /**
   * Session-level query tags as key-value pairs. Serialized and passed via session configuration
   * as "QUERY_TAGS". Values may be null/undefined to include a key without a value.
   * If both queryTags and configuration.QUERY_TAGS are specified, queryTags takes precedence.
   */
  queryTags?: Record<string, string | null | undefined>;
}

export default interface IDBSQLClient {
  connect(options: ConnectionOptions): Promise<IDBSQLClient>;

  openSession(request?: OpenSessionRequest): Promise<IDBSQLSession>;

  close(): Promise<void>;
}
