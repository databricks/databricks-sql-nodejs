import http from 'http';
import { Issuer, BaseClient, custom } from 'openid-client';
import HiveDriverError from '../../../errors/HiveDriverError';
import { LogLevel } from '../../../contracts/IDBSQLLogger';
import OAuthToken from './OAuthToken';
import AuthorizationCode from './AuthorizationCode';
import { OAuthScope, OAuthScopes } from './OAuthScope';
import IClientContext from '../../../contracts/IClientContext';

export interface OAuthManagerOptions {
  host: string;
  callbackPorts?: Array<number>;
  clientId?: string;
  azureTenantId?: string;
  clientSecret?: string;
  useDatabricksOAuthInAzure?: boolean;
  context: IClientContext;
}

function getDatabricksOIDCUrl(host: string): string {
  const schema = host.startsWith('https://') ? '' : 'https://';
  const trailingSlash = host.endsWith('/') ? '' : '/';
  return `${schema}${host}${trailingSlash}oidc`;
}

export default abstract class OAuthManager {
  protected readonly context: IClientContext;

  protected readonly options: OAuthManagerOptions;

  protected agent?: http.Agent;

  protected issuer?: Issuer;

  protected client?: BaseClient;

  constructor(options: OAuthManagerOptions) {
    this.options = options;
    this.context = options.context;
  }

  protected abstract getOIDCConfigUrl(): string;

  protected abstract getAuthorizationUrl(): string;

  protected abstract getClientId(): string;

  protected abstract getCallbackPorts(): Array<number>;

  protected getScopes(requestedScopes: OAuthScopes): OAuthScopes {
    return requestedScopes;
  }

  protected async getClient(): Promise<BaseClient> {
    // Obtain http agent each time when we need an OAuth client
    // to ensure that we always use a valid agent instance
    const connectionProvider = await this.context.getConnectionProvider();
    this.agent = await connectionProvider.getAgent();

    const getHttpOptions = () => ({
      agent: this.agent,
    });

    if (!this.issuer) {
      // To use custom http agent in Issuer.discover(), we'd have to set Issuer[custom.http_options].
      // However, that's a static field, and if multiple instances of OAuthManager used, race condition
      // may occur when they simultaneously override that field and then try to use Issuer.discover().
      // Therefore we create a local class derived from Issuer, and set that field for it, thus making
      // sure that it will not interfere with other instances (or other code that may use Issuer)
      class CustomIssuer extends Issuer {
        static [custom.http_options] = getHttpOptions;
      }

      const issuer = await CustomIssuer.discover(this.getOIDCConfigUrl());

      // Overwrite `authorization_endpoint` in default config (specifically needed for Azure flow
      // where this URL has to be different)
      this.issuer = new Issuer({
        ...issuer.metadata,
        authorization_endpoint: this.getAuthorizationUrl(),
      });

      this.issuer[custom.http_options] = getHttpOptions;
    }

    if (!this.client) {
      this.client = new this.issuer.Client({
        client_id: this.getClientId(),
        client_secret: this.options.clientSecret,
        token_endpoint_auth_method: this.options.clientSecret === undefined ? 'none' : 'client_secret_basic',
      });

      this.client[custom.http_options] = getHttpOptions;
    }

    return this.client;
  }

  private async refreshAccessTokenU2M(token: OAuthToken): Promise<OAuthToken> {
    if (!token.refreshToken) {
      const message = `OAuth access token expired on ${token.expirationTime}.`;
      this.context.getLogger().log(LogLevel.error, message);
      throw new HiveDriverError(message);
    }

    // Try to refresh using the refresh token
    this.context
      .getLogger()
      .log(LogLevel.debug, `Attempting to refresh OAuth access token that expired on ${token.expirationTime}`);

    const client = await this.getClient();
    const { access_token: accessToken, refresh_token: refreshToken } = await client.refresh(token.refreshToken);
    if (!accessToken || !refreshToken) {
      throw new Error('Failed to refresh token: invalid response');
    }
    return new OAuthToken(accessToken, refreshToken);
  }

  private async refreshAccessTokenM2M(): Promise<OAuthToken> {
    const { access_token: accessToken, refresh_token: refreshToken } = await this.getTokenM2M();

    if (!accessToken) {
      throw new Error('Failed to fetch access token');
    }

    return new OAuthToken(accessToken, refreshToken);
  }

  public async refreshAccessToken(token: OAuthToken): Promise<OAuthToken> {
    try {
      if (!token.hasExpired) {
        // The access token is fine. Just return it.
        return token;
      }
    } catch (error) {
      this.context.getLogger().log(LogLevel.error, `${error}`);
      throw error;
    }

    return this.options.clientSecret === undefined ? this.refreshAccessTokenU2M(token) : this.refreshAccessTokenM2M();
  }

  private async getTokenU2M(scopes: OAuthScopes) {
    const client = await this.getClient();

    const authCode = new AuthorizationCode({
      client,
      ports: this.getCallbackPorts(),
      context: this.context,
    });

    const mappedScopes = this.getScopes(scopes);

    const { code, verifier, redirectUri } = await authCode.fetch(mappedScopes);

    return client.grant({
      grant_type: 'authorization_code',
      code,
      code_verifier: verifier,
      redirect_uri: redirectUri,
    });
  }

  private async getTokenM2M() {
    const client = await this.getClient();

    // M2M flow doesn't really support token refreshing, and refresh should not be available
    // in response. Each time access token expires, client can just acquire a new one using
    // client secret. Here we explicitly return access token only as a sign that we're not going
    // to use refresh token for M2M flow anywhere later
    const { access_token: accessToken } = await client.grant({
      grant_type: 'client_credentials',
      scope: 'all-apis', // this is the only allowed scope for M2M flow
    });
    return { access_token: accessToken, refresh_token: undefined };
  }

  public async getToken(scopes: OAuthScopes): Promise<OAuthToken> {
    const { access_token: accessToken, refresh_token: refreshToken } =
      this.options.clientSecret === undefined ? await this.getTokenU2M(scopes) : await this.getTokenM2M();

    if (!accessToken) {
      throw new Error('Failed to fetch access token');
    }

    return new OAuthToken(accessToken, refreshToken);
  }

  public static getManager(options: OAuthManagerOptions): OAuthManager {
    // normalize
    const host = options.host.toLowerCase().replace('https://', '').split('/')[0];

    const awsDomains = ['.cloud.databricks.com', '.dev.databricks.com'];
    const isAWSDomain = awsDomains.some((domain) => host.endsWith(domain));

    if (isAWSDomain) {
      // eslint-disable-next-line @typescript-eslint/no-use-before-define
      return new DatabricksOAuthManager(options);
    }

    const azureDomains = ['.azuredatabricks.net', '.databricks.azure.cn', '.databricks.azure.us'];
    const isAzureDomain = azureDomains.some((domain) => host.endsWith(domain));

    if (isAzureDomain) {
      if (options.useDatabricksOAuthInAzure) {
        // eslint-disable-next-line @typescript-eslint/no-use-before-define
        return new DatabricksOAuthManager(options);
      }
      // eslint-disable-next-line @typescript-eslint/no-use-before-define
      return new AzureOAuthManager(options);
    }

    throw new Error(`OAuth is not supported for ${options.host}`);
  }
}

// Databricks InHouse OAuth Manager
export class DatabricksOAuthManager extends OAuthManager {
  public static defaultClientId = 'databricks-sql-connector';

  public static defaultCallbackPorts = [8030];

  protected getOIDCConfigUrl(): string {
    return `${getDatabricksOIDCUrl(this.options.host)}/.well-known/oauth-authorization-server`;
  }

  protected getAuthorizationUrl(): string {
    return `${getDatabricksOIDCUrl(this.options.host)}/oauth2/v2.0/authorize`;
  }

  protected getClientId(): string {
    return this.options.clientId ?? DatabricksOAuthManager.defaultClientId;
  }

  protected getCallbackPorts(): Array<number> {
    return this.options.callbackPorts ?? DatabricksOAuthManager.defaultCallbackPorts;
  }
}

export class AzureOAuthManager extends OAuthManager {
  public static defaultClientId = '96eecda7-19ea-49cc-abb5-240097d554f5';

  public static defaultCallbackPorts = [8030];

  public static datatricksAzureApp = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d';

  protected getOIDCConfigUrl(): string {
    return 'https://login.microsoftonline.com/organizations/v2.0/.well-known/openid-configuration';
  }

  protected getAuthorizationUrl(): string {
    return `${getDatabricksOIDCUrl(this.options.host)}/oauth2/v2.0/authorize`;
  }

  protected getClientId(): string {
    return this.options.clientId ?? AzureOAuthManager.defaultClientId;
  }

  protected getCallbackPorts(): Array<number> {
    return this.options.callbackPorts ?? AzureOAuthManager.defaultCallbackPorts;
  }

  protected getScopes(requestedScopes: OAuthScopes): OAuthScopes {
    // There is no corresponding scopes in Azure, instead, access control will be delegated to Databricks
    const tenantId = this.options.azureTenantId ?? AzureOAuthManager.datatricksAzureApp;
    const azureScopes = [`${tenantId}/user_impersonation`];

    if (requestedScopes.includes(OAuthScope.offlineAccess)) {
      azureScopes.push(OAuthScope.offlineAccess);
    }

    return azureScopes;
  }
}
