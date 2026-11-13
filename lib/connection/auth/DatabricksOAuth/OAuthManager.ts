import http from 'http';
import { Issuer, BaseClient, custom } from 'openid-client';
import AuthenticationError from '../../../errors/AuthenticationError';
import { LogLevel } from '../../../contracts/IDBSQLLogger';
import OAuthToken from './OAuthToken';
import AuthorizationCode from './AuthorizationCode';
import { OAuthScope, OAuthScopes, defaultOAuthScopes, scopeDelimiter } from './OAuthScope';
import IClientContext from '../../../contracts/IClientContext';
import { discoverAzureTenantId } from './AzureTenantDiscovery';

export enum OAuthFlow {
  U2M = 'U2M',
  M2M = 'M2M',
}

export interface OAuthManagerOptions {
  flow: OAuthFlow;
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

  protected httpAgent?: http.Agent;

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

  // `requestedScopes` is undefined when the caller configured none; return the flow's default then.
  protected abstract getScopes(requestedScopes?: OAuthScopes): OAuthScopes;

  protected async getClient(): Promise<BaseClient> {
    // Obtain http agent each time when we need an OAuth client
    // to ensure that we always use a valid agent instance
    const connectionProvider = await this.context.getConnectionProvider();
    // openid-client picks `http`/`https` by each request's URL, so the agent must
    // match that URL's protocol rather than the SQL transport's.
    this.agent = await connectionProvider.getAgent('https:');
    this.httpAgent = await connectionProvider.getAgent('http:');

    const getHttpOptions = (url?: { protocol: string }) => ({
      agent: url?.protocol === 'http:' ? this.httpAgent : this.agent,
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
      throw new AuthenticationError(message);
    }

    // Try to refresh using the refresh token
    this.context
      .getLogger()
      .log(LogLevel.debug, `Attempting to refresh OAuth access token that expired on ${token.expirationTime}`);

    const client = await this.getClient();
    const { access_token: accessToken, refresh_token: refreshToken } = await client.refresh(token.refreshToken);
    if (!accessToken || !refreshToken) {
      throw new AuthenticationError('Failed to refresh token: invalid response');
    }
    return new OAuthToken(accessToken, refreshToken, token.scopes);
  }

  private async refreshAccessTokenM2M(token: OAuthToken): Promise<OAuthToken> {
    return this.getTokenM2M(token.scopes ?? []);
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

    switch (this.options.flow) {
      case OAuthFlow.U2M:
        return this.refreshAccessTokenU2M(token);
      case OAuthFlow.M2M:
        return this.refreshAccessTokenM2M(token);
      // no default
    }
  }

  private async getTokenU2M(scopes?: OAuthScopes): Promise<OAuthToken> {
    const client = await this.getClient();

    const authCode = new AuthorizationCode({
      client,
      ports: this.getCallbackPorts(),
      context: this.context,
    });

    const mappedScopes = this.getScopes(scopes);

    const { code, verifier, redirectUri } = await authCode.fetch(mappedScopes);

    const { access_token: accessToken, refresh_token: refreshToken } = await client.grant({
      grant_type: 'authorization_code',
      code,
      code_verifier: verifier,
      redirect_uri: redirectUri,
    });

    if (!accessToken) {
      throw new AuthenticationError('Failed to fetch access token');
    }
    return new OAuthToken(accessToken, refreshToken, mappedScopes);
  }

  private async getTokenM2M(scopes?: OAuthScopes): Promise<OAuthToken> {
    const client = await this.getClient();

    const mappedScopes = this.getScopes(scopes);

    // M2M flow doesn't really support token refreshing, and refresh should not be available
    // in response. Each time access token expires, client can just acquire a new one using
    // client secret. Here we explicitly return access token only as a sign that we're not going
    // to use refresh token for M2M flow anywhere later
    const { access_token: accessToken } = await client.grant({
      grant_type: 'client_credentials',
      scope: mappedScopes.join(scopeDelimiter),
    });

    if (!accessToken) {
      throw new AuthenticationError('Failed to fetch access token');
    }
    return new OAuthToken(accessToken, undefined, mappedScopes);
  }

  /**
   * The scopes a new grant would request for `scopes` (after the flow's defaults and mapping), i.e.
   * the scopes `getToken(scopes)` records on the token it returns.
   */
  public getRequestedScopes(scopes?: OAuthScopes): OAuthScopes {
    return this.getScopes(scopes);
  }

  public async getToken(scopes?: OAuthScopes): Promise<OAuthToken> {
    switch (this.options.flow) {
      case OAuthFlow.U2M:
        return this.getTokenU2M(scopes);
      case OAuthFlow.M2M:
        return this.getTokenM2M(scopes);
      // no default
    }
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

    const gcpDomains = ['.gcp.databricks.com'];
    const isGCPDomain = gcpDomains.some((domain) => host.endsWith(domain));
    if (isGCPDomain) {
      // eslint-disable-next-line @typescript-eslint/no-use-before-define
      return new DatabricksOAuthManager(options);
    }

    if (options.useDatabricksOAuthInAzure) {
      const azureDomains = ['.azuredatabricks.net', '.databricks.azure.cn'];
      const isAzureDomain = azureDomains.some((domain) => host.endsWith(domain));
      if (isAzureDomain) {
        // eslint-disable-next-line @typescript-eslint/no-use-before-define
        return new DatabricksOAuthManager(options);
      }
    } else {
      const azureDomains = ['.azuredatabricks.net', '.databricks.azure.us', '.databricks.azure.cn'];
      const isAzureDomain = azureDomains.some((domain) => host.endsWith(domain));
      if (isAzureDomain) {
        // eslint-disable-next-line @typescript-eslint/no-use-before-define
        return new AzureOAuthManager(options);
      }
    }

    throw new AuthenticationError(`OAuth is not supported for ${options.host}`);
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

  protected getScopes(requestedScopes?: OAuthScopes): OAuthScopes {
    if (requestedScopes && requestedScopes.length > 0) {
      return requestedScopes;
    }
    return this.options.flow === OAuthFlow.M2M ? [OAuthScope.allAPIs] : defaultOAuthScopes;
  }
}

export class AzureOAuthManager extends OAuthManager {
  public static defaultClientId = '96eecda7-19ea-49cc-abb5-240097d554f5';

  public static defaultCallbackPorts = [8030];

  public static datatricksAzureApp = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d';

  private discoveredTenantId?: string;

  protected async getClient(): Promise<BaseClient> {
    // Entra rejects the client-credentials grant on the multi-tenant `/organizations` authority,
    // so M2M without an explicit tenant discovers the workspace's tenant first.
    if (this.options.flow === OAuthFlow.M2M && !this.options.azureTenantId?.trim() && !this.discoveredTenantId) {
      const connectionProvider = await this.context.getConnectionProvider();
      const agent = await connectionProvider.getAgent('https:');
      this.discoveredTenantId = await discoverAzureTenantId(this.options.host, agent);
    }
    return super.getClient();
  }

  protected getOIDCConfigUrl(): string {
    // Use logical OR so empty / whitespace-only azureTenantId also falls back to /organizations/
    // (`??` only substitutes for null/undefined, leaving `''` to produce a malformed `//v2.0/...` URL).
    const tenantPath = this.options.azureTenantId?.trim() || this.discoveredTenantId || 'organizations';
    return `https://login.microsoftonline.com/${tenantPath}/v2.0/.well-known/openid-configuration`;
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

  protected getScopes(requestedScopes: OAuthScopes = defaultOAuthScopes): OAuthScopes {
    // There is no corresponding scopes in Azure, instead, access control will be delegated to Databricks.
    // Scope must be the Azure *resource* ID (the Databricks Azure Login App), NOT the tenant ID.
    const resourceId = AzureOAuthManager.datatricksAzureApp;

    const azureScopes = [];

    switch (this.options.flow) {
      case OAuthFlow.U2M:
        azureScopes.push(`${resourceId}/user_impersonation`);
        break;
      case OAuthFlow.M2M:
        azureScopes.push(`${resourceId}/.default`);
        break;
      // no default
    }

    if (requestedScopes.includes(OAuthScope.offlineAccess)) {
      azureScopes.push(OAuthScope.offlineAccess);
    }

    return azureScopes;
  }
}
