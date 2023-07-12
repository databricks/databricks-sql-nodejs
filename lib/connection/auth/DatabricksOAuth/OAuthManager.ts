import { Issuer, BaseClient } from 'openid-client';
import HiveDriverError from '../../../errors/HiveDriverError';
import IDBSQLLogger, { LogLevel } from '../../../contracts/IDBSQLLogger';
import OAuthToken from './OAuthToken';
import AuthorizationCode from './AuthorizationCode';
import { OAuthScope, OAuthScopes } from './OAuthScope';

export interface OAuthManagerOptions {
  host: string;
  callbackPorts?: Array<number>;
  clientId?: string;
  logger?: IDBSQLLogger;
}

export default abstract class OAuthManager {
  protected readonly options: OAuthManagerOptions;

  protected readonly logger?: IDBSQLLogger;

  protected issuer?: Issuer;

  protected client?: BaseClient;

  constructor(options: OAuthManagerOptions) {
    this.options = options;
    this.logger = options.logger;
  }

  protected abstract getOIDCConfigUrl(): string;

  protected abstract getClientId(): string;

  protected abstract getCallbackPorts(): Array<number>;

  protected getScopes(requestedScopes: OAuthScopes): OAuthScopes {
    return requestedScopes;
  }

  protected async getClient(): Promise<BaseClient> {
    if (!this.issuer) {
      this.issuer = await Issuer.discover(this.getOIDCConfigUrl());
    }

    if (!this.client) {
      this.client = new this.issuer.Client({
        client_id: this.getClientId(),
        token_endpoint_auth_method: 'none',
      });
    }

    return this.client;
  }

  public async refreshAccessToken(token: OAuthToken): Promise<OAuthToken> {
    try {
      if (!token.hasExpired) {
        // The access token is fine. Just return it.
        return token;
      }
    } catch (error) {
      this.logger?.log(LogLevel.error, `${error}`);
      throw error;
    }

    if (!token.refreshToken) {
      const message = `OAuth access token expired on ${token.expirationTime}.`;
      this.logger?.log(LogLevel.error, message);
      throw new HiveDriverError(message);
    }

    // Try to refresh using the refresh token
    this.logger?.log(
      LogLevel.debug,
      `Attempting to refresh OAuth access token that expired on ${token.expirationTime}`,
    );

    const client = await this.getClient();
    const { access_token: accessToken, refresh_token: refreshToken } = await client.refresh(token.refreshToken);
    if (!accessToken || !refreshToken) {
      throw new Error('Failed to refresh token: invalid response');
    }
    return new OAuthToken(accessToken, refreshToken);
  }

  public async getToken(scopes: OAuthScopes): Promise<OAuthToken> {
    const client = await this.getClient();
    const authCode = new AuthorizationCode({
      client,
      ports: this.getCallbackPorts(),
      logger: this.logger,
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
      throw new Error('Failed to fetch access token');
    }

    return new OAuthToken(accessToken, refreshToken);
  }

  public static getManager(options: OAuthManagerOptions): OAuthManager {
    // normalize
    const host = options.host.toLowerCase().replace('https://', '').split('/')[0];

    // eslint-disable-next-line @typescript-eslint/no-use-before-define
    const managers = [AWSOAuthManager, AzureOAuthManager];

    for (const OAuthManagerClass of managers) {
      for (const domain of OAuthManagerClass.domains) {
        if (host.endsWith(domain)) {
          return new OAuthManagerClass(options);
        }
      }
    }

    throw new Error(`OAuth is not supported for ${options.host}`);
  }
}

export class AWSOAuthManager extends OAuthManager {
  public static domains = ['.cloud.databricks.com', '.dev.databricks.com'];

  public static defaultClientId = 'databricks-sql-connector';

  public static defaultCallbackPorts = [8030];

  protected getOIDCConfigUrl(): string {
    const { host } = this.options;
    const schema = host.startsWith('https://') ? '' : 'https://';
    const trailingSlash = host.endsWith('/') ? '' : '/';
    const oidcConfigPath = 'oidc/.well-known/oauth-authorization-server';
    return `${schema}${host}${trailingSlash}${oidcConfigPath}`;
  }

  protected getClientId(): string {
    return this.options.clientId ?? AWSOAuthManager.defaultClientId;
  }

  protected getCallbackPorts(): Array<number> {
    return this.options.callbackPorts ?? AWSOAuthManager.defaultCallbackPorts;
  }
}

export class AzureOAuthManager extends OAuthManager {
  public static domains = ['.azuredatabricks.net', '.databricks.azure.cn', '.databricks.azure.us'];

  public static defaultClientId = '96eecda7-19ea-49cc-abb5-240097d554f5';

  public static defaultCallbackPorts = [8030];

  public static datatricksAzureApp = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d';

  protected getOIDCConfigUrl(): string {
    return 'https://login.microsoftonline.com/organizations/v2.0/.well-known/openid-configuration';
  }

  protected getClientId(): string {
    return this.options.clientId ?? AzureOAuthManager.defaultClientId;
  }

  protected getCallbackPorts(): Array<number> {
    return this.options.callbackPorts ?? AzureOAuthManager.defaultCallbackPorts;
  }

  protected getScopes(requestedScopes: OAuthScopes): OAuthScopes {
    // There is no corresponding scopes in Azure, instead, access control will be delegated to Databricks
    const tenantId = AzureOAuthManager.datatricksAzureApp; // TODO: Get from env DATABRICKS_AZURE_TENANT_ID ?
    const azureScopes = [`${tenantId}/user_impersonation`];

    if (requestedScopes.includes(OAuthScope.offlineAccess)) {
      azureScopes.push(OAuthScope.offlineAccess);
    }

    return azureScopes;
  }
}
