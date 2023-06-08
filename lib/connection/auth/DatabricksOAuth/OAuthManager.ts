import { Issuer, BaseClient } from 'openid-client';
import HiveDriverError from '../../../errors/HiveDriverError';
import IDBSQLLogger, { LogLevel } from '../../../contracts/IDBSQLLogger';
import OAuthToken from './OAuthToken';
import AuthorizationCode from './AuthorizationCode';

const oidcConfigPath = 'oidc/.well-known/oauth-authorization-server';

export interface OAuthManagerOptions {
  host: string;
  callbackPorts: Array<number>;
  clientId: string;
  logger?: IDBSQLLogger;
}

export default class OAuthManager {
  private readonly options: OAuthManagerOptions;

  private readonly logger?: IDBSQLLogger;

  private issuer?: Issuer;

  private client?: BaseClient;

  constructor(options: OAuthManagerOptions) {
    this.options = options;
    this.logger = options.logger;
  }

  private async getClient(): Promise<BaseClient> {
    if (!this.issuer) {
      const { host } = this.options;
      const schema = host.startsWith('https://') ? '' : 'https://';
      const trailingSlash = host.endsWith('/') ? '' : '/';
      this.issuer = await Issuer.discover(`${schema}${host}${trailingSlash}${oidcConfigPath}`);
    }

    if (!this.client) {
      this.client = new this.issuer.Client({
        client_id: this.options.clientId,
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

  public async getToken(scopes: Array<string>): Promise<OAuthToken> {
    const client = await this.getClient();
    const authCode = new AuthorizationCode({
      client,
      ports: this.options.callbackPorts,
      logger: this.logger,
    });

    const { code, verifier, redirectUri } = await authCode.fetch(scopes);

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
}
