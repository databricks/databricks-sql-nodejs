import { HeadersInit } from 'node-fetch';
import IAuthentication from '../../contracts/IAuthentication';
import OAuthPersistence, { OAuthPersistenceCache } from './OAuthPersistence';
import OAuthManager, { OAuthManagerOptions, OAuthFlow } from './OAuthManager';
import { OAuthScopes, normalizeOAuthScopes } from './OAuthScope';
import IClientContext from '../../../contracts/IClientContext';

export { OAuthFlow };

export interface DatabricksOAuthOptions extends OAuthManagerOptions {
  scopes?: OAuthScopes;
  persistence?: OAuthPersistence;
  headers?: HeadersInit;
}

export default class DatabricksOAuth implements IAuthentication {
  private readonly context: IClientContext;

  private readonly options: DatabricksOAuthOptions;

  private manager?: OAuthManager;

  private readonly defaultPersistence = new OAuthPersistenceCache();

  constructor(options: DatabricksOAuthOptions) {
    this.context = options.context;
    this.options = options;
  }

  public async authenticate(): Promise<HeadersInit> {
    const { host, scopes, headers } = this.options;

    const persistence = this.options.persistence ?? this.defaultPersistence;

    const normalizedScopes = normalizeOAuthScopes(scopes);
    // Unset or blank scopes let the manager apply its flow's default
    const requestedScopes = normalizedScopes.length > 0 ? normalizedScopes : undefined;

    let token = await persistence.read(host);
    // M2M tokens can't be refreshed, so an expired persisted one is replaced by a new grant. Request
    // the configured scopes for it, not the scopes the cached token was issued with.
    if (token && this.options.flow === OAuthFlow.M2M && token.hasExpired) {
      token = undefined;
    }
    if (!token) {
      token = await this.getManager().getToken(requestedScopes);
    }

    token = await this.getManager().refreshAccessToken(token);
    await persistence.persist(host, token);

    return {
      ...headers,
      Authorization: `Bearer ${token.accessToken}`,
    };
  }

  private getManager(): OAuthManager {
    if (!this.manager) {
      this.manager = OAuthManager.getManager(this.options);
    }
    return this.manager;
  }
}
