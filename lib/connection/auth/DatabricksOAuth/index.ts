import { HeadersInit } from 'node-fetch';
import IAuthentication from '../../contracts/IAuthentication';
import OAuthPersistence, { OAuthPersistenceCache } from './OAuthPersistence';
import OAuthManager, { OAuthManagerOptions } from './OAuthManager';
import { OAuthScopes, defaultOAuthScopes } from './OAuthScope';
import IClientContext from '../../../contracts/IClientContext';

interface DatabricksOAuthOptions extends OAuthManagerOptions {
  scopes?: OAuthScopes;
  persistence?: OAuthPersistence;
  headers?: HeadersInit;
}

export default class DatabricksOAuth implements IAuthentication {
  private readonly context: IClientContext;

  private readonly options: DatabricksOAuthOptions;

  private readonly manager: OAuthManager;

  private readonly defaultPersistence = new OAuthPersistenceCache();

  constructor(options: DatabricksOAuthOptions) {
    this.context = options.context;
    this.options = options;
    this.manager = OAuthManager.getManager(this.options);
  }

  public async authenticate(): Promise<HeadersInit> {
    const { host, scopes, headers } = this.options;

    const persistence = this.options.persistence ?? this.defaultPersistence;

    let token = await persistence.read(host);
    if (!token) {
      token = await this.manager.getToken(scopes ?? defaultOAuthScopes);
    }

    token = await this.manager.refreshAccessToken(token);
    await persistence.persist(host, token);

    return {
      ...headers,
      Authorization: `Bearer ${token.accessToken}`,
    };
  }
}
