import { HeadersInit } from 'node-fetch';
import http from 'http';
import IAuthentication from '../../contracts/IAuthentication';
import IDBSQLLogger from '../../../contracts/IDBSQLLogger';
import OAuthPersistence, { OAuthPersistenceCache } from './OAuthPersistence';
import OAuthManager, { OAuthManagerOptions } from './OAuthManager';
import { OAuthScopes, defaultOAuthScopes } from './OAuthScope';

interface DatabricksOAuthOptions extends OAuthManagerOptions {
  scopes?: OAuthScopes;
  logger?: IDBSQLLogger;
  persistence?: OAuthPersistence;
  headers?: HeadersInit;
}

export default class DatabricksOAuth implements IAuthentication {
  private readonly options: DatabricksOAuthOptions;

  private readonly logger?: IDBSQLLogger;

  private readonly manager: OAuthManager;

  private readonly defaultPersistence = new OAuthPersistenceCache();

  constructor(options: DatabricksOAuthOptions) {
    this.options = options;
    this.logger = options.logger;
    this.manager = OAuthManager.getManager(this.options);
  }

  public async authenticate(agent?: http.Agent): Promise<HeadersInit> {
    this.manager.setAgent(agent);

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
