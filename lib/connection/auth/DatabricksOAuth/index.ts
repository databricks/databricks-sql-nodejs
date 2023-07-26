import { HttpHeaders } from 'thrift';
import IAuthentication from '../../contracts/IAuthentication';
import HttpTransport from '../../transports/HttpTransport';
import IDBSQLLogger from '../../../contracts/IDBSQLLogger';
import OAuthPersistence from './OAuthPersistence';
import OAuthManager, { OAuthManagerOptions } from './OAuthManager';
import { OAuthScopes, defaultOAuthScopes } from './OAuthScope';

interface DatabricksOAuthOptions extends OAuthManagerOptions {
  scopes?: OAuthScopes;
  logger?: IDBSQLLogger;
  persistence?: OAuthPersistence;
  headers?: HttpHeaders;
}

export default class DatabricksOAuth implements IAuthentication {
  private readonly options: DatabricksOAuthOptions;

  private readonly logger?: IDBSQLLogger;

  private readonly manager: OAuthManager;

  constructor(options: DatabricksOAuthOptions) {
    this.options = options;
    this.logger = options.logger;
    this.manager = OAuthManager.getManager(this.options);
  }

  public async authenticate(transport: HttpTransport): Promise<void> {
    const { host, scopes, headers, persistence } = this.options;

    let token = await persistence?.read(host);
    if (!token) {
      token = await this.manager.getToken(scopes ?? defaultOAuthScopes);
    }

    token = await this.manager.refreshAccessToken(token);
    await persistence?.persist(host, token);

    transport.updateHeaders({
      ...headers,
      Authorization: `Bearer ${token.accessToken}`,
    });
  }
}
