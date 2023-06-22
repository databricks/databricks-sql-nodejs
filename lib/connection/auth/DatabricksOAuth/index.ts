import { HttpHeaders } from 'thrift';
import IAuthentication from '../../contracts/IAuthentication';
import HttpTransport from '../../transports/HttpTransport';
import IDBSQLLogger from '../../../contracts/IDBSQLLogger';
import OAuthPersistence from './OAuthPersistence';
import OAuthManager from './OAuthManager';

interface DatabricksOAuthOptions {
  host: string;
  redirectPorts?: Array<number>;
  clientId?: string;
  scopes?: Array<string>;
  logger?: IDBSQLLogger;
  persistence?: OAuthPersistence;
  headers?: HttpHeaders;
}

const defaultOAuthOptions = {
  clientId: 'databricks-sql-connector',
  redirectPorts: [8030],
  scopes: ['sql', 'offline_access'],
} satisfies Partial<DatabricksOAuthOptions>;

export default class DatabricksOAuth implements IAuthentication {
  private readonly host: string;

  private readonly redirectPorts: Array<number>;

  private readonly clientId: string;

  private readonly scopes: Array<string>;

  private readonly logger?: IDBSQLLogger;

  private readonly persistence?: OAuthPersistence;

  private readonly headers?: HttpHeaders;

  private readonly manager: OAuthManager;

  constructor(options: DatabricksOAuthOptions) {
    this.host = options.host;
    this.redirectPorts = options.redirectPorts || defaultOAuthOptions.redirectPorts;
    this.clientId = options.clientId || defaultOAuthOptions.clientId;
    this.scopes = options.scopes || defaultOAuthOptions.scopes;
    this.logger = options.logger;
    this.persistence = options.persistence;
    this.headers = options.headers;

    this.manager = new OAuthManager({
      host: this.host,
      callbackPorts: this.redirectPorts,
      clientId: this.clientId,
      logger: this.logger,
    });
  }

  public async authenticate(transport: HttpTransport): Promise<void> {
    let token = await this.persistence?.read(this.host);
    if (!token) {
      token = await this.manager.getToken(this.scopes);
    }

    token = await this.manager.refreshAccessToken(token);
    await this.persistence?.persist(this.host, token);

    transport.updateHeaders({
      ...this.headers,
      Authorization: `Bearer ${token.accessToken}`,
    });
  }
}
