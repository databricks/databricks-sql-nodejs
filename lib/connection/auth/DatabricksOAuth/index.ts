import IAuthentication from '../../contracts/IAuthentication';
import ITransport from '../../contracts/ITransport';
import IDBSQLLogger from '../../../contracts/IDBSQLLogger';
import { AuthOptions } from '../../types/AuthOptions';
import OAuthPersistence from './OAuthPersistence';
import OAuthManager from './OAuthManager';

interface DatabricksOAuthOptions extends AuthOptions {
  host: string;
  redirectPorts?: Array<number>;
  clientId?: string;
  scopes?: Array<string>;
  logger?: IDBSQLLogger;
  persistence?: OAuthPersistence;
  headers?: object;
}

const defaultOAuthOptions = {
  clientId: 'databricks-sql-python',
  redirectPorts: [8020, 8021, 8022, 8023, 8024, 8025],
  scopes: ['sql', 'offline_access'],
} satisfies Partial<DatabricksOAuthOptions>;

export default class DatabricksOAuth implements IAuthentication {
  private readonly host: string;

  private readonly redirectPorts: Array<number>;

  private readonly clientId: string;

  private readonly scopes: Array<string>;

  private readonly logger?: IDBSQLLogger;

  private readonly persistence?: OAuthPersistence;

  private readonly headers?: object;

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

  async authenticate(transport: ITransport): Promise<ITransport> {
    let token = await this.persistence?.read(this.host);
    if (!token) {
      token = await this.manager.getToken(this.scopes);
    }

    token = await this.manager.refreshAccessToken(token);
    await this.persistence?.persist(this.host, token);

    transport.setOptions('headers', {
      ...this.headers,
      Authorization: `Bearer ${token.accessToken}`,
    });

    return transport;
  }
}
