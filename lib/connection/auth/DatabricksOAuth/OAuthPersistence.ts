import OAuthToken from './OAuthToken';

export default interface OAuthPersistence {
  persist(host: string, token: OAuthToken): Promise<void>;

  read(host: string): Promise<OAuthToken | undefined>;
}

export class OAuthPersistenceCache implements OAuthPersistence {
  private tokens: Record<string, OAuthToken | undefined> = {};

  async persist(host: string, token: OAuthToken) {
    this.tokens[host] = token;
  }

  async read(host: string) {
    return this.tokens[host];
  }
}
