import { expect } from 'chai';
import sinon from 'sinon';
import FederationProvider from '../../../../../lib/connection/auth/tokenProvider/FederationProvider';
import ITokenProvider from '../../../../../lib/connection/auth/tokenProvider/ITokenProvider';
import Token from '../../../../../lib/connection/auth/tokenProvider/Token';

function createJWT(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64');
  const body = Buffer.from(JSON.stringify(payload)).toString('base64');
  return `${header}.${body}.signature`;
}

class MockTokenProvider implements ITokenProvider {
  public tokenToReturn: Token;

  constructor(accessToken: string) {
    this.tokenToReturn = new Token(accessToken);
  }

  async getToken(): Promise<Token> {
    return this.tokenToReturn;
  }

  getName(): string {
    return 'MockTokenProvider';
  }
}

describe('FederationProvider', () => {
  describe('getToken', () => {
    it('should pass through token if issuer matches Databricks host', async () => {
      const jwt = createJWT({ iss: 'https://my-workspace.cloud.databricks.com' });
      const baseProvider = new MockTokenProvider(jwt);
      const federationProvider = new FederationProvider(baseProvider, 'my-workspace.cloud.databricks.com');

      const token = await federationProvider.getToken();

      expect(token.accessToken).to.equal(jwt);
    });

    it('should pass through non-JWT tokens', async () => {
      const baseProvider = new MockTokenProvider('not-a-jwt-token');
      const federationProvider = new FederationProvider(baseProvider, 'my-workspace.cloud.databricks.com');

      const token = await federationProvider.getToken();

      expect(token.accessToken).to.equal('not-a-jwt-token');
    });

    it('should pass through token when issuer matches (case insensitive)', async () => {
      const jwt = createJWT({ iss: 'https://MY-WORKSPACE.CLOUD.DATABRICKS.COM' });
      const baseProvider = new MockTokenProvider(jwt);
      const federationProvider = new FederationProvider(baseProvider, 'my-workspace.cloud.databricks.com');

      const token = await federationProvider.getToken();

      expect(token.accessToken).to.equal(jwt);
    });

    it('should pass through token when issuer matches (ignoring port)', async () => {
      const jwt = createJWT({ iss: 'https://my-workspace.cloud.databricks.com:443' });
      const baseProvider = new MockTokenProvider(jwt);
      const federationProvider = new FederationProvider(baseProvider, 'my-workspace.cloud.databricks.com');

      const token = await federationProvider.getToken();

      expect(token.accessToken).to.equal(jwt);
    });
  });

  describe('getName', () => {
    it('should return wrapped name', () => {
      const baseProvider = new MockTokenProvider('token');
      const federationProvider = new FederationProvider(baseProvider, 'host.com');

      expect(federationProvider.getName()).to.equal('federated[MockTokenProvider]');
    });
  });
});
