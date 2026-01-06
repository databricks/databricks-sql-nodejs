import { expect } from 'chai';
import sinon from 'sinon';
import nock from 'nock';
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
  afterEach(() => {
    nock.cleanAll();
  });

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

    it('should exchange token when issuer differs from Databricks host', async () => {
      const externalJwt = createJWT({ iss: 'https://external-idp.com' });
      const exchangedToken = 'exchanged-databricks-token';
      const baseProvider = new MockTokenProvider(externalJwt);

      nock('https://my-workspace.cloud.databricks.com')
        .post('/oidc/v1/token')
        .reply(200, {
          access_token: exchangedToken,
          token_type: 'Bearer',
          expires_in: 3600,
        });

      const federationProvider = new FederationProvider(baseProvider, 'https://my-workspace.cloud.databricks.com');

      const token = await federationProvider.getToken();

      expect(token.accessToken).to.equal(exchangedToken);
      expect(token.tokenType).to.equal('Bearer');
    });

    it('should include client_id in exchange request when provided', async () => {
      const externalJwt = createJWT({ iss: 'https://external-idp.com' });
      const baseProvider = new MockTokenProvider(externalJwt);

      let requestBody: string | undefined;
      nock('https://my-workspace.cloud.databricks.com')
        .post('/oidc/v1/token', (body) => {
          requestBody = body;
          return true;
        })
        .reply(200, {
          access_token: 'exchanged-token',
          token_type: 'Bearer',
        });

      const federationProvider = new FederationProvider(baseProvider, 'https://my-workspace.cloud.databricks.com', {
        clientId: 'my-client-id',
      });

      await federationProvider.getToken();

      expect(requestBody).to.include('client_id=my-client-id');
    });

    it('should fall back to original token on exchange failure by default', async () => {
      const externalJwt = createJWT({ iss: 'https://external-idp.com' });
      const baseProvider = new MockTokenProvider(externalJwt);

      nock('https://my-workspace.cloud.databricks.com')
        .post('/oidc/v1/token')
        .reply(401, { error: 'unauthorized' });

      const federationProvider = new FederationProvider(baseProvider, 'https://my-workspace.cloud.databricks.com');

      const token = await federationProvider.getToken();

      expect(token.accessToken).to.equal(externalJwt);
    });

    it('should throw error on exchange failure when fallback is disabled', async () => {
      const externalJwt = createJWT({ iss: 'https://external-idp.com' });
      const baseProvider = new MockTokenProvider(externalJwt);

      nock('https://my-workspace.cloud.databricks.com')
        .post('/oidc/v1/token')
        .reply(401, { error: 'unauthorized' });

      const federationProvider = new FederationProvider(baseProvider, 'https://my-workspace.cloud.databricks.com', {
        returnOriginalTokenOnFailure: false,
      });

      try {
        await federationProvider.getToken();
        expect.fail('Should have thrown an error');
      } catch (error: any) {
        expect(error.message).to.include('Token exchange failed');
      }
    });

    it('should handle host without protocol', async () => {
      const externalJwt = createJWT({ iss: 'https://external-idp.com' });
      const baseProvider = new MockTokenProvider(externalJwt);

      nock('https://my-workspace.cloud.databricks.com')
        .post('/oidc/v1/token')
        .reply(200, {
          access_token: 'exchanged-token',
          token_type: 'Bearer',
        });

      const federationProvider = new FederationProvider(
        baseProvider,
        'my-workspace.cloud.databricks.com', // No protocol
      );

      const token = await federationProvider.getToken();

      expect(token.accessToken).to.equal('exchanged-token');
    });

    it('should send correct token exchange parameters', async () => {
      const externalJwt = createJWT({ iss: 'https://external-idp.com' });
      const baseProvider = new MockTokenProvider(externalJwt);

      let requestBody: string | undefined;
      nock('https://my-workspace.cloud.databricks.com')
        .post('/oidc/v1/token', (body) => {
          requestBody = body;
          return true;
        })
        .reply(200, {
          access_token: 'exchanged-token',
        });

      const federationProvider = new FederationProvider(baseProvider, 'https://my-workspace.cloud.databricks.com');

      await federationProvider.getToken();

      expect(requestBody).to.include('grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange');
      expect(requestBody).to.include('subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Ajwt');
      expect(requestBody).to.include(`subject_token=${encodeURIComponent(externalJwt)}`);
      expect(requestBody).to.include('scope=sql');
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
