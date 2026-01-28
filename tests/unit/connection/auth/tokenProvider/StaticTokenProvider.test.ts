import { expect } from 'chai';
import StaticTokenProvider from '../../../../../lib/connection/auth/tokenProvider/StaticTokenProvider';

function createJWT(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64');
  const body = Buffer.from(JSON.stringify(payload)).toString('base64');
  return `${header}.${body}.signature`;
}

describe('StaticTokenProvider', () => {
  describe('constructor', () => {
    it('should create provider with access token only', async () => {
      const provider = new StaticTokenProvider('my-access-token');
      const token = await provider.getToken();

      expect(token.accessToken).to.equal('my-access-token');
      expect(token.tokenType).to.equal('Bearer');
    });

    it('should create provider with custom options', async () => {
      const expiresAt = new Date('2025-01-01T00:00:00Z');
      const provider = new StaticTokenProvider('my-access-token', {
        tokenType: 'CustomType',
        expiresAt,
        refreshToken: 'refresh-token',
        scopes: ['read', 'write'],
      });
      const token = await provider.getToken();

      expect(token.accessToken).to.equal('my-access-token');
      expect(token.tokenType).to.equal('CustomType');
      expect(token.expiresAt).to.deep.equal(expiresAt);
      expect(token.refreshToken).to.equal('refresh-token');
      expect(token.scopes).to.deep.equal(['read', 'write']);
    });
  });

  describe('fromJWT', () => {
    it('should create provider from JWT and extract expiration', async () => {
      const exp = Math.floor(Date.now() / 1000) + 3600;
      const jwt = createJWT({ exp, iss: 'test-issuer' });

      const provider = StaticTokenProvider.fromJWT(jwt);
      const token = await provider.getToken();

      expect(token.accessToken).to.equal(jwt);
      expect(token.expiresAt).to.be.instanceOf(Date);
      expect(Math.floor(token.expiresAt!.getTime() / 1000)).to.equal(exp);
    });

    it('should create provider from JWT with custom options', async () => {
      const jwt = createJWT({ exp: Math.floor(Date.now() / 1000) + 3600 });

      const provider = StaticTokenProvider.fromJWT(jwt, {
        tokenType: 'CustomType',
        refreshToken: 'refresh',
        scopes: ['sql'],
      });
      const token = await provider.getToken();

      expect(token.tokenType).to.equal('CustomType');
      expect(token.refreshToken).to.equal('refresh');
      expect(token.scopes).to.deep.equal(['sql']);
    });
  });

  describe('getToken', () => {
    it('should always return the same token', async () => {
      const provider = new StaticTokenProvider('my-token');

      const token1 = await provider.getToken();
      const token2 = await provider.getToken();

      expect(token1).to.equal(token2);
      expect(token1.accessToken).to.equal('my-token');
    });
  });

  describe('getName', () => {
    it('should return provider name', () => {
      const provider = new StaticTokenProvider('my-token');
      expect(provider.getName()).to.equal('StaticTokenProvider');
    });
  });
});
