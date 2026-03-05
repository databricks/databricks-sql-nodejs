import { expect } from 'chai';
import Token from '../../../../../lib/connection/auth/tokenProvider/Token';

function createJWT(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64');
  const body = Buffer.from(JSON.stringify(payload)).toString('base64');
  return `${header}.${body}.signature`;
}

describe('Token', () => {
  describe('constructor', () => {
    it('should create token with access token only', () => {
      const token = new Token('test-access-token');
      expect(token.accessToken).to.equal('test-access-token');
      expect(token.tokenType).to.equal('Bearer');
      expect(token.expiresAt).to.be.undefined;
      expect(token.refreshToken).to.be.undefined;
      expect(token.scopes).to.be.undefined;
    });

    it('should create token with all options', () => {
      const expiresAt = new Date('2025-01-01T00:00:00Z');
      const token = new Token('test-access-token', {
        tokenType: 'CustomType',
        expiresAt,
        refreshToken: 'refresh-token',
        scopes: ['read', 'write'],
      });
      expect(token.accessToken).to.equal('test-access-token');
      expect(token.tokenType).to.equal('CustomType');
      expect(token.expiresAt).to.deep.equal(expiresAt);
      expect(token.refreshToken).to.equal('refresh-token');
      expect(token.scopes).to.deep.equal(['read', 'write']);
    });
  });

  describe('isExpired', () => {
    it('should return false when expiration is not set', () => {
      const token = new Token('test-token');
      expect(token.isExpired()).to.be.false;
    });

    it('should return true when token is expired', () => {
      const expiresAt = new Date(Date.now() - 60000); // 1 minute ago
      const token = new Token('test-token', { expiresAt });
      expect(token.isExpired()).to.be.true;
    });

    it('should return false when token is not expired', () => {
      const expiresAt = new Date(Date.now() + 300000); // 5 minutes from now
      const token = new Token('test-token', { expiresAt });
      expect(token.isExpired()).to.be.false;
    });

    it('should return true when within 30 second safety buffer', () => {
      const expiresAt = new Date(Date.now() + 20000); // 20 seconds from now
      const token = new Token('test-token', { expiresAt });
      expect(token.isExpired()).to.be.true;
    });
  });

  describe('setAuthHeader', () => {
    it('should set Authorization header with default Bearer type', () => {
      const token = new Token('my-token');
      const headers = token.setAuthHeader({});
      expect(headers).to.deep.equal({ Authorization: 'Bearer my-token' });
    });

    it('should set Authorization header with custom type', () => {
      const token = new Token('my-token', { tokenType: 'Basic' });
      const headers = token.setAuthHeader({});
      expect(headers).to.deep.equal({ Authorization: 'Basic my-token' });
    });

    it('should preserve existing headers', () => {
      const token = new Token('my-token');
      const headers = token.setAuthHeader({ 'Content-Type': 'application/json' });
      expect(headers).to.deep.equal({
        'Content-Type': 'application/json',
        Authorization: 'Bearer my-token',
      });
    });
  });

  describe('fromJWT', () => {
    it('should extract expiration from JWT payload', () => {
      const exp = Math.floor(Date.now() / 1000) + 3600; // 1 hour from now
      const jwt = createJWT({ exp, iss: 'test-issuer' });
      const token = Token.fromJWT(jwt);

      expect(token.accessToken).to.equal(jwt);
      expect(token.tokenType).to.equal('Bearer');
      expect(token.expiresAt).to.be.instanceOf(Date);
      expect(Math.floor(token.expiresAt!.getTime() / 1000)).to.equal(exp);
    });

    it('should handle JWT without expiration', () => {
      const jwt = createJWT({ iss: 'test-issuer' });
      const token = Token.fromJWT(jwt);

      expect(token.accessToken).to.equal(jwt);
      expect(token.expiresAt).to.be.undefined;
    });

    it('should handle malformed JWT gracefully', () => {
      const token = Token.fromJWT('not-a-valid-jwt');
      expect(token.accessToken).to.equal('not-a-valid-jwt');
      expect(token.expiresAt).to.be.undefined;
    });

    it('should handle JWT with invalid base64 payload', () => {
      const token = Token.fromJWT('header.!!!invalid-base64!!!.signature');
      expect(token.accessToken).to.equal('header.!!!invalid-base64!!!.signature');
      expect(token.expiresAt).to.be.undefined;
    });

    it('should apply custom options', () => {
      const jwt = createJWT({ exp: Math.floor(Date.now() / 1000) + 3600 });
      const token = Token.fromJWT(jwt, {
        tokenType: 'CustomType',
        refreshToken: 'refresh',
        scopes: ['sql'],
      });

      expect(token.tokenType).to.equal('CustomType');
      expect(token.refreshToken).to.equal('refresh');
      expect(token.scopes).to.deep.equal(['sql']);
    });
  });

  describe('toJSON', () => {
    it('should serialize token to JSON', () => {
      const expiresAt = new Date('2025-01-01T00:00:00Z');
      const token = new Token('test-token', {
        tokenType: 'Bearer',
        expiresAt,
        refreshToken: 'refresh',
        scopes: ['read'],
      });

      const json = token.toJSON();
      expect(json).to.deep.equal({
        accessToken: 'test-token',
        tokenType: 'Bearer',
        expiresAt: '2025-01-01T00:00:00.000Z',
        refreshToken: 'refresh',
        scopes: ['read'],
      });
    });

    it('should handle undefined optional fields', () => {
      const token = new Token('test-token');
      const json = token.toJSON();

      expect(json.accessToken).to.equal('test-token');
      expect(json.tokenType).to.equal('Bearer');
      expect(json.expiresAt).to.be.undefined;
      expect(json.refreshToken).to.be.undefined;
      expect(json.scopes).to.be.undefined;
    });
  });
});
