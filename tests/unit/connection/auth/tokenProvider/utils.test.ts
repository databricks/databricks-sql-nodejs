import { expect } from 'chai';
import { decodeJWT, getJWTIssuer, isSameHost } from '../../../../../lib/connection/auth/tokenProvider/utils';

function createJWT(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64');
  const body = Buffer.from(JSON.stringify(payload)).toString('base64');
  return `${header}.${body}.signature`;
}

describe('Token Provider Utils', () => {
  describe('decodeJWT', () => {
    it('should decode valid JWT payload', () => {
      const payload = { iss: 'test-issuer', sub: 'user123', exp: 1234567890 };
      const jwt = createJWT(payload);

      const decoded = decodeJWT(jwt);

      expect(decoded).to.deep.equal(payload);
    });

    it('should return null for malformed JWT', () => {
      expect(decodeJWT('not-a-jwt')).to.be.null;
      expect(decodeJWT('')).to.be.null;
    });

    it('should return null for JWT with invalid base64 payload', () => {
      expect(decodeJWT('header.!!!invalid!!!.signature')).to.be.null;
    });

    it('should return null for JWT with non-JSON payload', () => {
      const header = Buffer.from('{}').toString('base64');
      const body = Buffer.from('not json').toString('base64');
      expect(decodeJWT(`${header}.${body}.sig`)).to.be.null;
    });
  });

  describe('getJWTIssuer', () => {
    it('should extract issuer from JWT', () => {
      const jwt = createJWT({ iss: 'https://my-issuer.com', sub: 'user' });
      expect(getJWTIssuer(jwt)).to.equal('https://my-issuer.com');
    });

    it('should return null if no issuer claim', () => {
      const jwt = createJWT({ sub: 'user' });
      expect(getJWTIssuer(jwt)).to.be.null;
    });

    it('should return null if issuer is not a string', () => {
      const jwt = createJWT({ iss: 123 });
      expect(getJWTIssuer(jwt)).to.be.null;
    });

    it('should return null for invalid JWT', () => {
      expect(getJWTIssuer('not-a-jwt')).to.be.null;
    });
  });

  describe('isSameHost', () => {
    it('should match identical hosts', () => {
      expect(isSameHost('example.com', 'example.com')).to.be.true;
    });

    it('should match hosts with different protocols', () => {
      expect(isSameHost('https://example.com', 'http://example.com')).to.be.true;
    });

    it('should match hosts ignoring ports', () => {
      expect(isSameHost('example.com', 'example.com:443')).to.be.true;
      expect(isSameHost('https://example.com:443', 'example.com')).to.be.true;
    });

    it('should match hosts case-insensitively', () => {
      expect(isSameHost('Example.COM', 'example.com')).to.be.true;
    });

    it('should not match different hosts', () => {
      expect(isSameHost('example.com', 'other.com')).to.be.false;
      expect(isSameHost('sub.example.com', 'example.com')).to.be.false;
    });

    it('should handle full URLs', () => {
      expect(isSameHost('https://my-workspace.cloud.databricks.com/path', 'my-workspace.cloud.databricks.com')).to.be
        .true;
    });

    it('should return false for invalid inputs', () => {
      expect(isSameHost('', '')).to.be.false;
    });
  });
});
