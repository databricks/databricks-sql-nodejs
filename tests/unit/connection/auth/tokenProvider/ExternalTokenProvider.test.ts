import { expect } from 'chai';
import sinon from 'sinon';
import ExternalTokenProvider from '../../../../../lib/connection/auth/tokenProvider/ExternalTokenProvider';

function createJWT(payload: Record<string, unknown>): string {
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64');
  const body = Buffer.from(JSON.stringify(payload)).toString('base64');
  return `${header}.${body}.signature`;
}

describe('ExternalTokenProvider', () => {
  describe('constructor', () => {
    it('should create provider with callback', async () => {
      const callback = sinon.stub().resolves('my-token');
      const provider = new ExternalTokenProvider(callback);

      await provider.getToken();

      expect(callback.calledOnce).to.be.true;
    });

    it('should use default name', () => {
      const provider = new ExternalTokenProvider(async () => 'token');
      expect(provider.getName()).to.equal('ExternalTokenProvider');
    });

    it('should use custom name', () => {
      const provider = new ExternalTokenProvider(async () => 'token', { name: 'MyCustomProvider' });
      expect(provider.getName()).to.equal('MyCustomProvider');
    });
  });

  describe('getToken', () => {
    it('should call callback and return token', async () => {
      const callback = sinon.stub().resolves('my-access-token');
      const provider = new ExternalTokenProvider(callback);

      const token = await provider.getToken();

      expect(token.accessToken).to.equal('my-access-token');
      expect(token.tokenType).to.equal('Bearer');
    });

    it('should extract expiration from JWT by default', async () => {
      const exp = Math.floor(Date.now() / 1000) + 3600;
      const jwt = createJWT({ exp, iss: 'test-issuer' });
      const callback = sinon.stub().resolves(jwt);
      const provider = new ExternalTokenProvider(callback);

      const token = await provider.getToken();

      expect(token.accessToken).to.equal(jwt);
      expect(token.expiresAt).to.be.instanceOf(Date);
      expect(Math.floor(token.expiresAt!.getTime() / 1000)).to.equal(exp);
    });

    it('should not parse JWT when parseJWT is false', async () => {
      const jwt = createJWT({ exp: Math.floor(Date.now() / 1000) + 3600 });
      const callback = sinon.stub().resolves(jwt);
      const provider = new ExternalTokenProvider(callback, { parseJWT: false });

      const token = await provider.getToken();

      expect(token.accessToken).to.equal(jwt);
      expect(token.expiresAt).to.be.undefined;
    });

    it('should call callback on each getToken call', async () => {
      let callCount = 0;
      const callback = async () => {
        callCount += 1;
        return `token-${callCount}`;
      };
      const provider = new ExternalTokenProvider(callback);

      const token1 = await provider.getToken();
      const token2 = await provider.getToken();

      expect(token1.accessToken).to.equal('token-1');
      expect(token2.accessToken).to.equal('token-2');
    });

    it('should propagate errors from callback', async () => {
      const error = new Error('Failed to get token');
      const callback = sinon.stub().rejects(error);
      const provider = new ExternalTokenProvider(callback);

      try {
        await provider.getToken();
        expect.fail('Should have thrown an error');
      } catch (e) {
        expect(e).to.equal(error);
      }
    });
  });

  describe('getName', () => {
    it('should return default name', () => {
      const provider = new ExternalTokenProvider(async () => 'token');
      expect(provider.getName()).to.equal('ExternalTokenProvider');
    });

    it('should return custom name', () => {
      const provider = new ExternalTokenProvider(async () => 'token', { name: 'VaultTokenProvider' });
      expect(provider.getName()).to.equal('VaultTokenProvider');
    });
  });
});
