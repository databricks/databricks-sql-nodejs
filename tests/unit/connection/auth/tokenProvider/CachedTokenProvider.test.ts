import { expect } from 'chai';
import sinon from 'sinon';
import CachedTokenProvider from '../../../../../lib/connection/auth/tokenProvider/CachedTokenProvider';
import ITokenProvider from '../../../../../lib/connection/auth/tokenProvider/ITokenProvider';
import Token from '../../../../../lib/connection/auth/tokenProvider/Token';

class MockTokenProvider implements ITokenProvider {
  public callCount = 0;
  public tokenToReturn: Token;

  constructor(expiresInMs: number = 3600000) {
    this.tokenToReturn = new Token(`token-${this.callCount}`, {
      expiresAt: new Date(Date.now() + expiresInMs),
    });
  }

  async getToken(): Promise<Token> {
    this.callCount += 1;
    this.tokenToReturn = new Token(`token-${this.callCount}`, {
      expiresAt: this.tokenToReturn.expiresAt,
    });
    return this.tokenToReturn;
  }

  getName(): string {
    return 'MockTokenProvider';
  }
}

describe('CachedTokenProvider', () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
    clock = sinon.useFakeTimers(Date.now());
  });

  afterEach(() => {
    clock.restore();
  });

  describe('getToken', () => {
    it('should cache tokens and return the same token on subsequent calls', async () => {
      const baseProvider = new MockTokenProvider(3600000); // 1 hour expiry
      const cachedProvider = new CachedTokenProvider(baseProvider);

      const token1 = await cachedProvider.getToken();
      const token2 = await cachedProvider.getToken();
      const token3 = await cachedProvider.getToken();

      expect(token1.accessToken).to.equal(token2.accessToken);
      expect(token2.accessToken).to.equal(token3.accessToken);
      expect(baseProvider.callCount).to.equal(1); // Only called once
    });

    it('should refresh token when it approaches expiry', async () => {
      const expiresInMs = 10 * 60 * 1000; // 10 minutes
      const baseProvider = new MockTokenProvider(expiresInMs);
      const cachedProvider = new CachedTokenProvider(baseProvider, {
        refreshThresholdMs: 5 * 60 * 1000, // 5 minutes threshold
      });

      const token1 = await cachedProvider.getToken();
      expect(baseProvider.callCount).to.equal(1);

      // Advance time to 6 minutes from now (within refresh threshold)
      clock.tick(6 * 60 * 1000);

      const token2 = await cachedProvider.getToken();
      expect(baseProvider.callCount).to.equal(2); // Should have refreshed
      expect(token1.accessToken).to.not.equal(token2.accessToken);
    });

    it('should not refresh token when not within threshold', async () => {
      const expiresInMs = 60 * 60 * 1000; // 1 hour
      const baseProvider = new MockTokenProvider(expiresInMs);
      const cachedProvider = new CachedTokenProvider(baseProvider, {
        refreshThresholdMs: 5 * 60 * 1000, // 5 minutes threshold
      });

      await cachedProvider.getToken();
      expect(baseProvider.callCount).to.equal(1);

      // Advance time by 10 minutes (still 50 minutes until expiry)
      clock.tick(10 * 60 * 1000);

      await cachedProvider.getToken();
      expect(baseProvider.callCount).to.equal(1); // Should still use cached
    });

    it('should handle tokens without expiration', async () => {
      const baseProvider: ITokenProvider = {
        async getToken() {
          return new Token('no-expiry-token');
        },
        getName() {
          return 'NoExpiryProvider';
        },
      };
      const getTokenSpy = sinon.spy(baseProvider, 'getToken');
      const cachedProvider = new CachedTokenProvider(baseProvider);

      await cachedProvider.getToken();
      await cachedProvider.getToken();
      await cachedProvider.getToken();

      expect(getTokenSpy.callCount).to.equal(1); // Should cache indefinitely
    });

    it('should handle concurrent getToken calls', async () => {
      let resolvePromise: (token: Token) => void;
      const slowProvider: ITokenProvider = {
        getToken() {
          return new Promise((resolve) => {
            resolvePromise = resolve;
          });
        },
        getName() {
          return 'SlowProvider';
        },
      };
      const getTokenSpy = sinon.spy(slowProvider, 'getToken');
      const cachedProvider = new CachedTokenProvider(slowProvider);

      // Start multiple concurrent requests
      const promise1 = cachedProvider.getToken();
      const promise2 = cachedProvider.getToken();
      const promise3 = cachedProvider.getToken();

      // Resolve the single underlying request
      resolvePromise!(new Token('concurrent-token'));

      const [token1, token2, token3] = await Promise.all([promise1, promise2, promise3]);

      expect(token1.accessToken).to.equal('concurrent-token');
      expect(token2.accessToken).to.equal('concurrent-token');
      expect(token3.accessToken).to.equal('concurrent-token');
      expect(getTokenSpy.callCount).to.equal(1); // Only one underlying call
    });
  });

  describe('clearCache', () => {
    it('should force a refresh on the next getToken call', async () => {
      const baseProvider = new MockTokenProvider(3600000);
      const cachedProvider = new CachedTokenProvider(baseProvider);

      const token1 = await cachedProvider.getToken();
      expect(baseProvider.callCount).to.equal(1);

      cachedProvider.clearCache();

      const token2 = await cachedProvider.getToken();
      expect(baseProvider.callCount).to.equal(2);
      expect(token1.accessToken).to.not.equal(token2.accessToken);
    });
  });

  describe('getName', () => {
    it('should return wrapped name', () => {
      const baseProvider = new MockTokenProvider();
      const cachedProvider = new CachedTokenProvider(baseProvider);

      expect(cachedProvider.getName()).to.equal('cached[MockTokenProvider]');
    });
  });
});
