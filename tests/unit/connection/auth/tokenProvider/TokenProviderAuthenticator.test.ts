import { expect } from 'chai';
import sinon from 'sinon';
import TokenProviderAuthenticator from '../../../../../lib/connection/auth/tokenProvider/TokenProviderAuthenticator';
import ITokenProvider from '../../../../../lib/connection/auth/tokenProvider/ITokenProvider';
import Token from '../../../../../lib/connection/auth/tokenProvider/Token';
import ClientContextStub from '../../../.stubs/ClientContextStub';

class MockTokenProvider implements ITokenProvider {
  private token: Token;

  private name: string;

  constructor(accessToken: string, name: string = 'MockTokenProvider') {
    this.token = new Token(accessToken);
    this.name = name;
  }

  async getToken(): Promise<Token> {
    return this.token;
  }

  getName(): string {
    return this.name;
  }

  setToken(token: Token): void {
    this.token = token;
  }
}

describe('TokenProviderAuthenticator', () => {
  let context: ClientContextStub;

  beforeEach(() => {
    context = new ClientContextStub();
  });

  describe('authenticate', () => {
    it('should return headers with Authorization', async () => {
      const provider = new MockTokenProvider('my-access-token');
      const authenticator = new TokenProviderAuthenticator(provider, context);

      const headers = await authenticator.authenticate();

      expect(headers).to.deep.equal({
        Authorization: 'Bearer my-access-token',
      });
    });

    it('should include additional headers', async () => {
      const provider = new MockTokenProvider('my-access-token');
      const authenticator = new TokenProviderAuthenticator(provider, context, {
        'Content-Type': 'application/json',
        'X-Custom-Header': 'custom-value',
      });

      const headers = await authenticator.authenticate();

      expect(headers).to.deep.equal({
        'Content-Type': 'application/json',
        'X-Custom-Header': 'custom-value',
        Authorization: 'Bearer my-access-token',
      });
    });

    it('should use token type from token', async () => {
      const provider = new MockTokenProvider('my-access-token');
      provider.setToken(new Token('my-token', { tokenType: 'Basic' }));
      const authenticator = new TokenProviderAuthenticator(provider, context);

      const headers = await authenticator.authenticate();

      expect(headers).to.deep.equal({
        Authorization: 'Basic my-token',
      });
    });

    it('should call provider getToken', async () => {
      const provider = new MockTokenProvider('my-access-token');
      const getTokenSpy = sinon.spy(provider, 'getToken');
      const authenticator = new TokenProviderAuthenticator(provider, context);

      await authenticator.authenticate();

      expect(getTokenSpy.calledOnce).to.be.true;
    });

    it('should propagate errors from provider', async () => {
      const error = new Error('Failed to get token');
      const provider: ITokenProvider = {
        async getToken() {
          throw error;
        },
        getName() {
          return 'ErrorProvider';
        },
      };
      const authenticator = new TokenProviderAuthenticator(provider, context);

      try {
        await authenticator.authenticate();
        expect.fail('Should have thrown an error');
      } catch (e) {
        expect(e).to.equal(error);
      }
    });

    it('should retry once when token is expired and succeed with fresh token', async () => {
      const expiredDate = new Date(Date.now() - 60000);
      const freshDate = new Date(Date.now() + 3600000);
      const expiredToken = new Token('expired-token', { expiresAt: expiredDate });
      const freshToken = new Token('fresh-token', { expiresAt: freshDate });

      let callCount = 0;
      const provider: ITokenProvider = {
        async getToken() {
          callCount += 1;
          return callCount === 1 ? expiredToken : freshToken;
        },
        getName() {
          return 'TestProvider';
        },
      };

      const authenticator = new TokenProviderAuthenticator(provider, context);
      const headers = await authenticator.authenticate();

      expect(callCount).to.equal(2);
      expect(headers).to.deep.equal({
        Authorization: 'Bearer fresh-token',
      });
    });

    it('should throw error when token is still expired after retry', async () => {
      const provider = new MockTokenProvider('my-access-token', 'TestProvider');
      const expiredDate = new Date(Date.now() - 60000);
      provider.setToken(new Token('expired-token', { expiresAt: expiredDate }));
      const authenticator = new TokenProviderAuthenticator(provider, context);

      try {
        await authenticator.authenticate();
        expect.fail('Should have thrown an error');
      } catch (e) {
        expect(e).to.be.instanceOf(Error);
        expect((e as Error).message).to.include('expired');
        expect((e as Error).message).to.include('TestProvider');
      }
    });
  });
});
