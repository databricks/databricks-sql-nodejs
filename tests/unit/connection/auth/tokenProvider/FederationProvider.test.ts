import { expect } from 'chai';
import sinon from 'sinon';
import type nodeFetch from 'node-fetch';
import FederationProvider, {
  setFederationFetchForTest,
} from '../../../../../lib/connection/auth/tokenProvider/FederationProvider';
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

  describe('exchange path', () => {
    // These tests exercise the federation HTTP exchange — the branch
    // taken when the source JWT's issuer doesn't match the Databricks
    // host. The branch contains the AbortController + node-fetch shim
    // typing fix; without coverage here a regression in those mechanics
    // would only surface in production.

    afterEach(() => {
      setFederationFetchForTest(); // restore real node-fetch
    });

    // Helper: build a fake node-fetch Response.
    function buildFakeResponse(opts: {
      ok: boolean;
      status?: number;
      statusText?: string;
      body?: unknown;
      text?: string;
    }): nodeFetch.Response {
      return {
        ok: opts.ok,
        status: opts.status ?? (opts.ok ? 200 : 500),
        statusText: opts.statusText ?? '',
        json: async () => opts.body,
        text: async () => opts.text ?? '',
      } as unknown as nodeFetch.Response;
    }

    it('should exchange foreign-issued JWT for a Databricks token', async () => {
      const foreignJwt = createJWT({ iss: 'https://idp.example.com' });
      const baseProvider = new MockTokenProvider(foreignJwt);
      const federationProvider = new FederationProvider(baseProvider, 'my-workspace.cloud.databricks.com');

      const fetchStub = sinon.stub<Parameters<typeof nodeFetch>, ReturnType<typeof nodeFetch>>().resolves(
        buildFakeResponse({
          ok: true,
          body: { access_token: 'exchanged-databricks-token', token_type: 'Bearer', expires_in: 3600 },
        }),
      );
      setFederationFetchForTest(fetchStub as unknown as typeof nodeFetch);

      const token = await federationProvider.getToken();

      expect(token.accessToken).to.equal('exchanged-databricks-token');
      expect(fetchStub.calledOnce).to.be.true;

      // The exchange must POST to the Databricks /oidc/v1/token endpoint.
      const [url, init] = fetchStub.firstCall.args;
      expect(String(url)).to.include('my-workspace.cloud.databricks.com');
      expect(String(url)).to.include('/oidc/v1/token');
      expect(init!.method).to.equal('POST');

      // Verify the signal propagates an AbortSignal — this is the cast
      // site that TS 5 type-strictness caught. Runtime-wise it must
      // still be a real AbortSignal-shaped object.
      const passedSignal = init!.signal as unknown as AbortSignal;
      expect(passedSignal, 'fetch init.signal must be set').to.exist;
      expect(typeof passedSignal.aborted, 'signal.aborted must be a boolean').to.equal('boolean');
      expect(passedSignal.aborted).to.be.false;
    });

    it('should propagate abort from the controller to the signal observed by fetch', async () => {
      const foreignJwt = createJWT({ iss: 'https://idp.example.com' });
      const baseProvider = new MockTokenProvider(foreignJwt);
      const federationProvider = new FederationProvider(baseProvider, 'my-workspace.cloud.databricks.com', {
        returnOriginalTokenOnFailure: false,
      });

      // Capture the signal so we can assert it implements the standard
      // AbortSignal contract. Resolve immediately with success to avoid
      // the 30s real-timeout path; the point is that the signal is wired
      // up, not to exercise the abort end-to-end.
      let capturedSignal: AbortSignal | undefined;
      const fetchStub = sinon
        .stub<Parameters<typeof nodeFetch>, ReturnType<typeof nodeFetch>>()
        .callsFake(async (_url, init) => {
          capturedSignal = init!.signal as unknown as AbortSignal;
          return buildFakeResponse({
            ok: true,
            body: { access_token: 'tok', token_type: 'Bearer', expires_in: 3600 },
          });
        });
      setFederationFetchForTest(fetchStub as unknown as typeof nodeFetch);

      await federationProvider.getToken();

      expect(capturedSignal, 'signal must reach fetch').to.exist;
      // The signal must implement the standard AbortSignal contract.
      expect(typeof capturedSignal!.aborted).to.equal('boolean');
      expect(typeof capturedSignal!.addEventListener).to.equal('function');
    });

    it('should fall back to original token when exchange fails (returnOriginalTokenOnFailure default)', async () => {
      const foreignJwt = createJWT({ iss: 'https://idp.example.com' });
      const baseProvider = new MockTokenProvider(foreignJwt);
      const federationProvider = new FederationProvider(baseProvider, 'my-workspace.cloud.databricks.com');

      const fetchStub = sinon
        .stub<Parameters<typeof nodeFetch>, ReturnType<typeof nodeFetch>>()
        .resolves(buildFakeResponse({ ok: false, status: 400, statusText: 'Bad Request', text: 'invalid_grant' }));
      setFederationFetchForTest(fetchStub as unknown as typeof nodeFetch);

      const token = await federationProvider.getToken();

      // Default behavior is to fall back to the original token on failure.
      // Retries kick in for 5xx; 400 is non-retryable so this should fail
      // fast on the first attempt.
      expect(token.accessToken).to.equal(foreignJwt);
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
