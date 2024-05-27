import { expect, AssertionError } from 'chai';
import sinon from 'sinon';
import net from 'net';
import { IncomingMessage, ServerResponse } from 'http';
import { BaseClient, AuthorizationParameters, CallbackParamsType, custom } from 'openid-client';
import AuthorizationCode, {
  AuthorizationCodeOptions,
} from '../../../../../lib/connection/auth/DatabricksOAuth/AuthorizationCode';

import ClientContextStub from '../../../.stubs/ClientContextStub';
import { OAuthCallbackServerStub } from '../../../.stubs/OAuth';

class IncomingMessageStub extends IncomingMessage {
  public params: CallbackParamsType;

  constructor(params: CallbackParamsType = {}) {
    super(new net.Socket());
    this.params = params;
  }
}

class ServerResponseStub<Request extends IncomingMessage = IncomingMessage> extends ServerResponse<Request> {}

// `BaseClient` is not actually exported from `openid-client`, just declared. So instead of extending it,
// we use it as an interface and declare all the dummy properties we're not going to use anyway
class OpenIDClientStub implements BaseClient {
  public code = 'test_authorization_code';

  public redirectUri?: string = undefined;

  authorizationUrl(params: AuthorizationParameters) {
    this.redirectUri = params.redirect_uri;
    return JSON.stringify({
      state: params.state,
      code: this.code,
    });
  }

  callbackParams(req: IncomingMessage) {
    return req instanceof IncomingMessageStub ? req.params : {};
  }

  // All the unused properties from `BaseClient`
  public metadata: any;

  public issuer: any;

  public endSessionUrl: any;

  public callback: any;

  public oauthCallback: any;

  public refresh: any;

  public userinfo: any;

  public requestResource: any;

  public grant: any;

  public introspect: any;

  public revoke: any;

  public requestObject: any;

  public deviceAuthorization: any;

  public pushedAuthorizationRequest: any;

  public [custom.http_options]: any;

  public [custom.clock_tolerance]: any;

  [key: string]: unknown;
}

function prepareTestInstances(options: Partial<AuthorizationCodeOptions>) {
  const oauthClient = new OpenIDClientStub();

  const httpServer = new OAuthCallbackServerStub();

  const openAuthUrl = sinon.stub<[string], Promise<void>>();

  const authCode = new AuthorizationCode({
    client: oauthClient,
    context: new ClientContextStub(),
    ports: [],
    ...options,
    openAuthUrl,
  });

  const authCodeSpy = sinon.spy(authCode);

  const createHttpServer = sinon.spy((requestHandler: (req: IncomingMessage, res: ServerResponse) => void) => {
    httpServer.requestHandler = requestHandler;
    return httpServer;
  });

  authCode['createHttpServer'] = createHttpServer;

  openAuthUrl.callsFake(async (authUrl) => {
    const params = JSON.parse(authUrl);
    const req = new IncomingMessageStub(params);
    const resp = new ServerResponseStub(req);
    httpServer.requestHandler(req, resp);
  });

  function reloadUrl() {
    setTimeout(() => {
      const args = openAuthUrl.firstCall.args;
      openAuthUrl(...args);
    }, 10);
  }

  return { oauthClient, authCode: authCodeSpy, httpServer, openAuthUrl, reloadUrl, createHttpServer };
}

describe('AuthorizationCode', () => {
  it('should fetch authorization code', async () => {
    const { authCode, oauthClient, openAuthUrl, createHttpServer } = prepareTestInstances({
      ports: [80, 8000],
    });

    const result = await authCode.fetch([]);
    expect(createHttpServer.callCount).to.be.equal(2);
    expect(openAuthUrl.callCount).to.be.equal(1);

    expect(result.code).to.be.equal(oauthClient.code);
    expect(result.verifier).to.not.be.empty;
    expect(result.redirectUri).to.be.equal(oauthClient.redirectUri);
  });

  it('should throw error if cannot start server on any port', async () => {
    const { authCode, openAuthUrl, createHttpServer } = prepareTestInstances({
      ports: [80, 443],
    });

    try {
      await authCode.fetch([]);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(createHttpServer.callCount).to.be.equal(2);
      expect(openAuthUrl.callCount).to.be.equal(0);

      expect(error.message).to.contain('all ports are in use');
    }
  });

  it('should re-throw unhandled server start errors', async () => {
    const { authCode, openAuthUrl, httpServer, createHttpServer } = prepareTestInstances({
      ports: [80],
    });

    const testError = new Error('Test');
    httpServer.listenError = testError;

    try {
      await authCode.fetch([]);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(createHttpServer.callCount).to.be.equal(1);
      expect(openAuthUrl.callCount).to.be.equal(0);

      expect(error).to.be.equal(testError);
    }
  });

  it('should re-throw unhandled server stop errors', async () => {
    const { authCode, openAuthUrl, httpServer, createHttpServer } = prepareTestInstances({
      ports: [8000],
    });

    const testError = new Error('Test');
    httpServer.closeError = testError;

    try {
      await authCode.fetch([]);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(createHttpServer.callCount).to.be.equal(1);
      expect(openAuthUrl.callCount).to.be.equal(1);

      expect(error).to.be.equal(testError);
    }
  });

  it('should throw an error if no code was returned', async () => {
    const { authCode, oauthClient, openAuthUrl, createHttpServer } = prepareTestInstances({
      ports: [8000],
    });

    sinon.stub(oauthClient, 'callbackParams').callsFake((req) => {
      // Omit authorization code from params
      const { code, ...otherParams } = req instanceof IncomingMessageStub ? req.params : { code: undefined };
      return otherParams;
    });

    try {
      await authCode.fetch([]);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(createHttpServer.callCount).to.be.equal(1);
      expect(openAuthUrl.callCount).to.be.equal(1);

      expect(error.message).to.contain('No path parameters were returned to the callback');
    }
  });

  it('should use error details from callback params', async () => {
    const { authCode, oauthClient, openAuthUrl, createHttpServer } = prepareTestInstances({
      ports: [8000],
    });

    sinon.stub(oauthClient, 'callbackParams').callsFake((req) => {
      // Omit authorization code from params
      const { code, ...otherParams } = req instanceof IncomingMessageStub ? req.params : { code: undefined };
      return {
        ...otherParams,
        error: 'test_error',
        error_description: 'Test error',
      };
    });

    try {
      await authCode.fetch([]);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(createHttpServer.callCount).to.be.equal(1);
      expect(openAuthUrl.callCount).to.be.equal(1);

      expect(error.message).to.contain('Test error');
    }
  });

  it('should serve 404 for unrecognized requests', async () => {
    const { authCode, oauthClient, reloadUrl, openAuthUrl, createHttpServer } = prepareTestInstances({
      ports: [8000],
    });

    sinon
      .stub(oauthClient, 'callbackParams')
      .onFirstCall()
      .callsFake(() => {
        // Repeat the same request after currently processed one.
        // We won't modify response on subsequent requests so OAuth routine can complete
        reloadUrl();
        // Return no params so request cannot be recognized
        return {};
      })
      .callThrough();

    await authCode.fetch([]);

    expect(createHttpServer.callCount).to.be.equal(1);
    expect(openAuthUrl.callCount).to.be.equal(2);
  });

  it('should not attempt to stop server if not running', async () => {
    const { authCode, oauthClient, openAuthUrl, httpServer, createHttpServer } = prepareTestInstances({
      ports: [8000],
    });

    const promise = authCode.fetch([]);

    httpServer.listening = false;
    httpServer.closeError = new Error('Test');

    const result = await promise;
    // We set up server to throw an error on close. If nothing happened - it means
    // that `authCode` never tried to stop it
    expect(result.code).to.be.equal(oauthClient.code);

    expect(createHttpServer.callCount).to.be.equal(1);
    expect(openAuthUrl.callCount).to.be.equal(1);
  });
});
