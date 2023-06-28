const { expect, AssertionError } = require('chai');
const { EventEmitter } = require('events');
const sinon = require('sinon');
const http = require('http');
const AuthorizationCode = require('../../../../../dist/connection/auth/DatabricksOAuth/AuthorizationCode').default;

class HttpServerMock extends EventEmitter {
  constructor() {
    super();
    this.requestHandler = () => {};
    this.listening = false;
    this.listenError = undefined; // error to emit on listen
    this.closeError = undefined; // error to emit on close
  }

  listen(port, host, callback) {
    if (this.listenError) {
      this.emit('error', this.listenError);
      this.listenError = undefined;
    } else if (port < 1000) {
      const error = new Error(`Address ${host}:${port} is already in use`);
      error.code = 'EADDRINUSE';
      this.emit('error', error);
    } else {
      this.listening = true;
      callback();
    }
  }

  close(callback) {
    this.requestHandler = () => {};
    this.listening = false;
    if (this.closeError) {
      this.emit('error', this.closeError);
      this.closeError = undefined;
    } else {
      callback();
    }
  }
}

class OAuthClientMock {
  constructor() {
    this.code = 'test_authorization_code';
    this.redirectUri = undefined;
  }

  authorizationUrl(params) {
    this.redirectUri = params.redirect_uri;
    return JSON.stringify({
      state: params.state,
      code: this.code,
    });
  }

  callbackParams(req) {
    return req.params;
  }
}

function prepareTestInstances(options) {
  const httpServer = new HttpServerMock();

  const oauthClient = new OAuthClientMock();

  const authCode = new AuthorizationCode({
    client: oauthClient,
    ...options,
  });

  sinon.stub(http, 'createServer').callsFake((requestHandler) => {
    httpServer.requestHandler = requestHandler;
    return httpServer;
  });

  sinon.stub(authCode, 'openUrl').callsFake((url) => {
    const params = JSON.parse(url);
    httpServer.requestHandler(
      { params },
      {
        writeHead: () => {},
        end: () => {},
      },
    );
  });

  function reloadUrl() {
    setTimeout(() => {
      const args = authCode.openUrl.firstCall.args;
      authCode.openUrl(...args);
    }, 10);
  }

  return { httpServer, oauthClient, authCode, reloadUrl };
}

describe('AuthorizationCode', () => {
  afterEach(() => {
    http.createServer.restore?.();
  });

  it('should fetch authorization code', async () => {
    const { authCode, oauthClient } = prepareTestInstances({
      ports: [80, 8000],
      logger: { log: () => {} },
    });

    const result = await authCode.fetch([]);
    expect(http.createServer.callCount).to.be.equal(2);
    expect(authCode.openUrl.callCount).to.be.equal(1);

    expect(result.code).to.be.equal(oauthClient.code);
    expect(result.verifier).to.not.be.empty;
    expect(result.redirectUri).to.be.equal(oauthClient.redirectUri);
  });

  it('should throw error if cannot start server on any port', async () => {
    const { authCode } = prepareTestInstances({
      ports: [80, 443],
    });

    try {
      await authCode.fetch([]);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(http.createServer.callCount).to.be.equal(2);
      expect(authCode.openUrl.callCount).to.be.equal(0);

      expect(error.message).to.contain('all ports are in use');
    }
  });

  it('should re-throw unhandled server start errors', async () => {
    const { authCode, httpServer } = prepareTestInstances({
      ports: [80],
    });

    const testError = new Error('Test');
    httpServer.listenError = testError;

    try {
      await authCode.fetch([]);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(http.createServer.callCount).to.be.equal(1);
      expect(authCode.openUrl.callCount).to.be.equal(0);

      expect(error).to.be.equal(testError);
    }
  });

  it('should re-throw unhandled server stop errors', async () => {
    const { authCode, httpServer } = prepareTestInstances({
      ports: [8000],
    });

    const testError = new Error('Test');
    httpServer.closeError = testError;

    try {
      await authCode.fetch([]);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(http.createServer.callCount).to.be.equal(1);
      expect(authCode.openUrl.callCount).to.be.equal(1);

      expect(error).to.be.equal(testError);
    }
  });

  it('should throw an error if no code was returned', async () => {
    const { authCode, oauthClient } = prepareTestInstances({
      ports: [8000],
    });

    sinon.stub(oauthClient, 'callbackParams').callsFake((req) => {
      // Omit authorization code from params
      const { code, ...otherParams } = req.params;
      return otherParams;
    });

    try {
      await authCode.fetch([]);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(http.createServer.callCount).to.be.equal(1);
      expect(authCode.openUrl.callCount).to.be.equal(1);

      expect(error.message).to.contain('No path parameters were returned to the callback');
    }
  });

  it('should use error details from callback params', async () => {
    const { authCode, oauthClient } = prepareTestInstances({
      ports: [8000],
    });

    sinon.stub(oauthClient, 'callbackParams').callsFake((req) => {
      // Omit authorization code from params
      const { code, ...otherParams } = req.params;
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
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(http.createServer.callCount).to.be.equal(1);
      expect(authCode.openUrl.callCount).to.be.equal(1);

      expect(error.message).to.contain('Test error');
    }
  });

  it('should serve 404 for unrecognized requests', async () => {
    const { authCode, oauthClient, reloadUrl } = prepareTestInstances({
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

    expect(http.createServer.callCount).to.be.equal(1);
    expect(authCode.openUrl.callCount).to.be.equal(2);
  });

  it('should not attempt to stop server if not running', async () => {
    const { authCode, oauthClient, httpServer } = prepareTestInstances({
      ports: [8000],
      logger: { log: () => {} },
    });

    const promise = authCode.fetch([]);

    httpServer.listening = false;
    httpServer.closeError = new Error('Test');

    const result = await promise;
    // We set up server to throw an error on close. If nothing happened - it means
    // that `authCode` never tried to stop it
    expect(result.code).to.be.equal(oauthClient.code);

    expect(http.createServer.callCount).to.be.equal(1);
    expect(authCode.openUrl.callCount).to.be.equal(1);
  });
});
