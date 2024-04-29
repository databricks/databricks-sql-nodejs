const { expect, AssertionError } = require('chai');
const sinon = require('sinon');
const openidClientLib = require('openid-client');
const { DBSQLLogger, LogLevel } = require('../../../../../lib');
const {
  DatabricksOAuthManager,
  AzureOAuthManager,
  OAuthFlow,
} = require('../../../../../lib/connection/auth/DatabricksOAuth/OAuthManager');
const OAuthToken = require('../../../../../lib/connection/auth/DatabricksOAuth/OAuthToken').default;
const { OAuthScope, scopeDelimiter } = require('../../../../../lib/connection/auth/DatabricksOAuth/OAuthScope');
const AuthorizationCodeModule = require('../../../../../lib/connection/auth/DatabricksOAuth/AuthorizationCode');

const { createValidAccessToken, createExpiredAccessToken } = require('./utils');

const logger = new DBSQLLogger({ level: LogLevel.error });

class AuthorizationCodeMock {
  constructor() {
    this.fetchResult = undefined;
    this.expectedScope = undefined;
  }

  async fetch(scopes) {
    if (this.expectedScope) {
      expect(scopes.join(scopeDelimiter)).to.be.equal(this.expectedScope);
    }
    return this.fetchResult;
  }
}

AuthorizationCodeMock.validCode = {
  code: 'auth_code',
  verifier: 'verifier_string',
  redirectUri: 'http://localhost:8000',
};

class OAuthClientMock {
  constructor() {
    this.clientOptions = {};
    this.expectedClientId = undefined;
    this.expectedClientSecret = undefined;
    this.expectedScope = undefined;

    this.grantError = undefined;
    this.refreshError = undefined;

    this.accessToken = undefined;
    this.refreshToken = undefined;
    this.recreateTokens();
  }

  recreateTokens() {
    const suffix = Math.random().toString(36).substring(2);
    this.accessToken = `${createValidAccessToken()}.${suffix}`;
    this.refreshToken = `refresh.${suffix}`;
  }

  async grantU2M(params) {
    if (this.grantError) {
      const error = this.grantError;
      this.grantError = undefined;
      throw error;
    }

    expect(params.grant_type).to.be.equal('authorization_code');
    expect(params.code).to.be.equal(AuthorizationCodeMock.validCode.code);
    expect(params.code_verifier).to.be.equal(AuthorizationCodeMock.validCode.verifier);
    expect(params.redirect_uri).to.be.equal(AuthorizationCodeMock.validCode.redirectUri);
    if (this.expectedScope) {
      expect(params.scope).to.be.equal(this.expectedScope);
    }

    return {
      access_token: this.accessToken,
      refresh_token: this.refreshToken,
    };
  }

  async grantM2M(params) {
    if (this.grantError) {
      const error = this.grantError;
      this.grantError = undefined;
      throw error;
    }

    expect(params.grant_type).to.be.equal('client_credentials');
    if (this.expectedScope) {
      expect(params.scope).to.be.equal(this.expectedScope);
    }

    return {
      access_token: this.accessToken,
      refresh_token: this.refreshToken,
    };
  }

  async grant(params) {
    switch (this.clientOptions.token_endpoint_auth_method) {
      case 'client_secret_basic':
        return this.grantM2M(params);
      case 'none':
        return this.grantU2M(params);
    }
    throw new Error(`OAuthClientMock: unrecognized auth method: ${this.clientOptions.token_endpoint_auth_method}`);
  }

  async refresh(refreshToken) {
    if (this.refreshError) {
      const error = this.refreshError;
      this.refreshError = undefined;
      throw error;
    }

    expect(refreshToken).to.be.equal(this.refreshToken);

    this.recreateTokens();
    return {
      access_token: this.accessToken,
      refresh_token: this.refreshToken,
    };
  }
}

[DatabricksOAuthManager, AzureOAuthManager].forEach((OAuthManagerClass) => {
  function prepareTestInstances(options) {
    const oauthClient = new OAuthClientMock();
    sinon.stub(oauthClient, 'grant').callThrough();
    sinon.stub(oauthClient, 'refresh').callThrough();

    oauthClient.expectedClientId = options?.clientId;
    oauthClient.expectedClientSecret = options?.clientSecret;

    const issuer = {
      Client: function (clientOptions) {
        oauthClient.clientOptions = clientOptions;
        return oauthClient;
      },
    };

    sinon.stub(openidClientLib, 'Issuer').returns(issuer);
    openidClientLib.Issuer.discover = () => Promise.resolve(issuer);

    const oauthManager = new OAuthManagerClass({
      host: 'https://example.com',
      ...options,
      context: {
        getLogger: () => logger,
        getConnectionProvider: async () => ({
          getAgent: async () => undefined,
        }),
      },
    });

    const authCode = new AuthorizationCodeMock();
    authCode.fetchResult = { ...AuthorizationCodeMock.validCode };

    sinon.stub(AuthorizationCodeModule, 'default').returns(authCode);

    return { oauthClient, oauthManager, authCode };
  }

  describe(OAuthManagerClass.name, () => {
    afterEach(() => {
      AuthorizationCodeModule.default.restore?.();
      openidClientLib.Issuer.restore?.();
    });

    describe('U2M flow', () => {
      function getExpectedScope(scopes) {
        switch (OAuthManagerClass) {
          case DatabricksOAuthManager:
            return [...scopes].join(scopeDelimiter);
          case AzureOAuthManager:
            const tenantId = AzureOAuthManager.datatricksAzureApp;
            return [`${tenantId}/user_impersonation`, ...scopes].join(scopeDelimiter);
        }
        return undefined;
      }

      it('should get access token', async () => {
        const { oauthManager, oauthClient, authCode } = prepareTestInstances({ flow: OAuthFlow.U2M });
        const requestedScopes = [OAuthScope.offlineAccess];
        authCode.expectedScope = getExpectedScope(requestedScopes);

        const token = await oauthManager.getToken(requestedScopes);
        expect(oauthClient.grant.called).to.be.true;
        expect(token).to.be.instanceOf(OAuthToken);
        expect(token.accessToken).to.be.equal(oauthClient.accessToken);
        expect(token.refreshToken).to.be.equal(oauthClient.refreshToken);
      });

      it('should throw an error if cannot get access token', async () => {
        const { oauthManager, oauthClient, authCode } = prepareTestInstances({ flow: OAuthFlow.U2M });
        const requestedScopes = [OAuthScope.offlineAccess];
        authCode.expectedScope = getExpectedScope(requestedScopes);

        // Make it return empty tokens
        oauthClient.accessToken = undefined;
        oauthClient.refreshToken = undefined;

        try {
          await oauthManager.getToken(requestedScopes);
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(oauthClient.grant.called).to.be.true;
          expect(error.message).to.contain('Failed to fetch access token');
        }
      });

      it('should re-throw unhandled errors when getting access token', async () => {
        const { oauthManager, oauthClient, authCode } = prepareTestInstances({ flow: OAuthFlow.U2M });
        const requestedScopes = [];
        authCode.expectedScope = getExpectedScope(requestedScopes);

        const testError = new Error('Test');
        oauthClient.grantError = testError;

        try {
          await oauthManager.getToken(requestedScopes);
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(oauthClient.grant.called).to.be.true;
          expect(error).to.be.equal(testError);
        }
      });

      it('should not refresh valid token', async () => {
        const { oauthManager, oauthClient, authCode } = prepareTestInstances({ flow: OAuthFlow.U2M });
        authCode.expectedScope = getExpectedScope([]);

        const token = new OAuthToken(createValidAccessToken(), oauthClient.refreshToken);
        expect(token.hasExpired).to.be.false;

        const newToken = await oauthManager.refreshAccessToken(token);
        expect(oauthClient.refresh.called).to.be.false;
        expect(newToken).to.be.instanceOf(OAuthToken);
        expect(newToken.accessToken).to.be.equal(token.accessToken);
        expect(newToken.hasExpired).to.be.false;
      });

      it('should throw an error if no refresh token is available', async () => {
        const { oauthManager, oauthClient, authCode } = prepareTestInstances({ flow: OAuthFlow.U2M });
        authCode.expectedScope = getExpectedScope([]);

        try {
          const token = new OAuthToken(createExpiredAccessToken());
          expect(token.hasExpired).to.be.true;

          await oauthManager.refreshAccessToken(token);
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(oauthClient.refresh.called).to.be.false;
          expect(error.message).to.contain('token expired');
        }
      });

      it('should throw an error for invalid token', async () => {
        const { oauthManager, oauthClient, authCode } = prepareTestInstances({ flow: OAuthFlow.U2M });
        authCode.expectedScope = getExpectedScope([]);

        try {
          const token = new OAuthToken('invalid_access_token', 'invalid_refresh_token');
          await oauthManager.refreshAccessToken(token);
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(oauthClient.refresh.called).to.be.false;
          // Random malformed string passed as access token will cause JSON parse errors
          expect(error).to.be.instanceof(TypeError);
        }
      });

      it('should refresh expired token', async () => {
        const { oauthManager, oauthClient, authCode } = prepareTestInstances({ flow: OAuthFlow.U2M });
        const requestedScopes = [OAuthScope.offlineAccess];
        authCode.expectedScope = getExpectedScope(requestedScopes);

        oauthClient.accessToken = createExpiredAccessToken();
        const token = await oauthManager.getToken(requestedScopes);
        expect(token.hasExpired).to.be.true;

        const newToken = await oauthManager.refreshAccessToken(token);
        expect(oauthClient.refresh.called).to.be.true;
        expect(newToken).to.be.instanceOf(OAuthToken);
        expect(newToken.accessToken).to.be.not.equal(token.accessToken);
        expect(newToken.hasExpired).to.be.false;
      });

      it('should throw an error if cannot refresh token', async () => {
        const { oauthManager, oauthClient, authCode } = prepareTestInstances({ flow: OAuthFlow.U2M });
        authCode.expectedScope = getExpectedScope([]);

        oauthClient.refresh.restore();
        sinon.stub(oauthClient, 'refresh').returns({});

        try {
          const token = new OAuthToken(createExpiredAccessToken(), oauthClient.refreshToken);
          expect(token.hasExpired).to.be.true;

          await oauthManager.refreshAccessToken(token);
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(oauthClient.refresh.called).to.be.true;
          expect(error.message).to.contain('invalid response');
        }
      });
    });

    describe('M2M flow', () => {
      function getExpectedScope(scopes) {
        switch (OAuthManagerClass) {
          case DatabricksOAuthManager:
            return [OAuthScope.allAPIs].join(scopeDelimiter);
          case AzureOAuthManager:
            const tenantId = AzureOAuthManager.datatricksAzureApp;
            return [`${tenantId}/.default`, ...scopes].join(scopeDelimiter);
        }
        return undefined;
      }

      it('should get access token', async () => {
        const { oauthManager, oauthClient } = prepareTestInstances({
          flow: OAuthFlow.M2M,
          clientId: 'test_client_id',
          clientSecret: 'test_client_secret',
        });
        const requestedScopes = [OAuthScope.offlineAccess];
        oauthClient.expectedScope = getExpectedScope(requestedScopes);

        const token = await oauthManager.getToken(requestedScopes);
        expect(oauthClient.grant.called).to.be.true;
        expect(token).to.be.instanceOf(OAuthToken);
        expect(token.accessToken).to.be.equal(oauthClient.accessToken);
        expect(token.refreshToken).to.be.undefined;
      });

      it('should throw an error if cannot get access token', async () => {
        const { oauthManager, oauthClient } = prepareTestInstances({
          flow: OAuthFlow.M2M,
          clientId: 'test_client_id',
          clientSecret: 'test_client_secret',
        });
        const requestedScopes = [OAuthScope.offlineAccess];
        oauthClient.expectedScope = getExpectedScope(requestedScopes);

        // Make it return empty tokens
        oauthClient.accessToken = undefined;
        oauthClient.refreshToken = undefined;

        try {
          await oauthManager.getToken(requestedScopes);
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(oauthClient.grant.called).to.be.true;
          expect(error.message).to.contain('Failed to fetch access token');
        }
      });

      it('should re-throw unhandled errors when getting access token', async () => {
        const { oauthManager, oauthClient } = prepareTestInstances({
          flow: OAuthFlow.M2M,
          clientId: 'test_client_id',
          clientSecret: 'test_client_secret',
        });
        const requestedScopes = [];
        oauthClient.expectedScope = getExpectedScope(requestedScopes);

        const testError = new Error('Test');
        oauthClient.grantError = testError;

        try {
          await oauthManager.getToken(requestedScopes);
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(oauthClient.grant.called).to.be.true;
          expect(error).to.be.equal(testError);
        }
      });

      it('should not refresh valid token', async () => {
        const { oauthManager, oauthClient } = prepareTestInstances({
          flow: OAuthFlow.M2M,
          clientId: 'test_client_id',
          clientSecret: 'test_client_secret',
        });
        oauthClient.expectedScope = getExpectedScope([]);

        const token = new OAuthToken(createValidAccessToken());
        expect(token.hasExpired).to.be.false;

        const newToken = await oauthManager.refreshAccessToken(token);
        expect(oauthClient.grant.called).to.be.false;
        expect(oauthClient.refresh.called).to.be.false;
        expect(newToken).to.be.instanceOf(OAuthToken);
        expect(newToken.accessToken).to.be.equal(token.accessToken);
        expect(newToken.hasExpired).to.be.false;
      });

      it('should refresh expired token', async () => {
        const { oauthManager, oauthClient } = prepareTestInstances({
          flow: OAuthFlow.M2M,
          clientId: 'test_client_id',
          clientSecret: 'test_client_secret',
        });
        const requestedScopes = [OAuthScope.offlineAccess];
        oauthClient.expectedScope = getExpectedScope(requestedScopes);

        oauthClient.accessToken = createExpiredAccessToken();
        const token = await oauthManager.getToken(requestedScopes);
        expect(token.hasExpired).to.be.true;

        oauthClient.accessToken = createValidAccessToken();
        const newToken = await oauthManager.refreshAccessToken(token);
        expect(oauthClient.grant.called).to.be.true;
        expect(oauthClient.refresh.called).to.be.false;
        expect(newToken).to.be.instanceOf(OAuthToken);
        expect(newToken.accessToken).to.be.not.equal(token.accessToken);
        expect(newToken.hasExpired).to.be.false;
      });

      it('should throw an error if cannot refresh token', async () => {
        const { oauthManager, oauthClient } = prepareTestInstances({
          flow: OAuthFlow.M2M,
          clientId: 'test_client_id',
          clientSecret: 'test_client_secret',
        });
        const requestedScopes = [OAuthScope.offlineAccess];
        oauthClient.expectedScope = getExpectedScope(requestedScopes);

        oauthClient.accessToken = createExpiredAccessToken();
        const token = await oauthManager.getToken(requestedScopes);
        expect(token.hasExpired).to.be.true;

        oauthClient.grant.restore();
        sinon.stub(oauthClient, 'grant').returns({});

        try {
          await oauthManager.refreshAccessToken(token);
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(oauthClient.grant.called).to.be.true;
          expect(oauthClient.refresh.called).to.be.false;
          expect(error.message).to.contain('Failed to fetch access token');
        }
      });
    });
  });
});
