import { AssertionError, expect } from 'chai';
import sinon, { SinonStub } from 'sinon';
import { Issuer, BaseClient, TokenSet, GrantBody, IssuerMetadata, ClientMetadata, custom } from 'openid-client';
// Import the whole module once more - to stub some of its exports
import openidClientLib from 'openid-client';
// Import the whole module to stub its default export
import * as AuthorizationCodeModule from '../../../../../lib/connection/auth/DatabricksOAuth/AuthorizationCode';

import {
  AzureOAuthManager,
  DatabricksOAuthManager,
  OAuthFlow,
  OAuthManagerOptions,
} from '../../../../../lib/connection/auth/DatabricksOAuth/OAuthManager';
import OAuthToken from '../../../../../lib/connection/auth/DatabricksOAuth/OAuthToken';
import { OAuthScope, scopeDelimiter } from '../../../../../lib/connection/auth/DatabricksOAuth/OAuthScope';
import { AuthorizationCodeStub, createExpiredAccessToken, createValidAccessToken } from '../../../.stubs/OAuth';
import ClientContextStub from '../../../.stubs/ClientContextStub';

// `BaseClient` is not actually exported from `openid-client`, just declared. So instead of extending it,
// we use it as an interface and declare all the dummy properties we're not going to use anyway
class OpenIDClientStub implements BaseClient {
  public clientOptions: ClientMetadata = { client_id: 'test_client' };

  public expectedClientId?: string = undefined;

  public expectedClientSecret?: string = undefined;

  public expectedScope?: string = undefined;

  public grantError?: Error = undefined;

  public refreshError?: Error = undefined;

  public accessToken?: string = undefined;

  public refreshToken?: string = undefined;

  constructor() {
    this.recreateTokens();
  }

  recreateTokens() {
    const suffix = Math.random().toString(36).substring(2);
    this.accessToken = `${createValidAccessToken()}.${suffix}`;
    this.refreshToken = `refresh.${suffix}`;
  }

  async grantU2M(params: GrantBody) {
    if (this.grantError) {
      const error = this.grantError;
      this.grantError = undefined;
      throw error;
    }

    expect(params.grant_type).to.be.equal('authorization_code');
    expect(params.code).to.be.equal(AuthorizationCodeStub.validCode.code);
    expect(params.code_verifier).to.be.equal(AuthorizationCodeStub.validCode.verifier);
    expect(params.redirect_uri).to.be.equal(AuthorizationCodeStub.validCode.redirectUri);
    if (this.expectedScope) {
      expect(params.scope).to.be.equal(this.expectedScope);
    }

    return new TokenSet({
      access_token: this.accessToken,
      refresh_token: this.refreshToken,
    });
  }

  async grantM2M(params: GrantBody) {
    if (this.grantError) {
      const error = this.grantError;
      this.grantError = undefined;
      throw error;
    }

    expect(params.grant_type).to.be.equal('client_credentials');
    if (this.expectedScope) {
      expect(params.scope).to.be.equal(this.expectedScope);
    }

    return new TokenSet({
      access_token: this.accessToken,
      refresh_token: this.refreshToken,
    });
  }

  async grant(params: GrantBody) {
    switch (this.clientOptions.token_endpoint_auth_method) {
      case 'client_secret_basic':
        return this.grantM2M(params);
      case 'none':
        return this.grantU2M(params);
    }
    throw new Error(`OAuthClientStub: unrecognized auth method: ${this.clientOptions.token_endpoint_auth_method}`);
  }

  async refresh(refreshToken: string) {
    if (this.refreshError) {
      const error = this.refreshError;
      this.refreshError = undefined;
      throw error;
    }

    expect(refreshToken).to.be.equal(this.refreshToken);

    this.recreateTokens();
    return new TokenSet({
      access_token: this.accessToken,
      refresh_token: this.refreshToken,
    });
  }

  // All the unused properties from `BaseClient`
  public metadata: any;

  public issuer: any;

  public authorizationUrl: any;

  public callbackParams: any;

  public endSessionUrl: any;

  public callback: any;

  public oauthCallback: any;

  public userinfo: any;

  public requestResource: any;

  public introspect: any;

  public revoke: any;

  public requestObject: any;

  public deviceAuthorization: any;

  public pushedAuthorizationRequest: any;

  public [custom.http_options]: any;

  public [custom.clock_tolerance]: any;

  [key: string]: unknown;

  public static [custom.http_options]: any;

  public static [custom.clock_tolerance]: any;
}

[DatabricksOAuthManager, AzureOAuthManager].forEach((OAuthManagerClass) => {
  afterEach(() => {
    (openidClientLib.Issuer as unknown as SinonStub).restore?.();
    (AuthorizationCodeModule.default as unknown as SinonStub).restore?.();
  });

  function prepareTestInstances(options: Partial<OAuthManagerOptions>) {
    const oauthClient = sinon.spy(new OpenIDClientStub());

    oauthClient.expectedClientId = options?.clientId;
    oauthClient.expectedClientSecret = options?.clientSecret;

    const issuer: Issuer<OpenIDClientStub> = {
      Client: class extends OpenIDClientStub {
        constructor(clientOptions: ClientMetadata) {
          super();
          oauthClient.clientOptions = clientOptions;
          return oauthClient;
        }
      },

      FAPI1Client: OpenIDClientStub,

      metadata: { issuer: 'test' },
      [custom.http_options]: () => ({}),

      discover: async () => issuer,
    };

    sinon.stub(openidClientLib, 'Issuer').returns(issuer);
    // Now `openidClientLib.Issuer` is a Sinon wrapper function which doesn't have a `discover` method.
    // It is safe to just assign it (`sinon.stub` won't work anyway)
    openidClientLib.Issuer.discover = async () => issuer;

    const oauthManager = new OAuthManagerClass({
      host: 'https://example.com',
      flow: OAuthFlow.M2M,
      ...options,
      context: new ClientContextStub(),
    });

    const authCode = new AuthorizationCodeStub();
    authCode.fetchResult = { ...AuthorizationCodeStub.validCode };

    sinon.stub(AuthorizationCodeModule, 'default').returns(authCode);

    return { oauthClient, oauthManager, authCode };
  }

  describe(OAuthManagerClass.name, () => {
    describe('U2M flow', () => {
      function getExpectedScope(scopes: Array<string>) {
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
          if (error instanceof AssertionError || !(error instanceof Error)) {
            throw error;
          }
          expect(oauthClient.grant.called).to.be.true;
          expect(error.message).to.contain('Failed to fetch access token');
        }
      });

      it('should re-throw unhandled errors when getting access token', async () => {
        const { oauthManager, oauthClient, authCode } = prepareTestInstances({ flow: OAuthFlow.U2M });
        const requestedScopes: Array<string> = [];
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
          if (error instanceof AssertionError || !(error instanceof Error)) {
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
        sinon.stub(oauthClient, 'refresh').returns(
          Promise.resolve(
            new TokenSet({
              access_token: undefined,
              refresh_token: undefined,
            }),
          ),
        );

        try {
          const token = new OAuthToken(createExpiredAccessToken(), oauthClient.refreshToken);
          expect(token.hasExpired).to.be.true;

          await oauthManager.refreshAccessToken(token);
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError || !(error instanceof Error)) {
            throw error;
          }
          expect(oauthClient.refresh.called).to.be.true;
          expect(error.message).to.contain('invalid response');
        }
      });
    });

    describe('M2M flow', () => {
      function getExpectedScope(scopes: Array<string>) {
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
          if (error instanceof AssertionError || !(error instanceof Error)) {
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
        const requestedScopes: Array<string> = [];
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
        sinon.stub(oauthClient, 'grant').returns(
          Promise.resolve(
            new TokenSet({
              access_token: undefined,
              refresh_token: undefined,
            }),
          ),
        );

        try {
          await oauthManager.refreshAccessToken(token);
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError || !(error instanceof Error)) {
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
