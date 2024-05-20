import { expect } from 'chai';
import sinon from 'sinon';
import DatabricksOAuth, { DatabricksOAuthOptions, OAuthFlow } from '../../../../../lib/connection/auth/DatabricksOAuth';
import OAuthToken from '../../../../../lib/connection/auth/DatabricksOAuth/OAuthToken';
import { OAuthManagerOptions } from '../../../../../lib/connection/auth/DatabricksOAuth/OAuthManager';

import {
  createExpiredAccessToken,
  createValidAccessToken,
  OAuthManagerStub,
  OAuthPersistenceStub,
} from '../../../.stubs/OAuth';
import ClientContextStub from '../../../.stubs/ClientContextStub';

class DatabricksOAuthTest extends DatabricksOAuth {
  public manager: OAuthManagerStub;

  constructor(options: Partial<DatabricksOAuthOptions> = {}) {
    super({
      context: new ClientContextStub(),
      flow: OAuthFlow.M2M,
      host: 'localhost',
      ...options,
    });

    this.manager = this.createManager(this.options);
  }

  protected createManager(options: OAuthManagerOptions): OAuthManagerStub {
    return new OAuthManagerStub(options);
  }
}

describe('DatabricksOAuth', () => {
  it('should get persisted token if available', async () => {
    const persistence = sinon.spy(new OAuthPersistenceStub());
    persistence.token = new OAuthToken(createValidAccessToken());

    const provider = new DatabricksOAuthTest({ persistence });

    await provider.authenticate();
    expect(persistence.read.called).to.be.true;
  });

  it('should get new token if storage not available', async () => {
    const provider = new DatabricksOAuthTest();
    const oauthManager = sinon.spy(provider.manager);

    await provider.authenticate();
    expect(oauthManager.getToken.called).to.be.true;
  });

  it('should get new token if persisted token not available, and store valid token', async () => {
    const persistence = sinon.spy(new OAuthPersistenceStub());
    persistence.token = undefined;

    const provider = new DatabricksOAuthTest({ persistence });
    const oauthManager = sinon.spy(provider.manager);

    await provider.authenticate();
    expect(oauthManager.getToken.called).to.be.true;
    expect(persistence.persist.called).to.be.true;
    expect(persistence.token).to.be.equal(oauthManager.getTokenResult);
  });

  it('should refresh expired token and store new token', async () => {
    const persistence = sinon.spy(new OAuthPersistenceStub());
    persistence.token = undefined;

    const provider = new DatabricksOAuthTest({ persistence });
    const oauthManager = sinon.spy(provider.manager);

    oauthManager.getTokenResult = new OAuthToken(createExpiredAccessToken());
    oauthManager.refreshTokenResult = new OAuthToken(createValidAccessToken());

    await provider.authenticate();
    expect(oauthManager.getToken.called).to.be.true;
    expect(oauthManager.refreshAccessToken.called).to.be.true;
    expect(oauthManager.refreshAccessToken.firstCall.firstArg).to.be.equal(oauthManager.getTokenResult);
    expect(persistence.token).to.be.equal(oauthManager.refreshTokenResult);
    expect(persistence.persist.called).to.be.true;
    expect(persistence.token).to.be.equal(oauthManager.refreshTokenResult);
  });

  it('should configure transport using valid token', async () => {
    const provider = new DatabricksOAuthTest();
    const oauthManager = sinon.spy(provider.manager);

    const authHeaders = await provider.authenticate();
    expect(oauthManager.getToken.called).to.be.true;
    expect(Object.keys(authHeaders)).to.deep.equal(['Authorization']);
  });
});
