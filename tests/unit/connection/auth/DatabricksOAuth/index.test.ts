import { expect } from 'chai';
import sinon from 'sinon';
import DatabricksOAuth, { OAuthFlow } from '../../../../../lib/connection/auth/DatabricksOAuth';
import OAuthToken from '../../../../../lib/connection/auth/DatabricksOAuth/OAuthToken';

import {
  createExpiredAccessToken,
  createValidAccessToken,
  OAuthManagerStub,
  OAuthPersistenceStub,
} from '../../../.stubs/OAuth';
import ClientContextStub from '../../../.stubs/ClientContextStub';

const optionsStub = {
  context: new ClientContextStub(),
  flow: OAuthFlow.M2M,
  host: 'localhost',
};

describe('DatabricksOAuth', () => {
  it('should get persisted token if available', async () => {
    const persistence = sinon.spy(new OAuthPersistenceStub());
    persistence.token = new OAuthToken(createValidAccessToken());

    const options = { ...optionsStub, persistence };
    const provider = new DatabricksOAuth(options);
    provider['manager'] = new OAuthManagerStub(options);

    await provider.authenticate();
    expect(persistence.read.called).to.be.true;
  });

  it('should get new token if storage not available', async () => {
    const options = { ...optionsStub };

    const oauthManager = new OAuthManagerStub(options);
    const oauthManagerSpy = sinon.spy(oauthManager);

    const provider = new DatabricksOAuth(options);
    provider['manager'] = oauthManager;

    await provider.authenticate();
    expect(oauthManagerSpy.getToken.called).to.be.true;
  });

  it('should get new token if persisted token not available, and store valid token', async () => {
    const persistence = sinon.spy(new OAuthPersistenceStub());
    persistence.token = undefined;

    const options = { ...optionsStub, persistence };

    const oauthManager = new OAuthManagerStub(options);
    const oauthManagerSpy = sinon.spy(oauthManager);

    const provider = new DatabricksOAuth(options);
    provider['manager'] = oauthManager;

    await provider.authenticate();
    expect(oauthManagerSpy.getToken.called).to.be.true;
    expect(persistence.persist.called).to.be.true;
    expect(persistence.token).to.be.equal(oauthManagerSpy.getTokenResult);
  });

  it('should refresh expired token and store new token', async () => {
    const persistence = sinon.spy(new OAuthPersistenceStub());
    persistence.token = undefined;

    const options = { ...optionsStub, persistence };

    const oauthManager = new OAuthManagerStub(options);
    const oauthManagerSpy = sinon.spy(oauthManager);

    const provider = new DatabricksOAuth(options);
    provider['manager'] = oauthManager;

    oauthManagerSpy.getTokenResult = new OAuthToken(createExpiredAccessToken());
    oauthManagerSpy.refreshTokenResult = new OAuthToken(createValidAccessToken());

    await provider.authenticate();
    expect(oauthManagerSpy.getToken.called).to.be.true;
    expect(oauthManagerSpy.refreshAccessToken.called).to.be.true;
    expect(oauthManagerSpy.refreshAccessToken.firstCall.firstArg).to.be.equal(oauthManagerSpy.getTokenResult);
    expect(persistence.token).to.be.equal(oauthManagerSpy.refreshTokenResult);
    expect(persistence.persist.called).to.be.true;
    expect(persistence.token).to.be.equal(oauthManagerSpy.refreshTokenResult);
  });

  it('should configure transport using valid token', async () => {
    const options = { ...optionsStub };

    const oauthManager = new OAuthManagerStub(options);
    const oauthManagerSpy = sinon.spy(oauthManager);

    const provider = new DatabricksOAuth(options);
    provider['manager'] = oauthManager;

    const authHeaders = await provider.authenticate();
    expect(oauthManagerSpy.getToken.called).to.be.true;
    expect(Object.keys(authHeaders)).to.deep.equal(['Authorization']);
  });
});
