const { expect, AssertionError } = require('chai');
const sinon = require('sinon');
const DatabricksOAuth = require('../../../../../lib/connection/auth/DatabricksOAuth/index').default;
const OAuthToken = require('../../../../../lib/connection/auth/DatabricksOAuth/OAuthToken').default;
const OAuthManager = require('../../../../../lib/connection/auth/DatabricksOAuth/OAuthManager').default;

const { createValidAccessToken, createExpiredAccessToken } = require('./utils');

class OAuthManagerMock {
  constructor() {
    this.getTokenResult = new OAuthToken(createValidAccessToken());
    this.refreshTokenResult = new OAuthToken(createValidAccessToken());
  }

  async refreshAccessToken(token) {
    return token.hasExpired ? this.refreshTokenResult : token;
  }

  async getToken() {
    return this.getTokenResult;
  }
}

class OAuthPersistenceMock {
  constructor() {
    this.token = undefined;

    sinon.stub(this, 'persist').callThrough();
    sinon.stub(this, 'read').callThrough();
  }

  async persist(host, token) {
    this.token = token;
  }

  async read() {
    return this.token;
  }
}

function prepareTestInstances(options) {
  const oauthManager = new OAuthManagerMock();

  sinon.stub(oauthManager, 'refreshAccessToken').callThrough();
  sinon.stub(oauthManager, 'getToken').callThrough();

  sinon.stub(OAuthManager, 'getManager').returns(oauthManager);

  const provider = new DatabricksOAuth({ ...options });

  return { oauthManager, provider };
}

describe('DatabricksOAuth', () => {
  afterEach(() => {
    OAuthManager.getManager.restore?.();
  });

  it('should get persisted token if available', async () => {
    const persistence = new OAuthPersistenceMock();
    persistence.token = new OAuthToken(createValidAccessToken());

    const { provider } = prepareTestInstances({ persistence });

    await provider.authenticate();
    expect(persistence.read.called).to.be.true;
  });

  it('should get new token if storage not available', async () => {
    const { oauthManager, provider } = prepareTestInstances();

    await provider.authenticate();
    expect(oauthManager.getToken.called).to.be.true;
  });

  it('should get new token if persisted token not available, and store valid token', async () => {
    const persistence = new OAuthPersistenceMock();
    persistence.token = undefined;
    const { oauthManager, provider } = prepareTestInstances({ persistence });

    await provider.authenticate();
    expect(oauthManager.getToken.called).to.be.true;
    expect(persistence.persist.called).to.be.true;
    expect(persistence.token).to.be.equal(oauthManager.getTokenResult);
  });

  it('should refresh expired token and store new token', async () => {
    const persistence = new OAuthPersistenceMock();
    persistence.token = undefined;

    const { oauthManager, provider } = prepareTestInstances({ persistence });
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
    const { oauthManager, provider } = prepareTestInstances();

    const authHeaders = await provider.authenticate();
    expect(oauthManager.getToken.called).to.be.true;
    expect(Object.keys(authHeaders)).to.deep.equal(['Authorization']);
  });
});
