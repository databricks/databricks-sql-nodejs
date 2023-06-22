const { expect, AssertionError } = require('chai');
const sinon = require('sinon');
const DatabricksOAuth = require('../../../../../dist/connection/auth/DatabricksOAuth/index').default;
const OAuthToken = require('../../../../../dist/connection/auth/DatabricksOAuth/OAuthToken').default;
const OAuthManagerModule = require('../../../../../dist/connection/auth/DatabricksOAuth/OAuthManager');

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

class TransportMock {
  constructor() {
    this.headers = {};
  }

  updateHeaders(newHeaders) {
    this.headers = {
      ...this.headers,
      ...newHeaders,
    };
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

  sinon.stub(OAuthManagerModule, 'default').returns(oauthManager);

  const provider = new DatabricksOAuth({ ...options });

  const transport = new TransportMock();
  sinon.stub(transport, 'updateHeaders').callThrough();

  return { oauthManager, provider, transport };
}

describe('DatabricksOAuth', () => {
  afterEach(() => {
    OAuthManagerModule.default.restore?.();
  });

  it('should get persisted token if available', async () => {
    const persistence = new OAuthPersistenceMock();
    persistence.token = new OAuthToken(createValidAccessToken());

    const { provider, transport } = prepareTestInstances({ persistence });

    await provider.authenticate(transport);
    expect(persistence.read.called).to.be.true;
  });

  it('should get new token if storage not available', async () => {
    const { oauthManager, provider, transport } = prepareTestInstances();

    await provider.authenticate(transport);
    expect(oauthManager.getToken.called).to.be.true;
  });

  it('should get new token if persisted token not available, and store valid token', async () => {
    const persistence = new OAuthPersistenceMock();
    persistence.token = undefined;
    const { oauthManager, provider, transport } = prepareTestInstances({ persistence });

    await provider.authenticate(transport);
    expect(oauthManager.getToken.called).to.be.true;
    expect(persistence.persist.called).to.be.true;
    expect(persistence.token).to.be.equal(oauthManager.getTokenResult);
  });

  it('should refresh expired token and store new token', async () => {
    const persistence = new OAuthPersistenceMock();
    persistence.token = undefined;

    const { oauthManager, provider, transport } = prepareTestInstances({ persistence });
    oauthManager.getTokenResult = new OAuthToken(createExpiredAccessToken());
    oauthManager.refreshTokenResult = new OAuthToken(createValidAccessToken());

    await provider.authenticate(transport);
    expect(oauthManager.getToken.called).to.be.true;
    expect(oauthManager.refreshAccessToken.called).to.be.true;
    expect(oauthManager.refreshAccessToken.firstCall.firstArg).to.be.equal(oauthManager.getTokenResult);
    expect(persistence.token).to.be.equal(oauthManager.refreshTokenResult);
    expect(persistence.persist.called).to.be.true;
    expect(persistence.token).to.be.equal(oauthManager.refreshTokenResult);
  });

  it('should configure transport using valid token', async () => {
    const { oauthManager, provider, transport } = prepareTestInstances();

    const initialHeaders = {
      x: 'x',
      y: 'y',
    };

    transport.headers = initialHeaders;

    await provider.authenticate(transport);
    expect(oauthManager.getToken.called).to.be.true;
    expect(transport.updateHeaders.called).to.be.true;
    expect(Object.keys(transport.headers)).to.deep.equal([...Object.keys(initialHeaders), 'Authorization']);
  });
});
