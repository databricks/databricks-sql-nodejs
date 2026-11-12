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

  it('should pass normalized configured scopes to the manager, or none when unset or blank', async () => {
    const cases: Array<[Array<string> | undefined, Array<string> | undefined]> = [
      [[' sql   offline_access '], ['sql', 'offline_access']],
      [['sql', 'sql'], ['sql']],
      [['   '], undefined],
      [[], undefined],
      [undefined, undefined],
    ];
    for (const [scopes, expected] of cases) {
      const options = { ...optionsStub, scopes };
      const oauthManager = new OAuthManagerStub(options);
      const oauthManagerSpy = sinon.spy(oauthManager);
      const provider = new DatabricksOAuth(options);
      provider['manager'] = oauthManager;

      // eslint-disable-next-line no-await-in-loop
      await provider.authenticate();
      const [requested] = oauthManagerSpy.getToken.firstCall.args as unknown[];
      expect(requested, JSON.stringify(scopes)).to.deep.equal(expected);
    }
  });

  it('should re-acquire an expired persisted M2M token with the configured scopes, not the cached ones', async () => {
    const cases: Array<[Array<string> | undefined, Array<string> | undefined]> = [
      [['sql'], ['sql']],
      [undefined, undefined],
    ];
    for (const [scopes, expected] of cases) {
      const persistence = sinon.spy(new OAuthPersistenceStub());
      persistence.token = new OAuthToken(createExpiredAccessToken(), undefined, ['all-apis']);

      const options = { ...optionsStub, flow: OAuthFlow.M2M, scopes, persistence };
      const oauthManager = new OAuthManagerStub(options);
      const oauthManagerSpy = sinon.spy(oauthManager);
      const provider = new DatabricksOAuth(options);
      provider['manager'] = oauthManager;

      // eslint-disable-next-line no-await-in-loop
      await provider.authenticate();
      expect(oauthManagerSpy.getToken.calledOnce, JSON.stringify(scopes)).to.be.true;
      const [requested] = oauthManagerSpy.getToken.firstCall.args as unknown[];
      expect(requested, JSON.stringify(scopes)).to.deep.equal(expected);
      expect(oauthManagerSpy.refreshAccessToken.firstCall.firstArg).to.be.equal(oauthManagerSpy.getTokenResult);
      expect(persistence.token).to.be.equal(oauthManagerSpy.getTokenResult);
    }
  });

  it('should reuse a valid persisted M2M token issued for the configured scopes without a new grant', async () => {
    const persistence = sinon.spy(new OAuthPersistenceStub());
    const cached = new OAuthToken(createValidAccessToken(), undefined, ['offline_access', 'sql']);
    persistence.token = cached;

    const options = { ...optionsStub, flow: OAuthFlow.M2M, scopes: ['sql offline_access'], persistence };
    const oauthManager = new OAuthManagerStub(options);
    const oauthManagerSpy = sinon.spy(oauthManager);
    const provider = new DatabricksOAuth(options);
    provider['manager'] = oauthManager;

    await provider.authenticate();
    expect(oauthManagerSpy.getToken.called).to.be.false;
    expect(persistence.token).to.be.equal(cached);
  });

  it('should keep refreshing an expired persisted U2M token issued for the configured scopes', async () => {
    const persistence = sinon.spy(new OAuthPersistenceStub());
    const cached = new OAuthToken(createExpiredAccessToken(), 'refresh-token', ['sql']);
    persistence.token = cached;

    const options = { ...optionsStub, flow: OAuthFlow.U2M, scopes: ['sql'], persistence };
    const oauthManager = new OAuthManagerStub(options);
    const oauthManagerSpy = sinon.spy(oauthManager);
    const provider = new DatabricksOAuth(options);
    provider['manager'] = oauthManager;

    await provider.authenticate();
    expect(oauthManagerSpy.getToken.called).to.be.false;
    expect(oauthManagerSpy.refreshAccessToken.firstCall.firstArg).to.be.equal(cached);
  });

  it('should discard a persisted token issued for other scopes and acquire one with the configured scopes', async () => {
    const cases: Array<[OAuthFlow, OAuthToken]> = [
      [OAuthFlow.M2M, new OAuthToken(createValidAccessToken(), undefined, ['all-apis'])],
      [OAuthFlow.U2M, new OAuthToken(createValidAccessToken(), 'refresh-token', ['all-apis', 'offline_access'])],
      [OAuthFlow.U2M, new OAuthToken(createExpiredAccessToken(), 'refresh-token', ['sql', 'offline_access'])],
    ];
    for (const [flow, cached] of cases) {
      const persistence = sinon.spy(new OAuthPersistenceStub());
      persistence.token = cached;

      const options = { ...optionsStub, flow, scopes: ['sql'], persistence };
      const oauthManager = new OAuthManagerStub(options);
      const oauthManagerSpy = sinon.spy(oauthManager);
      const provider = new DatabricksOAuth(options);
      provider['manager'] = oauthManager;

      // eslint-disable-next-line no-await-in-loop
      await provider.authenticate();
      expect(oauthManagerSpy.getToken.calledOnce, flow).to.be.true;
      const [requested] = oauthManagerSpy.getToken.firstCall.args as unknown[];
      expect(requested, flow).to.deep.equal(['sql']);
      expect(oauthManagerSpy.refreshAccessToken.firstCall.firstArg, flow).to.be.equal(oauthManagerSpy.getTokenResult);
      expect(persistence.token, flow).to.be.equal(oauthManagerSpy.getTokenResult);
    }
  });

  it('should compare persisted scopes with the default scopes when none are configured', async () => {
    const options = { ...optionsStub, flow: OAuthFlow.U2M, host: 'example.cloud.databricks.com' };
    for (const [cachedScopes, reused] of [
      [['offline_access', 'sql'], true],
      [['all-apis', 'offline_access'], false],
    ] as Array<[Array<string>, boolean]>) {
      const persistence = new OAuthPersistenceStub();
      const cached = new OAuthToken(createValidAccessToken(), 'refresh-token', cachedScopes);
      persistence.token = cached;

      const provider = new DatabricksOAuth({ ...options, persistence });
      // Real scope defaults/mapping, but no network: stub only the grant
      const manager = provider['getManager']();
      const getToken = sinon.stub(manager, 'getToken').resolves(new OAuthToken(createValidAccessToken()));

      // eslint-disable-next-line no-await-in-loop
      await provider.authenticate();
      expect(getToken.called, JSON.stringify(cachedScopes)).to.equal(!reused);
      expect(persistence.token === cached, JSON.stringify(cachedScopes)).to.equal(reused);
    }
  });

  it('should keep a persisted token that does not record its scopes', async () => {
    const persistence = sinon.spy(new OAuthPersistenceStub());
    const cached = new OAuthToken(createValidAccessToken(), 'refresh-token');
    persistence.token = cached;

    const options = { ...optionsStub, flow: OAuthFlow.U2M, scopes: ['sql'], persistence };
    const oauthManager = new OAuthManagerStub(options);
    const oauthManagerSpy = sinon.spy(oauthManager);
    const provider = new DatabricksOAuth(options);
    provider['manager'] = oauthManager;

    await provider.authenticate();
    expect(oauthManagerSpy.getToken.called).to.be.false;
    expect(persistence.token).to.be.equal(cached);
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
