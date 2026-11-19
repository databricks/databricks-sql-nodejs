import { expect } from 'chai';
import sinon from 'sinon';
import { Response } from 'node-fetch';
import FeatureFlagCache from '../../../lib/FeatureFlagCache';
import ClientContextStub from '../.stubs/ClientContextStub';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import DBSQLClient from '../../../lib/DBSQLClient';
import ThriftBackend from '../../../lib/thrift-backend/ThriftBackend';
import TelemetryClientProvider from '../../../lib/telemetry/TelemetryClientProvider';
import DatabricksOAuth from '../../../lib/connection/auth/DatabricksOAuth';
import StaticTokenProvider from '../../../lib/connection/auth/tokenProvider/StaticTokenProvider';

describe('FeatureFlagCache idle retention', () => {
  const host = 'cache.databricks.com';
  const options = { host, path: '/sql/1.0/warehouses/test', token: 'identity-a' };
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
    (FeatureFlagCache as any).sharedContexts.clear();
    (FeatureFlagCache as any).idleContexts?.clear();
    clock = sinon.useFakeTimers();
  });

  afterEach(() => {
    sinon.restore();
    TelemetryClientProvider.__resetInstanceForTests();
    clock.restore();
  });

  function makeCache(config: any = {}, auth?: any) {
    return new FeatureFlagCache(new ClientContextStub(config), auth);
  }

  function response(value = 'true', ttl = 60) {
    return new Response(JSON.stringify({ flags: [{ name: 'flag', value }], ttl_seconds: ttl }));
  }

  it('reuses a successful response after the last consumer closes', async () => {
    const first = makeCache();
    const fetch = sinon.stub(first as any, 'fetchWithRetry').resolves(response());
    first.getOrCreateContext(host);
    expect(await first.getBoolean(host, 'flag')).to.equal(true);
    first.releaseContext(host);
    const next = makeCache();
    const nextFetch = sinon.stub(next as any, 'fetchWithRetry').resolves(response('false'));
    next.getOrCreateContext(host);
    expect(await next.getBoolean(host, 'flag')).to.equal(true);
    expect(fetch.callCount).to.equal(1);
    expect(nextFetch.callCount).to.equal(0);
    next.releaseContext(host);
  });

  it('keeps the original expiry across repeated close/reopen and wall-clock jumps', async () => {
    const first = makeCache();
    sinon.stub(first as any, 'fetchWithRetry').resolves(response());
    first.getOrCreateContext(host);
    await first.getBoolean(host, 'flag');
    first.releaseContext(host);
    clock.tick(59000);
    clock.setSystemTime(-100000000);
    const next = makeCache();
    const fetch = sinon.stub(next as any, 'fetchWithRetry').resolves(response('false'));
    next.getOrCreateContext(host);
    expect(await next.getBoolean(host, 'flag')).to.equal(true);
    clock.tick(1001);
    expect(await next.getBoolean(host, 'flag')).to.equal(false);
    expect(fetch.callCount).to.equal(1);
    next.releaseContext(host);
  });

  it('does not expire early on a forward wall-clock jump', async () => {
    const cache = makeCache();
    const fetch = sinon.stub(cache as any, 'fetchWithRetry').resolves(response());
    cache.getOrCreateContext(host);
    await cache.getBoolean(host, 'flag');
    clock.setSystemTime(100000000);
    await cache.getBoolean(host, 'flag');
    expect(fetch.callCount).to.equal(1);
    cache.releaseContext(host);
  });

  it('preserves every typed reader and null/missing defaults after close', async () => {
    const cases: Array<[string, string, any]> = [
      ['getBoolean', 'True', true],
      ['getBoolean', 'FALSE', false],
      ['getBoolean', 'null', false],
      ['getInt32', '-2147483648', -2147483648],
      ['getInt32', '2147483648', undefined],
      ['getInt64', '9223372036854775807', BigInt('9223372036854775807')],
      ['getInt64', '9223372036854775808', undefined],
      ['getDouble', '1.25', 1.25],
      ['getDouble', '1e400', undefined],
      ['getString', '"hello"', 'hello'],
      ['getString', 'null', undefined],
      ['getStringList', '["a","b"]', ['a', 'b']],
      ['getStringList', '[null]', undefined],
    ];
    const fetch = sinon
      .stub(FeatureFlagCache.prototype as any, 'fetchWithRetry')
      .resolves(
        new Response(JSON.stringify({ flags: cases.map(([, value], index) => ({ name: String(index), value })) })),
      );
    for (let iteration = 0; iteration < 2; iteration += 1) {
      const cache = makeCache();
      cache.getOrCreateContext(host);
      for (const [index, [method, , expected]] of cases.entries()) {
        expect(await (cache as any)[method](host, String(index))).to.deep.equal(expected);
      }
      expect(await cache.getString(host, 'missing', 'fallback')).to.equal('fallback');
      cache.releaseContext(host);
    }
    expect(fetch.callCount).to.equal(1);
  });

  for (const enabled of [true, false]) {
    it(`preserves telemetry enablement (${enabled}) and the environment kill switch on an idle hit`, async () => {
      sinon.stub(ThriftBackend.prototype, 'connect').resolves();
      sinon.stub(ThriftBackend.prototype, 'close').resolves();
      const fetch = sinon.stub(FeatureFlagCache.prototype as any, 'fetchWithRetry').callsFake(
        async () =>
          new Response(
            JSON.stringify({
              flags: [
                {
                  name: 'databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForNodeJs',
                  value: String(enabled),
                },
              ],
            }),
          ),
      );
      const previous = process.env.DATABRICKS_TELEMETRY_DISABLED;
      delete process.env.DATABRICKS_TELEMETRY_DISABLED;
      try {
        for (const killed of [false, true, false]) {
          if (killed) process.env.DATABRICKS_TELEMETRY_DISABLED = 'true';
          else delete process.env.DATABRICKS_TELEMETRY_DISABLED;
          const client = new DBSQLClient({ logger: { log: () => {} } });
          await client.connect(options);
          expect(client.getTelemetryStats() !== undefined).to.equal(enabled && !killed);
          await client.close();
        }
        expect(fetch.callCount).to.equal(1);
      } finally {
        if (previous === undefined) delete process.env.DATABRICKS_TELEMETRY_DISABLED;
        else process.env.DATABRICKS_TELEMETRY_DISABLED = previous;
      }
    });
  }

  it('partitions active and idle responses by workspace or normalized host, as on main', async () => {
    const fetch = sinon.stub(FeatureFlagCache.prototype as any, 'fetchWithRetry').callsFake(async () => response());
    const caches = [makeCache(), makeCache(), makeCache({ customHeaders: { 'x-databricks-org-id': '1' } })];
    const hosts = [host, 'other.databricks.com', host];
    const contexts = caches.map((cache, index) => cache.getOrCreateContext(hosts[index]));
    expect(new Set(contexts).size).to.equal(3);
    await Promise.all(caches.map((cache, index) => cache.getBoolean(hosts[index], 'flag')));
    expect(fetch.callCount).to.equal(3);
    caches.forEach((cache, index) => cache.releaseContext(hosts[index]));
    expect((FeatureFlagCache as any).idleContexts.size).to.equal(3);
  });

  it('shares active and idle workspace values across hosts, providers and non-routing headers', async () => {
    const firstAuth = { authenticate: sinon.stub().resolves({ Authorization: 'Bearer first-secret' }) };
    const nextAuth = { authenticate: sinon.stub().resolves({ Authorization: 'Bearer next-secret' }) };
    const first = makeCache({ customHeaders: { 'x-databricks-org-id': '1', 'X-Custom': 'first' } }, firstAuth);
    const next = makeCache(
      { customHeaders: { 'X-Databricks-Org-Id': '1', 'X-Custom': 'next' }, userAgentEntry: 'other-app' },
      nextAuth,
    );
    const fetch = sinon.stub(first as any, 'fetchWithRetry').resolves(response());
    const nextFetch = sinon.stub(next as any, 'fetchWithRetry').resolves(response('false'));
    const original = first.getOrCreateContext(host);
    expect(next.getOrCreateContext('other.databricks.com')).to.equal(original);
    expect(
      await Promise.all([first.getBoolean(host, 'flag'), next.getBoolean('other.databricks.com', 'flag')]),
    ).to.deep.equal([true, true]);
    first.releaseContext(host);
    next.releaseContext('other.databricks.com');
    clock.tick(2000);
    expect(next.getOrCreateContext('third.databricks.com')).to.equal(original);
    expect(await next.getBoolean('third.databricks.com', 'flag')).to.equal(true);
    expect(fetch.callCount).to.equal(1);
    expect(nextFetch.callCount).to.equal(0);
    clock.tick(58001);
    expect(await next.getBoolean('third.databricks.com', 'flag')).to.equal(false);
    expect(nextAuth.authenticate.callCount).to.equal(1);
    expect(nextFetch.callCount).to.equal(1);
    next.releaseContext('third.databricks.com');
    expect(JSON.stringify([...(FeatureFlagCache as any).idleContexts])).to.not.contain('secret');
  });

  it('canonicalizes fallback hosts across an idle reconnect', async () => {
    const first = makeCache();
    sinon.stub(first as any, 'fetchWithRetry').resolves(response());
    const original = first.getOrCreateContext(host);
    await first.getBoolean(host, 'flag');
    first.releaseContext(host);
    const next = makeCache();
    expect(next.getOrCreateContext(`https://${host.toUpperCase()}:443/`)).to.equal(original);
    next.releaseContext(host);
  });

  for (const authType of [
    'access-token',
    'databricks-oauth',
    'token-provider',
    'external-token',
    'static-token',
    'custom',
  ]) {
    it(`deduplicates 50 distinct ${authType} clients before and after idle`, async () => {
      const oauth = sinon.stub(DatabricksOAuth.prototype, 'authenticate').resolves({ Authorization: 'Bearer oauth' });
      sinon.stub(ThriftBackend.prototype, 'connect').resolves();
      sinon.stub(ThriftBackend.prototype, 'close').resolves();
      const fetch = sinon.stub(FeatureFlagCache.prototype as any, 'fetchWithRetry').callsFake(async () => response());
      const burst = async () => {
        const clients = Array.from({ length: 50 }, () => new DBSQLClient({ logger: { log: () => {} } }));
        await Promise.all(
          clients.map((client, index) => {
            const common = { host, path: options.path, telemetryEnabled: false };
            const token = `distinct-secret-${index}`;
            let connection: ConnectionOptions;
            switch (authType) {
              case 'databricks-oauth':
                connection = { ...common, authType, oauthClientId: 'test-id', oauthClientSecret: 'test-secret' };
                break;
              case 'token-provider':
                connection = {
                  ...common,
                  authType,
                  tokenProvider: new StaticTokenProvider(token),
                  enableTokenFederation: false,
                };
                break;
              case 'external-token':
                connection = { ...common, authType, getToken: async () => token, enableTokenFederation: false };
                break;
              case 'static-token':
                connection = { ...common, authType, staticToken: token, enableTokenFederation: false };
                break;
              case 'custom':
                connection = {
                  ...common,
                  authType,
                  provider: { authenticate: async () => ({ Authorization: `Bearer ${token}` }) },
                };
                break;
              default:
                connection = { ...common, token };
            }
            return client.connect(connection);
          }),
        );
        expect(
          await Promise.all(clients.map((client) => (client as any).getFeatureFlagCache().getBoolean(host, 'flag'))),
        ).to.deep.equal(Array(50).fill(true));
        expect((FeatureFlagCache as any).sharedContexts.size).to.equal(1);
        await Promise.all(clients.map((client) => client.close()));
      };
      await burst();
      expect(fetch.callCount).to.equal(1);
      clock.tick(2000);
      await burst();
      expect(fetch.callCount).to.equal(1);
      expect(oauth.callCount).to.equal(authType === 'databricks-oauth' ? 1 : 0);
      expect((FeatureFlagCache as any).sharedContexts.size).to.equal(0);
      expect((FeatureFlagCache as any).idleContexts.size).to.equal(1);
    });
  }

  for (const [name, body, status] of [
    ['missing flags', {}, 200],
    ['null flags', { flags: null }, 200],
    ['malformed flags', { flags: [{ name: 'flag', value: true }] }, 200],
    ['partially malformed flags', { flags: [{ name: 'flag', value: 'true' }, null] }, 200],
    ['invalid JSON', '{', 200],
    ['HTTP failure', {}, 403],
  ] as Array<[string, any, number]>) {
    it(`does not retain ${name} after close`, async () => {
      const first = makeCache();
      const fetch = sinon
        .stub(first as any, 'fetchWithRetry')
        .resolves(new Response(typeof body === 'string' ? body : JSON.stringify(body), { status }));
      first.getOrCreateContext(host);
      await first.getBoolean(host, 'flag');
      await first.getBoolean(host, 'flag');
      expect(fetch.callCount).to.equal(1);
      first.releaseContext(host);
      const next = makeCache();
      const nextFetch = sinon.stub(next as any, 'fetchWithRetry').resolves(response());
      next.getOrCreateContext(host);
      expect(await next.getBoolean(host, 'flag')).to.equal(true);
      expect(nextFetch.callCount).to.equal(1);
      next.releaseContext(host);
    });
  }

  it('deduplicates a cold concurrent burst and a burst after idle', async () => {
    let complete!: (response: Response) => void;
    const fetch = sinon.stub(FeatureFlagCache.prototype as any, 'fetchWithRetry').returns(
      new Promise<Response>((resolve) => {
        complete = resolve;
      }),
    );
    const clients = Array.from({ length: 50 }, () => makeCache());
    clients.forEach((cache) => cache.getOrCreateContext(host));
    const reads = clients.map((cache) => cache.getBoolean(host, 'flag'));
    await clock.tickAsync(0);
    expect(fetch.callCount).to.equal(1);
    complete(response());
    expect(await Promise.all(reads)).to.deep.equal(Array(50).fill(true));
    clients.forEach((cache) => cache.releaseContext(host));
    clock.tick(2000);
    for (const cache of clients) cache.getOrCreateContext(host);
    expect(await Promise.all(clients.map((cache) => cache.getBoolean(host, 'flag')))).to.deep.equal(
      Array(50).fill(true),
    );
    expect(fetch.callCount).to.equal(1);
    clients.forEach((cache) => cache.releaseContext(host));
  });

  it('does not retain a response completed after release', async () => {
    let complete!: (response: Response) => void;
    const cache = makeCache();
    sinon.stub(cache as any, 'fetchWithRetry').returns(
      new Promise<Response>((resolve) => {
        complete = resolve;
      }),
    );
    cache.getOrCreateContext(host);
    const read = cache.getBoolean(host, 'flag');
    await clock.tickAsync(0);
    cache.releaseContext(host);
    complete(response());
    await read;
    expect((FeatureFlagCache as any).sharedContexts.size).to.equal(0);
    expect((FeatureFlagCache as any).idleContexts.size).to.equal(0);
  });

  it('bounds retained contexts after 1000 distinct workspaces without retaining credentials', async () => {
    sinon.stub(FeatureFlagCache.prototype as any, 'fetchWithRetry').callsFake(async () => response());
    for (let iteration = 0; iteration < 1000; iteration += 1) {
      const cache = makeCache(
        { customHeaders: { 'x-databricks-org-id': String(iteration) } },
        {
          authenticate: async () => ({ Authorization: 'Bearer secret-identity' }),
        },
      );
      cache.getOrCreateContext(host);
      expect(await cache.getBoolean(host, 'flag')).to.equal(true);
      cache.releaseContext(host);
    }
    expect((FeatureFlagCache as any).sharedContexts.size).to.equal(0);
    expect((FeatureFlagCache as any).idleContexts.size).to.equal(100);
    expect(JSON.stringify([...(FeatureFlagCache as any).idleContexts])).to.not.contain('secret-identity');
    clock.tick(61000);
    const next = makeCache();
    next.getOrCreateContext(host);
    await next.getBoolean(host, 'flag');
    next.releaseContext(host);
    expect((FeatureFlagCache as any).idleContexts.size).to.equal(1);
  });
});
