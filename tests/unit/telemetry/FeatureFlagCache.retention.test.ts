import { expect } from 'chai';
import sinon from 'sinon';
import { Response } from 'node-fetch';
import FeatureFlagCache from '../../../lib/FeatureFlagCache';
import ClientContextStub from '../.stubs/ClientContextStub';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import DBSQLClient from '../../../lib/DBSQLClient';
import ThriftBackend from '../../../lib/thrift-backend/ThriftBackend';
import TelemetryClientProvider from '../../../lib/telemetry/TelemetryClientProvider';

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

  function makeCache(overrides: Partial<ConnectionOptions> = {}, config: any = {}, auth?: any) {
    return new FeatureFlagCache(new ClientContextStub(config), auth, { ...options, ...overrides } as ConnectionOptions);
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

  for (const [name, overrides, config] of [
    ['token', { token: 'identity-b' }, {}],
    ['workspace', {}, { customHeaders: { 'x-databricks-org-id': '2' } }],
    ['user agent', {}, { userAgentEntry: 'other-app' }],
    ['custom header', {}, { customHeaders: { 'X-Custom': 'other' } }],
    ['server trust', { checkServerCertificate: false }, {}],
    ['custom CA', { customCaCert: 'ca-other' }, {}],
    ['client certificate', { clientCert: 'cert-other', clientKey: 'key-other' }, {}],
    ['proxy', { proxy: { protocol: 'http', host: 'proxy.test', port: 8080 } }, {}],
    ['protocol', { https: false }, {}],
    ['port', { port: 8443 }, {}],
  ] as Array<[string, Partial<ConnectionOptions>, any]>) {
    it(`partitions active and idle responses by ${name}`, async () => {
      const first = makeCache();
      sinon.stub(first as any, 'fetchWithRetry').resolves(response());
      const original = first.getOrCreateContext(host);
      await first.getBoolean(host, 'flag');
      const other = makeCache(overrides, config);
      expect(other.getOrCreateContext(host)).to.not.equal(original);
      other.releaseContext(host);
      first.releaseContext(host);
      const next = makeCache(overrides, config);
      const fetch = sinon.stub(next as any, 'fetchWithRetry').resolves(response('false'));
      next.getOrCreateContext(host);
      expect(await next.getBoolean(host, 'flag')).to.equal(false);
      expect(fetch.callCount).to.equal(1);
      next.releaseContext(host);
    });
  }

  it('includes the host even when the workspace is the same', () => {
    const cache = makeCache({}, { customHeaders: { 'x-databricks-org-id': '1' } });
    expect(cache.getOrCreateContext(host)).to.not.equal(cache.getOrCreateContext('other.databricks.com'));
    cache.releaseContext(host);
    cache.releaseContext('other.databricks.com');
  });

  it('canonicalizes host and custom header casing and order', () => {
    const first = makeCache({}, { customHeaders: { 'X-Route': 'a', 'X-Databricks-Org-Id': '1' } });
    const next = makeCache({}, { customHeaders: { 'x-databricks-org-id': '1', 'x-route': 'a' } });
    expect(first.getOrCreateContext(host)).to.equal(next.getOrCreateContext(`https://${host.toUpperCase()}:443/`));
    first.releaseContext(host);
    next.releaseContext(host);
  });

  it('never retains a dynamic or deprecated authentication provider across close', async () => {
    const auth = { authenticate: sinon.stub().resolves({ Authorization: 'Bearer changing-token' }) };
    for (let iteration = 0; iteration < 2; iteration += 1) {
      const cache = makeCache({}, {}, auth);
      const fetch = sinon.stub(cache as any, 'fetchWithRetry').resolves(response(iteration === 0 ? 'true' : 'false'));
      cache.getOrCreateContext(host);
      expect(await cache.getBoolean(host, 'flag')).to.equal(iteration === 0);
      expect(fetch.callCount).to.equal(1);
      cache.releaseContext(host);
    }
    expect((FeatureFlagCache as any).idleContexts.size).to.equal(0);
    expect(auth.authenticate.callCount).to.equal(2);
  });

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

  it('bounds retained contexts after 1000 distinct identities without retaining credentials', async () => {
    sinon.stub(FeatureFlagCache.prototype as any, 'fetchWithRetry').callsFake(async () => response());
    for (let iteration = 0; iteration < 1000; iteration += 1) {
      const cache = makeCache({ token: `secret-identity-${iteration}` });
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
