/**
 * Copyright (c) 2025 Databricks Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { expect } from 'chai';
import sinon from 'sinon';
import { Response } from 'node-fetch';
import FeatureFlagCache from '../../../lib/FeatureFlagCache';
import ClientContextStub from '../.stubs/ClientContextStub';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

describe('FeatureFlagCache', () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
    (FeatureFlagCache as any).sharedContexts.clear();
    clock = sinon.useFakeTimers();
  });

  afterEach(() => {
    clock.restore();
  });

  describe('getOrCreateContext', () => {
    it('should create a new context for a host', () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const ctx = cache.getOrCreateContext(host);

      expect(ctx).to.not.be.undefined;
      expect(ctx.refCount).to.equal(1);
      expect(ctx.cacheDuration).to.equal(15 * 60 * 1000); // 15 minutes
      expect(ctx.flags).to.be.undefined;
      expect(ctx.lastFetched).to.be.undefined;
    });

    it('should increment reference count on subsequent calls', () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const ctx1 = cache.getOrCreateContext(host);
      expect(ctx1.refCount).to.equal(1);

      const ctx2 = cache.getOrCreateContext(host);
      expect(ctx2.refCount).to.equal(2);
      expect(ctx1).to.equal(ctx2); // Same object reference
    });

    it('should manage multiple hosts independently', () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host1 = 'host1.databricks.com';
      const host2 = 'host2.databricks.com';

      const ctx1 = cache.getOrCreateContext(host1);
      const ctx2 = cache.getOrCreateContext(host2);

      expect(ctx1).to.not.equal(ctx2);
      expect(ctx1.refCount).to.equal(1);
      expect(ctx2.refCount).to.equal(1);
    });
  });

  describe('releaseContext', () => {
    it('should decrement reference count', () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      cache.getOrCreateContext(host);
      cache.getOrCreateContext(host);
      const ctx = cache.getOrCreateContext(host);
      expect(ctx.refCount).to.equal(3);

      cache.releaseContext(host);
      expect(ctx.refCount).to.equal(2);
    });

    it('should remove context when refCount reaches zero', () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      cache.getOrCreateContext(host);
      cache.releaseContext(host);

      // After release, getting context again should create a new one with refCount=1
      const ctx = cache.getOrCreateContext(host);
      expect(ctx.refCount).to.equal(1);
    });

    it('should handle releasing non-existent host gracefully', () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);

      // Should not throw
      expect(() => cache.releaseContext('non-existent-host.databricks.com')).to.not.throw();
    });

    it('should handle releasing host with refCount already at zero', () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      cache.getOrCreateContext(host);
      cache.releaseContext(host);

      // Second release should not throw
      expect(() => cache.releaseContext(host)).to.not.throw();
    });
  });

  describe('getBoolean', () => {
    it('should return false for non-existent host', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);

      const enabled = await cache.getBoolean('non-existent-host.databricks.com', 'flag');
      expect(enabled).to.be.false;
    });

    it('should fetch feature flag when context exists but not fetched', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      // Stub the private fetchFeatureFlags method
      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlags').resolves(new Map([['flag', 'true']]));

      cache.getOrCreateContext(host);
      const enabled = await cache.getBoolean(host, 'flag');

      expect(fetchStub.calledOnce).to.be.true;
      expect(fetchStub.calledWith(host)).to.be.true;
      expect(enabled).to.be.true;

      fetchStub.restore();
    });

    it('should use cached value if not expired', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlags').resolves(new Map([['flag', 'true']]));

      cache.getOrCreateContext(host);

      // First call - should fetch
      await cache.getBoolean(host, 'flag');
      expect(fetchStub.calledOnce).to.be.true;

      // Advance time by 10 minutes (less than 15 minute TTL)
      clock.tick(10 * 60 * 1000);

      // Second call - should use cached value
      const enabled = await cache.getBoolean(host, 'flag');
      expect(fetchStub.calledOnce).to.be.true; // Still only called once
      expect(enabled).to.be.true;

      fetchStub.restore();
    });

    it('should refetch when cache expires after 15 minutes', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlags');
      fetchStub.onFirstCall().resolves(new Map([['flag', 'true']]));
      fetchStub.onSecondCall().resolves(new Map([['flag', 'false']]));

      cache.getOrCreateContext(host);

      // First call - should fetch
      const enabled1 = await cache.getBoolean(host, 'flag');
      expect(enabled1).to.be.true;
      expect(fetchStub.calledOnce).to.be.true;

      // Advance time by 16 minutes (more than 15 minute TTL)
      clock.tick(16 * 60 * 1000);

      // Second call - should refetch due to expiration
      const enabled2 = await cache.getBoolean(host, 'flag');
      expect(enabled2).to.be.false;
      expect(fetchStub.calledTwice).to.be.true;

      fetchStub.restore();
    });

    it('should log errors at debug level and return false on fetch failure', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlags').rejects(new Error('Network error'));

      cache.getOrCreateContext(host);
      const enabled = await cache.getBoolean(host, 'flag');

      expect(enabled).to.be.false;
      expect(logSpy.calledWith(LogLevel.debug, 'Error fetching feature flag: Network error')).to.be.true;

      fetchStub.restore();
      logSpy.restore();
    });

    it('should not propagate exceptions from fetchFeatureFlags', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlags').rejects(new Error('Network error'));

      cache.getOrCreateContext(host);

      // Should not throw
      const enabled = await cache.getBoolean(host, 'flag');
      expect(enabled).to.equal(false);

      fetchStub.restore();
    });

    it('should return false when the flag is missing', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlags').resolves(undefined);

      cache.getOrCreateContext(host);
      const enabled = await cache.getBoolean(host, 'flag');

      expect(enabled).to.be.false;

      fetchStub.restore();
    });
  });

  describe('fetchFeatureFlags', () => {
    it('should default to false when the HTTP request fails', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      // Stub the network seam so the test is deterministic. The real
      // `fetchWithRetry` makes an HTTP call with a 10s timeout to the
      // (bogus) host; under mocha's 2s default this passed only when the
      // DNS failure happened to resolve quickly — flaky across runners /
      // Node versions (it timed out on Node 14/16/18 in CI). The behavior
      // under test is just that `fetchFeatureFlags` resolves to `false`.
      const fetchStub = sinon.stub(cache as any, 'fetchWithRetry').rejects(new Error('network disabled in test'));

      cache.getOrCreateContext(host);
      const result = await cache.getBoolean(host, 'flag');
      expect(result).to.be.false;

      fetchStub.restore();
    });
  });

  describe('Integration scenarios', () => {
    it('should handle multiple connections to same host with caching', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlags').resolves(new Map([['flag', 'true']]));

      // Simulate 3 connections to same host
      cache.getOrCreateContext(host);
      cache.getOrCreateContext(host);
      cache.getOrCreateContext(host);

      // All connections check telemetry - should only fetch once
      await cache.getBoolean(host, 'flag');
      await cache.getBoolean(host, 'flag');
      await cache.getBoolean(host, 'flag');

      expect(fetchStub.calledOnce).to.be.true;

      // Close all connections
      cache.releaseContext(host);
      cache.releaseContext(host);
      cache.releaseContext(host);

      // Context should be removed
      const enabled = await cache.getBoolean(host, 'flag');
      expect(enabled).to.be.false; // No context, returns false

      fetchStub.restore();
    });

    it('should maintain separate state for different hosts', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host1 = 'host1.databricks.com';
      const host2 = 'host2.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlags');
      fetchStub.withArgs(host1).resolves(new Map([['flag', 'true']]));
      fetchStub.withArgs(host2).resolves(new Map([['flag', 'false']]));

      cache.getOrCreateContext(host1);
      cache.getOrCreateContext(host2);

      const enabled1 = await cache.getBoolean(host1, 'flag');
      const enabled2 = await cache.getBoolean(host2, 'flag');

      expect(enabled1).to.be.true;
      expect(enabled2).to.be.false;

      fetchStub.restore();
    });
  });

  describe('customHeaders propagation (SPOG)', () => {
    function makeJsonResponse(body: unknown) {
      return Promise.resolve({
        ok: true,
        status: 200,
        statusText: 'OK',
        json: () => Promise.resolve(body),
        text: () => Promise.resolve(''),
      });
    }

    it('attaches config.customHeaders to the feature-flag GET', async () => {
      const context = new ClientContextStub({
        customHeaders: { 'x-databricks-org-id': '12345678901234' },
      } as any);
      const cache = new FeatureFlagCache(context);
      const stub = sinon.stub(cache as any, 'fetchWithRetry').returns(makeJsonResponse({ flags: [] }));

      await (cache as any).fetchFeatureFlags('host.example.com');

      expect(stub.calledOnce).to.be.true;
      const init = stub.firstCall.args[1] as { headers: Record<string, string> };
      expect(init.headers['x-databricks-org-id']).to.equal('12345678901234');
      stub.restore();
    });

    it('does not set x-databricks-org-id when customHeaders is empty', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const stub = sinon.stub(cache as any, 'fetchWithRetry').returns(makeJsonResponse({ flags: [] }));

      await (cache as any).fetchFeatureFlags('host.example.com');

      const init = stub.firstCall.args[1] as { headers: Record<string, string> };
      expect(init.headers).to.not.have.property('x-databricks-org-id');
      stub.restore();
    });
  });

  it('reads all six types from one GET without telemetry or a session', async () => {
    const cases = [
      ['getBoolean', 'True', true],
      ['getBoolean', '"true"', false],
      ['getInt32', '2147483647', 2147483647],
      ['getInt32', '2147483648', undefined],
      ['getInt64', '9223372036854775807', BigInt('9223372036854775807')],
      ['getInt64', '-9223372036854775808', BigInt('-9223372036854775808')],
      ['getInt64', '9223372036854775808', undefined],
      ['getInt64', '1.5', undefined],
      ['getInt64', '01', undefined],
      ['getInt64', 'true', undefined],
      ['getDouble', '1.25', 1.25],
      ['getDouble', '1e400', undefined],
      ['getString', '"hello"', 'hello'],
      ['getString', 'null', undefined],
      ['getStringList', '["a","b"]', ['a', 'b']],
      ['getStringList', '[null]', undefined],
      ['getString', 'invalid', undefined],
    ];
    const cache = new FeatureFlagCache(new ClientContextStub());
    cache.getOrCreateContext('test-host');
    const fetchStub = sinon.stub(cache as any, 'fetchWithRetry').resolves(
      new Response(
        JSON.stringify({
          flags: cases.map(([, value], index) => ({ name: String(index), value })),
          ttl_seconds: 60,
        }),
      ),
    );
    await Promise.all(
      cases.map(async ([method, , expected], index) => {
        expect(await (cache as any)[method as string]('test-host', String(index))).to.deep.equal(expected);
      }),
    );
    expect(await cache.getString('test-host', 'missing', 'fallback')).to.equal('fallback');
    expect(fetchStub.calledOnce).to.be.true;
  });

  it('shares values per workspace, refreshes with the current caller, and defaults on failure', async () => {
    const first = new FeatureFlagCache(new ClientContextStub({ customHeaders: { 'x-databricks-org-id': '1' } }));
    const current = new FeatureFlagCache(new ClientContextStub({ customHeaders: { 'X-Databricks-Org-Id': '1' } }));
    const other = new FeatureFlagCache(new ClientContextStub({ customHeaders: { 'x-databricks-org-id': '2' } }));
    expect(first.getOrCreateContext('host-a')).to.equal(current.getOrCreateContext('host-b'));
    expect(other.getOrCreateContext('host-a')).to.not.equal(first.getOrCreateContext('host-a'));
    const response = (value: string) =>
      new Response(
        JSON.stringify({
          flags: [{ name: 'flag', value }],
          ttl_seconds: 60,
        }),
      );
    const firstFetch = sinon.stub(first as any, 'fetchWithRetry').resolves(response('true'));
    const currentFetch = sinon.stub(current as any, 'fetchWithRetry').resolves(response('FALSE'));
    expect(await Promise.all([first.getBoolean('host-a', 'flag'), current.getBoolean('host-b', 'flag')])).to.deep.equal(
      [true, true],
    );
    expect(currentFetch.called).to.be.false;
    clock.tick(61000);
    expect(await current.getBoolean('host-b', 'flag', true)).to.be.false;
    expect(firstFetch.calledOnce).to.be.true;
    expect(currentFetch.calledOnce).to.be.true;
    clock.tick(61000);
    currentFetch.rejects(new Error('offline'));
    expect(await current.getBoolean('host-b', 'flag', true)).to.be.true;
    expect(await current.getBoolean('host-b', 'flag')).to.be.false;
    expect(currentFetch.calledTwice).to.be.true;
  });
});
