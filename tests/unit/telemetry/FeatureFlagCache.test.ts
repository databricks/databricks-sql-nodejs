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
import FeatureFlagCache, { FeatureFlagContext } from '../../../lib/telemetry/FeatureFlagCache';
import ClientContextStub from '../.stubs/ClientContextStub';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

describe('FeatureFlagCache', () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
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
      expect(ctx.telemetryEnabled).to.be.undefined;
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

  describe('isTelemetryEnabled', () => {
    it('should return false for non-existent host', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);

      const enabled = await cache.isTelemetryEnabled('non-existent-host.databricks.com');
      expect(enabled).to.be.false;
    });

    it('should fetch feature flag when context exists but not fetched', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      // Stub the private fetchFeatureFlag method
      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlag').resolves(true);

      cache.getOrCreateContext(host);
      const enabled = await cache.isTelemetryEnabled(host);

      expect(fetchStub.calledOnce).to.be.true;
      expect(fetchStub.calledWith(host)).to.be.true;
      expect(enabled).to.be.true;

      fetchStub.restore();
    });

    it('should use cached value if not expired', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlag').resolves(true);

      cache.getOrCreateContext(host);

      // First call - should fetch
      await cache.isTelemetryEnabled(host);
      expect(fetchStub.calledOnce).to.be.true;

      // Advance time by 10 minutes (less than 15 minute TTL)
      clock.tick(10 * 60 * 1000);

      // Second call - should use cached value
      const enabled = await cache.isTelemetryEnabled(host);
      expect(fetchStub.calledOnce).to.be.true; // Still only called once
      expect(enabled).to.be.true;

      fetchStub.restore();
    });

    it('should refetch when cache expires after 15 minutes', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlag');
      fetchStub.onFirstCall().resolves(true);
      fetchStub.onSecondCall().resolves(false);

      cache.getOrCreateContext(host);

      // First call - should fetch
      const enabled1 = await cache.isTelemetryEnabled(host);
      expect(enabled1).to.be.true;
      expect(fetchStub.calledOnce).to.be.true;

      // Advance time by 16 minutes (more than 15 minute TTL)
      clock.tick(16 * 60 * 1000);

      // Second call - should refetch due to expiration
      const enabled2 = await cache.isTelemetryEnabled(host);
      expect(enabled2).to.be.false;
      expect(fetchStub.calledTwice).to.be.true;

      fetchStub.restore();
    });

    it('should log errors at debug level and return false on fetch failure', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlag').rejects(new Error('Network error'));

      cache.getOrCreateContext(host);
      const enabled = await cache.isTelemetryEnabled(host);

      expect(enabled).to.be.false;
      expect(logSpy.calledWith(LogLevel.debug, 'Error fetching feature flag: Network error')).to.be.true;

      fetchStub.restore();
      logSpy.restore();
    });

    it('should not propagate exceptions from fetchFeatureFlag', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlag').rejects(new Error('Network error'));

      cache.getOrCreateContext(host);

      // Should not throw
      const enabled = await cache.isTelemetryEnabled(host);
      expect(enabled).to.equal(false);

      fetchStub.restore();
    });

    it('should return false when telemetryEnabled is undefined', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlag').resolves(undefined);

      cache.getOrCreateContext(host);
      const enabled = await cache.isTelemetryEnabled(host);

      expect(enabled).to.be.false;

      fetchStub.restore();
    });
  });

  describe('fetchFeatureFlag', () => {
    it('should return false as placeholder implementation', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      // Access private method through any cast
      const result = await (cache as any).fetchFeatureFlag(host);
      expect(result).to.be.false;
    });
  });

  describe('Integration scenarios', () => {
    it('should handle multiple connections to same host with caching', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host = 'test-host.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlag').resolves(true);

      // Simulate 3 connections to same host
      cache.getOrCreateContext(host);
      cache.getOrCreateContext(host);
      cache.getOrCreateContext(host);

      // All connections check telemetry - should only fetch once
      await cache.isTelemetryEnabled(host);
      await cache.isTelemetryEnabled(host);
      await cache.isTelemetryEnabled(host);

      expect(fetchStub.calledOnce).to.be.true;

      // Close all connections
      cache.releaseContext(host);
      cache.releaseContext(host);
      cache.releaseContext(host);

      // Context should be removed
      const enabled = await cache.isTelemetryEnabled(host);
      expect(enabled).to.be.false; // No context, returns false

      fetchStub.restore();
    });

    it('should maintain separate state for different hosts', async () => {
      const context = new ClientContextStub();
      const cache = new FeatureFlagCache(context);
      const host1 = 'host1.databricks.com';
      const host2 = 'host2.databricks.com';

      const fetchStub = sinon.stub(cache as any, 'fetchFeatureFlag');
      fetchStub.withArgs(host1).resolves(true);
      fetchStub.withArgs(host2).resolves(false);

      cache.getOrCreateContext(host1);
      cache.getOrCreateContext(host2);

      const enabled1 = await cache.isTelemetryEnabled(host1);
      const enabled2 = await cache.isTelemetryEnabled(host2);

      expect(enabled1).to.be.true;
      expect(enabled2).to.be.false;

      fetchStub.restore();
    });
  });
});
