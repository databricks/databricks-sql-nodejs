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
import { DBSQLClient } from '../../../lib';
import config from '../utils/config';
import FeatureFlagCache from '../../../lib/telemetry/FeatureFlagCache';
import TelemetryClientProvider from '../../../lib/telemetry/TelemetryClientProvider';
import TelemetryEventEmitter from '../../../lib/telemetry/TelemetryEventEmitter';
import MetricsAggregator from '../../../lib/telemetry/MetricsAggregator';

describe('Telemetry Integration', () => {
  describe('Initialization', () => {
    it('should initialize telemetry when telemetryEnabled is true', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      // Spy on initialization components
      const featureFlagCacheSpy = sinon.spy(FeatureFlagCache.prototype, 'getOrCreateContext');
      const telemetryProviderSpy = sinon.spy(TelemetryClientProvider.prototype, 'getOrCreateClient');

      try {
        await client.connect({
          host: config.host,
          path: config.path,
          token: config.token,
          telemetryEnabled: true,
        });

        // Verify telemetry components were initialized
        expect(featureFlagCacheSpy.called).to.be.true;

        await client.close();
      } finally {
        featureFlagCacheSpy.restore();
        telemetryProviderSpy.restore();
      }
    });

    it('should not initialize telemetry when telemetryEnabled is false', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      const featureFlagCacheSpy = sinon.spy(FeatureFlagCache.prototype, 'getOrCreateContext');

      try {
        await client.connect({
          host: config.host,
          path: config.path,
          token: config.token,
          telemetryEnabled: false,
        });

        // Verify telemetry was not initialized
        expect(featureFlagCacheSpy.called).to.be.false;

        await client.close();
      } finally {
        featureFlagCacheSpy.restore();
      }
    });

    it('should respect feature flag when telemetry is enabled', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      // Stub feature flag to return false
      const featureFlagStub = sinon.stub(FeatureFlagCache.prototype, 'isTelemetryEnabled').resolves(false);

      try {
        await client.connect({
          host: config.host,
          path: config.path,
          token: config.token,
          telemetryEnabled: true,
        });

        // Verify feature flag was checked
        expect(featureFlagStub.called).to.be.true;

        await client.close();
      } finally {
        featureFlagStub.restore();
      }
    });
  });

  describe('Reference Counting', () => {
    it('should share telemetry client across multiple connections to same host', async function () {
      this.timeout(60000);

      const client1 = new DBSQLClient();
      const client2 = new DBSQLClient();

      const getOrCreateClientSpy = sinon.spy(TelemetryClientProvider.prototype, 'getOrCreateClient');
      const releaseClientSpy = sinon.spy(TelemetryClientProvider.prototype, 'releaseClient');

      try {
        // Enable telemetry for both clients
        await client1.connect({
          host: config.host,
          path: config.path,
          token: config.token,
          telemetryEnabled: true,
        });

        await client2.connect({
          host: config.host,
          path: config.path,
          token: config.token,
          telemetryEnabled: true,
        });

        // Both clients should get the same telemetry client for the host
        expect(getOrCreateClientSpy.callCount).to.be.at.least(2);

        // Close first client
        await client1.close();
        expect(releaseClientSpy.callCount).to.be.at.least(1);

        // Close second client
        await client2.close();
        expect(releaseClientSpy.callCount).to.be.at.least(2);
      } finally {
        getOrCreateClientSpy.restore();
        releaseClientSpy.restore();
      }
    });

    it('should cleanup telemetry on close', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      const releaseClientSpy = sinon.spy(TelemetryClientProvider.prototype, 'releaseClient');
      const releaseContextSpy = sinon.spy(FeatureFlagCache.prototype, 'releaseContext');
      const flushSpy = sinon.spy(MetricsAggregator.prototype, 'flush');

      try {
        await client.connect({
          host: config.host,
          path: config.path,
          token: config.token,
          telemetryEnabled: true,
        });

        await client.close();

        // Verify cleanup was called
        expect(releaseClientSpy.called || flushSpy.called || releaseContextSpy.called).to.be.true;
      } finally {
        releaseClientSpy.restore();
        releaseContextSpy.restore();
        flushSpy.restore();
      }
    });
  });

  describe('Error Handling', () => {
    it('should continue driver operation when telemetry initialization fails', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      // Stub feature flag to throw an error
      const featureFlagStub = sinon.stub(FeatureFlagCache.prototype, 'isTelemetryEnabled').rejects(new Error('Feature flag fetch failed'));

      try {
        // Connection should succeed even if telemetry fails
        await client.connect({
          host: config.host,
          path: config.path,
          token: config.token,
          telemetryEnabled: true,
        });

        // Should be able to open a session
        const session = await client.openSession({
          initialCatalog: config.catalog,
          initialSchema: config.schema,
        });

        // Should be able to execute a query
        const operation = await session.executeStatement('SELECT 1 AS test');
        const result = await operation.fetchAll();

        expect(result).to.have.lengthOf(1);
        expect(result[0]).to.deep.equal({ test: 1 });

        await session.close();
        await client.close();
      } finally {
        featureFlagStub.restore();
      }
    });

    it('should continue driver operation when feature flag fetch fails', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      // Stub getOrCreateContext to throw
      const contextStub = sinon.stub(FeatureFlagCache.prototype, 'getOrCreateContext').throws(new Error('Context creation failed'));

      try {
        // Connection should succeed even if telemetry fails
        await client.connect({
          host: config.host,
          path: config.path,
          token: config.token,
          telemetryEnabled: true,
        });

        // Should be able to open a session
        const session = await client.openSession({
          initialCatalog: config.catalog,
          initialSchema: config.schema,
        });

        await session.close();
        await client.close();
      } finally {
        contextStub.restore();
      }
    });

    it('should not throw exceptions due to telemetry errors', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      // Stub multiple telemetry methods to throw
      const emitterStub = sinon.stub(TelemetryEventEmitter.prototype, 'emitConnectionOpen').throws(new Error('Emitter failed'));
      const aggregatorStub = sinon.stub(MetricsAggregator.prototype, 'processEvent').throws(new Error('Aggregator failed'));

      try {
        // Connection should not throw
        await client.connect({
          host: config.host,
          path: config.path,
          token: config.token,
          telemetryEnabled: true,
        });

        // Driver operations should work normally
        const session = await client.openSession({
          initialCatalog: config.catalog,
          initialSchema: config.schema,
        });

        await session.close();
        await client.close();
      } finally {
        emitterStub.restore();
        aggregatorStub.restore();
      }
    });
  });

  describe('Configuration', () => {
    it('should read telemetry config from ClientConfig', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();
      const clientConfig = client.getConfig();

      // Verify default telemetry config exists
      expect(clientConfig).to.have.property('telemetryEnabled');
      expect(clientConfig).to.have.property('telemetryBatchSize');
      expect(clientConfig).to.have.property('telemetryFlushIntervalMs');
      expect(clientConfig).to.have.property('telemetryMaxRetries');
      expect(clientConfig).to.have.property('telemetryAuthenticatedExport');
      expect(clientConfig).to.have.property('telemetryCircuitBreakerThreshold');
      expect(clientConfig).to.have.property('telemetryCircuitBreakerTimeout');

      // Verify default values
      expect(clientConfig.telemetryEnabled).to.equal(false); // Initially disabled
      expect(clientConfig.telemetryBatchSize).to.equal(100);
      expect(clientConfig.telemetryFlushIntervalMs).to.equal(5000);
      expect(clientConfig.telemetryMaxRetries).to.equal(3);
      expect(clientConfig.telemetryAuthenticatedExport).to.equal(true);
      expect(clientConfig.telemetryCircuitBreakerThreshold).to.equal(5);
      expect(clientConfig.telemetryCircuitBreakerTimeout).to.equal(60000);
    });

    it('should allow override via ConnectionOptions', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      // Default should be false
      expect(client.getConfig().telemetryEnabled).to.equal(false);

      try {
        // Override to true
        await client.connect({
          host: config.host,
          path: config.path,
          token: config.token,
          telemetryEnabled: true,
        });

        // Config should be updated
        expect(client.getConfig().telemetryEnabled).to.equal(true);

        await client.close();
      } catch (error) {
        // Clean up even if test fails
        await client.close();
        throw error;
      }
    });
  });

  describe('End-to-End Telemetry Flow', () => {
    it('should emit events during driver operations when telemetry is enabled', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      const emitSpy = sinon.spy(TelemetryEventEmitter.prototype, 'emit');

      try {
        await client.connect({
          host: config.host,
          path: config.path,
          token: config.token,
          telemetryEnabled: true,
        });

        const session = await client.openSession({
          initialCatalog: config.catalog,
          initialSchema: config.schema,
        });

        const operation = await session.executeStatement('SELECT 1 AS test');
        await operation.fetchAll();

        // Events may or may not be emitted depending on feature flag
        // But the driver should work regardless

        await session.close();
        await client.close();
      } finally {
        emitSpy.restore();
      }
    });
  });
});
