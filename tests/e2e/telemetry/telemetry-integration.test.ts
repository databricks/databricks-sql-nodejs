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
import FeatureFlagCache from '../../../lib/telemetry/FeatureFlagCache';
import TelemetryClientProvider from '../../../lib/telemetry/TelemetryClientProvider';
import TelemetryEventEmitter from '../../../lib/telemetry/TelemetryEventEmitter';
import MetricsAggregator from '../../../lib/telemetry/MetricsAggregator';

// Load test config with a skip-guard if e2e credentials are not present.
// The shared `../utils/config` runs process.exit(1) on missing env vars at
// import time, so we cannot use it directly without breaking unconfigured
// CI runs. Read env vars directly here and skip the suite if any are missing.
interface TestConfig {
  host: string;
  path: string;
  token: string;
  catalog: string;
  schema: string;
}

function loadConfigOrSkip(suite: Mocha.Suite): TestConfig | null {
  // Loading overrides from `config.local` first matches the precedence used
  // by the shared `../utils/config` loader, so engineers who have a local
  // override file get the same behavior here.
  let overrides: Record<string, string | undefined> = {};
  try {
    // eslint-disable-next-line global-require, @typescript-eslint/no-var-requires
    const loaded = require('../utils/config.local');
    if (typeof loaded === 'object' && loaded !== null) {
      overrides = loaded as Record<string, string | undefined>;
    }
  } catch {
    // no local overrides; rely on env vars
  }

  const cfg = {
    host: overrides.host ?? process.env.E2E_HOST,
    path: overrides.path ?? process.env.E2E_PATH,
    token: overrides.token ?? process.env.E2E_ACCESS_TOKEN,
    catalog: overrides.catalog ?? process.env.E2E_CATALOG,
    schema: overrides.schema ?? process.env.E2E_SCHEMA,
  };
  const missing = Object.entries(cfg)
    .filter(([, v]) => v === undefined || v === '')
    .map(([k]) => k);
  if (missing.length > 0) {
    // eslint-disable-next-line no-console
    console.warn(
      `[telemetry-integration] Skipping suite: missing E2E config [${missing.join(', ')}]. ` +
        `Set E2E_HOST/E2E_PATH/E2E_ACCESS_TOKEN/E2E_CATALOG/E2E_SCHEMA or create tests/e2e/utils/config.local.{ts,js}.`,
    );
    suite.pending = true;
    return null;
  }
  return cfg as TestConfig;
}

describe('Telemetry Integration', () => {
  let config: TestConfig | null = null;

  // The e2e test runner executes many suites before this one; any earlier
  // DBSQLClient connect leaves a TelemetryClient in the process-wide singleton
  // for the test host. Reset before our first test so the FF cache and
  // refcount spies in this suite observe a clean lineage.
  before(function () {
    config = loadConfigOrSkip(this.test!.parent!);
    if (!config) {
      this.skip();
      return;
    }
    TelemetryClientProvider.__resetInstanceForTests();
  });

  // Reset the process-wide singleton between tests so refcount + cached
  // feature flags from one test don't leak into the next. Combined with the
  // `before` reset above, every test sees a fresh provider regardless of
  // what other e2e suites did first.
  beforeEach(() => {
    TelemetryClientProvider.__resetInstanceForTests();
  });

  afterEach(() => {
    TelemetryClientProvider.__resetInstanceForTests();
    sinon.restore();
  });

  describe('Initialization', () => {
    it('should initialize telemetry when telemetryEnabled is true', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      // Spy on the per-host telemetry provider AND the feature-flag cache.
      // Both should fire on a telemetry-enabled connect; asserting on both
      // guards against a future refactor that bypasses one but not the other.
      const featureFlagCacheSpy = sinon.spy(FeatureFlagCache.prototype, 'getOrCreateContext');
      const telemetryProviderSpy = sinon.spy(TelemetryClientProvider.prototype, 'getOrCreateClient');

      try {
        await client.connect({
          host: config!.host,
          path: config!.path,
          token: config!.token,
          telemetryEnabled: true,
        });

        expect(featureFlagCacheSpy.called).to.be.true;
        expect(telemetryProviderSpy.callCount).to.equal(1);
        // The host the provider was invoked with should match the connect host.
        const hostArg = telemetryProviderSpy.firstCall.args[1];
        expect(hostArg).to.equal(config!.host);

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
          host: config!.host,
          path: config!.path,
          token: config!.token,
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
          host: config!.host,
          path: config!.path,
          token: config!.token,
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
        await client1.connect({
          host: config!.host,
          path: config!.path,
          token: config!.token,
          telemetryEnabled: true,
        });
        await client2.connect({
          host: config!.host,
          path: config!.path,
          token: config!.token,
          telemetryEnabled: true,
        });

        // Each connect calls getOrCreateClient exactly once when the feature
        // flag is on (no FF-disabled release path). Tightened from
        // `at.least(2)` to `equal(2)` — an off-by-one refcount leak from a
        // reconnect path would now fail the test.
        expect(getOrCreateClientSpy.callCount).to.equal(2);
        // Both calls must target the same host so the singleton actually shares.
        const host1 = getOrCreateClientSpy.firstCall.args[1];
        const host2 = getOrCreateClientSpy.secondCall.args[1];
        expect(host1).to.equal(host2);

        await client1.close();
        expect(releaseClientSpy.callCount).to.equal(1);

        await client2.close();
        expect(releaseClientSpy.callCount).to.equal(2);
      } finally {
        getOrCreateClientSpy.restore();
        releaseClientSpy.restore();
      }
    });

    it('should cleanup telemetry on close', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      const releaseClientSpy = sinon.spy(TelemetryClientProvider.prototype, 'releaseClient');
      const aggregatorCloseSpy = sinon.spy(MetricsAggregator.prototype, 'close');

      try {
        await client.connect({
          host: config!.host,
          path: config!.path,
          token: config!.token,
          telemetryEnabled: true,
        });

        await client.close();

        // releaseClient is the refcount surface; MetricsAggregator.close is
        // the cleanup the last refcount holder triggers. Both must fire on a
        // clean close. We do NOT assert flush() was called because this test
        // never `openSession`s, so the pending-metrics buffer is empty and
        // the close-drain pattern legitimately skips the final flush. The
        // previous disjunction (`releaseClient || flush || releaseContext`)
        // over multiple spies meant a regression breaking one would still
        // pass — these explicit asserts catch that.
        expect(releaseClientSpy.called, 'releaseClient should be called on close').to.be.true;
        expect(aggregatorCloseSpy.called, 'MetricsAggregator.close should run on close').to.be.true;
      } finally {
        releaseClientSpy.restore();
        aggregatorCloseSpy.restore();
      }
    });
  });

  describe('Error Handling', () => {
    it('should continue driver operation when telemetry initialization fails', async function () {
      this.timeout(30000);

      const client = new DBSQLClient();

      // Stub feature flag to throw an error
      const featureFlagStub = sinon
        .stub(FeatureFlagCache.prototype, 'isTelemetryEnabled')
        .rejects(new Error('Feature flag fetch failed'));

      try {
        // Connection should succeed even if telemetry fails
        await client.connect({
          host: config!.host,
          path: config!.path,
          token: config!.token,
          telemetryEnabled: true,
        });

        // Should be able to open a session
        const session = await client.openSession({
          initialCatalog: config!.catalog,
          initialSchema: config!.schema,
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
      const contextStub = sinon
        .stub(FeatureFlagCache.prototype, 'getOrCreateContext')
        .throws(new Error('Context creation failed'));

      try {
        // Connection should succeed even if telemetry fails
        await client.connect({
          host: config!.host,
          path: config!.path,
          token: config!.token,
          telemetryEnabled: true,
        });

        // Should be able to open a session
        const session = await client.openSession({
          initialCatalog: config!.catalog,
          initialSchema: config!.schema,
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
      const emitterStub = sinon
        .stub(TelemetryEventEmitter.prototype, 'emitConnectionOpen')
        .throws(new Error('Emitter failed'));
      const aggregatorStub = sinon
        .stub(MetricsAggregator.prototype, 'processEvent')
        .throws(new Error('Aggregator failed'));

      try {
        // Connection should not throw
        await client.connect({
          host: config!.host,
          path: config!.path,
          token: config!.token,
          telemetryEnabled: true,
        });

        // Driver operations should work normally
        const session = await client.openSession({
          initialCatalog: config!.catalog,
          initialSchema: config!.schema,
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
    it('should read telemetry config from ClientConfig', function () {
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

      // Verify default values. telemetryEnabled defaults to true (gated by
      // remote feature flag and DATABRICKS_TELEMETRY_DISABLED env var).
      expect(clientConfig.telemetryEnabled).to.equal(true);
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

      // Default is true; verify explicit override to false.
      expect(client.getConfig().telemetryEnabled).to.equal(true);

      try {
        await client.connect({
          host: config!.host,
          path: config!.path,
          token: config!.token,
          telemetryEnabled: false,
        });

        // Config should reflect the override
        expect(client.getConfig().telemetryEnabled).to.equal(false);

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
          host: config!.host,
          path: config!.path,
          token: config!.token,
          telemetryEnabled: true,
        });

        const session = await client.openSession({
          initialCatalog: config!.catalog,
          initialSchema: config!.schema,
        });

        const operation = await session.executeStatement('SELECT 1 AS test');
        await operation.fetchAll();

        // If the feature flag is on, the emitter MUST have produced at least
        // CONNECTION_OPEN + STATEMENT_START events by now. We don't assert
        // call count because the feature flag is server-controlled — but if
        // any events fired, the typed event-name argument should match one
        // of the telemetry event types.
        if (emitSpy.called) {
          const validEventTypes = new Set([
            'connection.open',
            'connection.close',
            'statement.start',
            'statement.complete',
            'cloudfetch.chunk',
            'telemetry.error',
          ]);
          for (const call of emitSpy.getCalls()) {
            const eventName = String(call.args[0]);
            expect(validEventTypes.has(eventName), `Unexpected event name: ${eventName}`).to.be.true;
          }
        }

        await session.close();
        await client.close();
      } finally {
        emitSpy.restore();
      }
    });
  });
});
