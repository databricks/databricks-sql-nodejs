import { expect } from 'chai';
import sinon from 'sinon';
import { DBSQLClient } from '../../lib';
import config from './utils/config';
import FeatureFlagCache from '../../lib/FeatureFlagCache';

describe('Thrift feature flags across idle reconnects', () => {
  let sharedContexts: sinon.SinonStub;
  let idleContexts: sinon.SinonStub;

  beforeEach(() => {
    sharedContexts = sinon.stub(FeatureFlagCache as any, 'sharedContexts').value(new Map());
    idleContexts = sinon.stub(FeatureFlagCache as any, 'idleContexts').value(new Map());
  });

  afterEach(() => {
    idleContexts.restore();
    sharedContexts.restore();
  });

  for (const reuseClient of [false, true]) {
    it(`reuses flags and preserves results when ${
      reuseClient ? 'reconnecting a client' : 'creating another client'
    }`, async () => {
      const fetch = sinon.spy(FeatureFlagCache.prototype as any, 'fetchFeatureFlags');
      let client = new DBSQLClient();
      let enabled: boolean | undefined;
      try {
        const cycle = async (iteration: number) => {
          if (!reuseClient && iteration > 0) client = new DBSQLClient();
          try {
            await client.connect({
              host: config.host,
              path: config.path,
              token: config.token,
              userAgentEntry: `feature-flag-reconnect-${reuseClient}`,
            });
            if (iteration === 0) enabled = client.getTelemetryStats() !== undefined;
            expect(client.getTelemetryStats() !== undefined).to.equal(enabled);
            const session = await client.openSession();
            try {
              const operation = await session.executeStatement('SELECT 1 AS value, CAST(NULL AS STRING) AS missing');
              try {
                expect(await operation.fetchAll({ maxRows: 1 })).to.deep.equal([{ value: 1, missing: null }]);
              } finally {
                await operation.close();
              }
            } finally {
              await session.close();
            }
          } finally {
            await client.close();
          }
        };
        await cycle(0);
        await cycle(1);
        expect(fetch.callCount).to.equal(1);
      } finally {
        fetch.restore();
      }
    });
  }
});
