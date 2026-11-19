import { expect } from 'chai';
import { channel } from 'diagnostics_channel';
import { DBSQLClient } from '../../lib';
import config from './utils/config';
import FeatureFlagCache from '../../lib/FeatureFlagCache';

describe('Thrift feature flags across idle reconnects', () => {
  beforeEach(() => {
    (FeatureFlagCache as any).idleContexts.clear();
  });

  for (const reuseClient of [false, true]) {
    it(`reuses flags and preserves results when ${
      reuseClient ? 'reconnecting a client' : 'creating another client'
    }`, async () => {
      let requests = 0;
      const onRequest = ({ request }: any) => {
        if (request.method === 'GET' && request.path.includes('/connector-service/feature-flags/NODEJS/')) {
          requests += 1;
        }
      };
      const requestsChannel = channel('http.client.request.start');
      requestsChannel.subscribe(onRequest);
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
        expect(requests).to.equal(1);
      } finally {
        requestsChannel.unsubscribe(onRequest);
      }
    });
  }
});
