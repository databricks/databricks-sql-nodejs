import { expect } from 'chai';
import { DBSQLClient } from '../../../lib';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import config from '../utils/config';

describe('client close telemetry lifecycle', function closeSuite() {
  this.timeout(60000);

  for (const useKernel of [false, true]) {
    for (const waitForTelemetry of [true, false]) {
      it(`${
        useKernel ? 'kernel' : 'Thrift'
      } closes and reopens with waitForTelemetry=${waitForTelemetry}`, async () => {
        let errors = 0;
        const client = new DBSQLClient({
          logger: {
            log(level) {
              if (level === LogLevel.error) errors += 1;
            },
          },
        });
        try {
          const cycle = async (): Promise<void> => {
            await client.connect({ host: config.host, path: config.path, token: config.token, ...{ useKernel } });
            if (!useKernel) expect(client.getTelemetryStats()).not.to.equal(undefined);
            const session = await client.openSession();
            const operation = await session.executeStatement(
              'SELECT id, CAST(NULL AS STRING) AS missing, id % 2 = 0 AS even FROM range(12) ORDER BY id LIMIT 3',
              { maxRows: 2 },
            );
            const rows = (await operation.fetchAll({ maxRows: 2 })) as Array<Record<string, unknown>>;
            expect(rows.map((row) => ({ ...row, id: Number(row.id) }))).to.deep.equal([
              { id: 0, missing: null, even: true },
              { id: 1, missing: null, even: false },
              { id: 2, missing: null, even: true },
            ]);
            await operation.close();
            const early = await session.executeStatement('SELECT id FROM range(100) ORDER BY id', { maxRows: 1 });
            await early.fetchChunk({ maxRows: 1 });
            await early.close();
            const cancelled = await session.executeStatement('SELECT id FROM range(100)', { runAsync: true });
            await cancelled.cancel();
            await cancelled.close();
            await client.close({ waitForTelemetry });
          };
          await cycle();
          await cycle();
          await cycle();
          expect(errors).to.equal(0);
        } finally {
          await client.close();
        }
      });
    }
  }
});
