import { expect } from 'chai';
import { DBSQLClient } from '../../lib';
import config from './utils/config';

describe('Concurrent result fetching', () => {
  for (const useKernel of [false, true]) {
    for (const options of [{}, { maxRows: 17 }, { maxRows: 17, disableBuffering: true }]) {
      it(`returns each id once across overlapping fetchAll calls (${useKernel ? 'kernel' : 'thrift'}, ${JSON.stringify(
        options,
      )})`, async () => {
        const client = new DBSQLClient();
        try {
          await client.connect({ host: config.host, path: config.path, token: config.token, useKernel } as any);
          const session = await client.openSession();
          try {
            const rowCount = options.maxRows ? 1000 : 100000;
            const operation = await session.executeStatement(
              `SELECT id FROM range(0, ${rowCount}) AS T(id) ORDER BY id`,
            );
            try {
              const chunks = await Promise.all([operation.fetchAll(options), operation.fetchAll(options)]);
              const ids = chunks.flat().map((row: any) => Number(row.id));
              expect(ids.sort((left, right) => left - right)).to.deep.equal(
                Array.from({ length: rowCount }, (unused, index) => index),
              );
              expect(await operation.hasMoreRows()).to.equal(false);
            } finally {
              await operation.close();
            }
          } finally {
            await session.close();
          }
        } finally {
          await client.close();
        }
      });
    }
  }
});
