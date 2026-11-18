import { expect } from 'chai';
import { DBSQLClient } from '../../../lib';
import DBSQLOperation from '../../../lib/DBSQLOperation';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import { ResultFormat } from '../../../lib/contracts/ResultMetadata';
import { tryGetKernelNative } from '../../../lib/kernel/KernelNativeLoader';
import config from '../utils/config';

describe('Concurrent result fetching', () => {
  for (const useKernel of [false, true]) {
    for (const { cloudFetch, options } of [
      { cloudFetch: false, options: {} },
      { cloudFetch: false, options: { maxRows: 17 } },
      { cloudFetch: true, options: {} },
    ]) {
      const delivery = cloudFetch ? 'CloudFetch' : `inline, ${JSON.stringify(options)}`;
      const test = useKernel && !tryGetKernelNative() ? it.skip : it;
      test(`returns each id once across overlapping fetchAll calls (${
        useKernel ? 'kernel' : 'thrift'
      }, ${delivery})`, async () => {
        const messages: string[] = [];
        const client = new DBSQLClient({
          logger: {
            log(level, message) {
              if (level === LogLevel.info) messages.push(message);
            },
          },
        });
        client.getConfig().useCloudFetch = useKernel || cloudFetch;
        try {
          await client.connect({ host: config.host, path: config.path, token: config.token, useKernel } as any);
          const session = await client.openSession();
          try {
            const rowCount = options.maxRows ? 1000 : 100000;
            const query = cloudFetch
              ? `SELECT id, concat(
                  sha2(CAST(id AS STRING), 512), sha2(CAST(id + 1 AS STRING), 512),
                  sha2(CAST(id + 2 AS STRING), 512), sha2(CAST(id + 3 AS STRING), 512)
                ) AS payload FROM range(0, 100000) AS T(id) ORDER BY id`
              : `SELECT id FROM range(0, ${rowCount}) AS T(id) ORDER BY id`;
            const operation = await session.executeStatement(query);
            try {
              const chunks = await Promise.all([operation.fetchAll(options), operation.fetchAll(options)]);
              const ids = chunks.flat().map((row: any) => Number(row.id));
              expect(ids.sort((left, right) => left - right)).to.deep.equal(
                Array.from({ length: rowCount }, (unused, index) => index),
              );
              expect(await operation.hasMoreRows()).to.equal(false);
              if (useKernel) {
                const reader = cloudFetch ? 'CloudFetch' : 'inline Arrow';
                expect(messages.some((message) => message.includes(`Using ${reader} reader:`))).to.equal(true);
              } else if (cloudFetch) {
                const metadata = await (operation as DBSQLOperation).getResultMetadata();
                expect(metadata.resultFormat).to.equal(ResultFormat.UrlBased);
              }
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
