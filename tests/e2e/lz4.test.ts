import { createHash } from 'crypto';
import { expect } from 'chai';
import { DBSQLClient } from '../../lib';
import CloudFetchResultHandler from '../../lib/result/CloudFetchResultHandler';
import IOperation from '../../lib/contracts/IOperation';
import config from './utils/config';

describe('LZ4 result parity', () => {
  for (const useCloudFetch of [false, true]) {
    it(`preserves every value with CloudFetch=${useCloudFetch}`, async () => {
      const client = new DBSQLClient();
      await client.connect({ host: config.host, path: config.path, token: config.token });
      let operation: IOperation | undefined;
      const session = await client.openSession();
      try {
        const read = async (useLZ4Compression: boolean) => {
          operation = await session.executeStatement(
            `SELECT id, IF(id % 7 = 0, NULL, id * 0.125) AS value,
              IF(id % 5 = 0, NULL, sha2(CAST(id AS STRING), 512)) AS text,
              IF(id % 3 = 0, NULL, id % 2 = 0) AS flag
              FROM range(500000) ORDER BY id`,
            { useCloudFetch, useLZ4Compression, maxRows: null },
          );
          await operation.finished();
          const handler = await (operation as any).getResultHandler();
          expect(handler.source.source.isLZ4Compressed).to.equal(useLZ4Compression);
          expect(handler.source.source instanceof CloudFetchResultHandler).to.equal(useCloudFetch);
          const hash = createHash('sha256');
          let count = 0;
          for await (const chunk of operation.iterateChunks({ maxRows: 11111 })) {
            for (const row of chunk as Array<{
              id: number;
              value: number | null;
              text: string | null;
              flag: boolean | null;
            }>) {
              expect(row.id).to.equal(count);
              expect(row.value).to.equal(count % 7 === 0 ? null : count * 0.125);
              expect(row.text).to.equal(
                count % 5 === 0 ? null : createHash('sha512').update(String(count)).digest('hex'),
              );
              expect(row.flag).to.equal(count % 3 === 0 ? null : count % 2 === 0);
              hash.update(JSON.stringify(row));
              count += 1;
            }
          }
          expect(count).to.equal(500000);
          await operation.close();
          operation = undefined;
          return hash.digest('hex');
        };
        const uncompressed = await read(false);
        expect(await read(true)).to.equal(uncompressed);
      } finally {
        await operation?.close();
        await session.close();
        await client.close();
      }
    });
  }
});
