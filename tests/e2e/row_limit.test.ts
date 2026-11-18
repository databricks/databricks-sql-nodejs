import { expect } from 'chai';
import sinon from 'sinon';
import { after, before, describe, it } from 'mocha';
import { DBSQLClient } from '../../lib';
import { ConnectionOptions } from '../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../lib/contracts/InternalConnectionOptions';
import JsonResultHandler from '../../lib/result/JsonResultHandler';
import ArrowResultConverter from '../../lib/result/ArrowResultConverter';
import config from './utils/config';

describe('rowLimit', () => {
  for (const useKernel of [false, true]) {
    for (const arrowEnabled of useKernel ? [true] : [false, true]) {
      describe(`${useKernel ? 'kernel' : 'Thrift'} (arrowEnabled=${arrowEnabled})`, () => {
        const client = new DBSQLClient();

        before(async () => {
          const clientConfig = client.getConfig();
          sinon.stub(client, 'getConfig').returns({ ...clientConfig, arrowEnabled });
          await client.connect({
            host: config.host,
            path: config.path,
            token: config.token,
            useKernel,
          } as ConnectionOptions & InternalConnectionOptions);
        });

        after(async () => {
          await client.close();
        });

        for (const disableBuffering of [false, true]) {
          for (const rowLimit of [0, 3, 20, 25]) {
            it(`caps ${rowLimit} rows across fetches (disableBuffering=${disableBuffering})`, async () => {
              const session = await client.openSession();
              try {
                const operation = await session.executeStatement('SELECT id FROM RANGE(20) ORDER BY id', {
                  rowLimit,
                  maxRows: disableBuffering ? null : 2,
                  useCloudFetch: false,
                });
                try {
                  await operation.finished();
                  if (!useKernel) {
                    // @ts-expect-error Inspect the real negotiated format, not just the requested config.
                    const handler = await operation.getResultHandler();
                    expect(handler.source).to.be.instanceOf(arrowEnabled ? ArrowResultConverter : JsonResultHandler);
                  }
                  const rows: number[] = [];
                  // eslint-disable-next-line no-await-in-loop
                  while (await operation.hasMoreRows()) {
                    // eslint-disable-next-line no-await-in-loop
                    const chunk = await operation.fetchChunk({ maxRows: 2, disableBuffering });
                    rows.push(...chunk.map((row: any) => Number(row.id)));
                  }
                  expect(rows).to.deep.equal(Array.from({ length: Math.min(rowLimit, 20) }, (_, index) => index));
                  expect(await operation.hasMoreRows()).to.be.false;
                  expect(await operation.fetchChunk({ maxRows: 2, disableBuffering })).to.deep.equal([]);
                  expect(await operation.fetchAll()).to.deep.equal([]);
                } finally {
                  await operation.close();
                }
              } finally {
                await session.close();
              }
            });
          }
        }
      });
    }
  }
});
