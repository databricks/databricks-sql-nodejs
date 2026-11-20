import { expect } from 'chai';
import { DBSQLClient } from '../../../lib';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../../lib/contracts/InternalConnectionOptions';
import config from './config';

const largeInteger = BigInt('9007199254740993');

export default function rowMaterializationTests(useKernel: boolean) {
  describe(`Arrow row materialization (${useKernel ? 'kernel' : 'Thrift'})`, () => {
    for (const rowMode of ['object', 'array'] as const) {
      for (const maxRows of [1000, 20000]) {
        for (const api of ['fetchAll', 'fetchChunk', 'iterateChunks', 'iterateRows'] as const) {
          it(`preserves chunked rows (${rowMode}, ${api}, maxRows=${maxRows})`, async () => {
            const client = new DBSQLClient();
            try {
              await client.connect({
                host: config.host,
                path: config.path,
                token: config.token,
                useKernel,
              } as ConnectionOptions & InternalConnectionOptions);
              const session = await client.openSession();
              try {
                const operation = await session.executeStatement(
                  'SELECT CAST(id AS INT) AS id, IF(id % 17 = 0, NULL, id % 2 = 0) AS value FROM range(10003) ORDER BY id',
                  { rowMode, maxRows: 127 },
                );
                try {
                  const rows: object[] = [];
                  if (api === 'fetchAll') {
                    rows.push(...(await operation.fetchAll({ maxRows })));
                  } else if (api === 'fetchChunk') {
                    const chunk = await operation.fetchChunk({ maxRows });
                    expect(chunk.length).to.equal(Math.min(maxRows, 10003));
                    rows.push(...chunk, ...(await operation.fetchAll({ maxRows })));
                  } else if (api === 'iterateChunks') {
                    for await (const chunk of operation.iterateChunks({ maxRows })) {
                      expect(chunk.length).to.equal(Math.min(maxRows, 10003 - rows.length));
                      rows.push(...chunk);
                    }
                  } else {
                    for await (const row of operation.iterateRows({ maxRows })) rows.push(row);
                  }
                  expect(rows).to.deep.equal(
                    Array.from({ length: 10003 }, (_, id) => {
                      const value = id % 17 === 0 ? null : id % 2 === 0;
                      return rowMode === 'array' ? [id, value] : { id, value };
                    }),
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
      for (const useArrowNativeTypes of [false, true]) {
        for (const preserveBigNumericPrecision of [false, true]) {
          it(`preserves values (${rowMode}, native=${useArrowNativeTypes}, precision=${preserveBigNumericPrecision})`, async () => {
            const client = new DBSQLClient();
            const base = client.getConfig();
            client.getConfig = () => ({ ...base, useArrowNativeTypes });
            try {
              await client.connect({
                host: config.host,
                path: config.path,
                token: config.token,
                useKernel,
                preserveBigNumericPrecision,
              } as ConnectionOptions & InternalConnectionOptions);
              const session = await client.openSession();
              try {
                const operation = await session.executeStatement(
                  `
                SELECT CAST(id AS INT) AS id,
                  IF(id = 1, NULL, true) AS b,
                  IF(id = 1, NULL, CAST(-7 AS TINYINT)) AS tiny,
                  IF(id = 1, NULL, CAST(300 AS SMALLINT)) AS small,
                  IF(id = 1, NULL, CAST(9007199254740993 AS BIGINT)) AS big,
                  IF(id = 1, NULL, CAST(1.25 AS DECIMAL(30,4))) AS dec,
                  IF(id = 1, NULL, CAST(1.25 AS FLOAT)) AS flt,
                  IF(id = 1, NULL, CAST(-2.5 AS DOUBLE)) AS dbl,
                  IF(id = 1, NULL, 'hello🌍') AS txt,
                  IF(id = 1, NULL, unhex('0080ff')) AS bin,
                  IF(id = 1, NULL, DATE '2026-01-02') AS dt,
                  IF(id = 1, NULL, TIMESTAMP '2026-01-02 03:04:05') AS ts,
                  IF(id = 1, NULL, array(1, NULL, 3)) AS arr,
                  IF(id = 1, NULL, map('a', 1, 'b', NULL)) AS mp,
                  IF(id = 1, NULL, named_struct('items', array(1, NULL), 'text', 'x')) AS st,
                  IF(id = 1, NULL, INTERVAL '1-2' YEAR TO MONTH) AS ym,
                  IF(id = 1, NULL, INTERVAL '1 02:03:04.123456' DAY TO SECOND) AS day_time
                FROM range(3) ORDER BY id
              `,
                  { rowMode },
                );
                try {
                  const precise = preserveBigNumericPrecision;
                  const values = {
                    id: 0,
                    b: true,
                    tiny: -7,
                    small: 300,
                    big: precise ? largeInteger : 9007199254740992,
                    dec: precise ? '1.2500' : 1.25,
                    flt: 1.25,
                    dbl: -2.5,
                    txt: 'hello🌍',
                    bin: Buffer.from([0, 128, 255]),
                    dt: new Date('2026-01-02T00:00:00Z'),
                    ts: useKernel || useArrowNativeTypes ? new Date('2026-01-02T03:04:05Z') : '2026-01-02 03:04:05',
                    arr: [1, null, 3],
                    mp: { a: 1, b: null },
                    st: { items: [1, null], text: 'x' },
                    ym: '1-2',
                    day_time: '1 02:03:04.123456000',
                  };
                  const expected = [
                    values,
                    Object.fromEntries(Object.keys(values).map((key) => [key, key === 'id' ? 1 : null])),
                    { ...values, id: 2 },
                  ];
                  expect(await operation.fetchAll()).to.deep.equal(
                    rowMode === 'array' ? expected.map(Object.values) : expected,
                  );
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
    }
  });
}
