const { expect } = require('chai');
const Int64 = require('node-int64');
const config = require('./utils/config');
const { DBSQLClient, DBSQLParameter } = require('../..');

const openSession = async () => {
  const client = new DBSQLClient();

  const connection = await client.connect({
    host: config.host,
    path: config.path,
    token: config.token,
  });

  return connection.openSession({
    initialCatalog: config.database[0],
    initialSchema: config.database[1],
  });
};

describe('Query parameters', () => {
  it('should use named parameters', async () => {
    const session = await openSession();
    const operation = await session.executeStatement(
      `
        SELECT
          :p_bool AS col_bool,
          :p_int AS col_int,
          :p_double AS col_double,
          :p_bigint_1 AS col_bigint_1,
          :p_bigint_2 AS col_bigint_2,
          :p_date as col_date,
          :p_timestamp as col_timestamp,
          :p_str AS col_str
      `,
      {
        namedParameters: {
          p_bool: new DBSQLParameter({ value: true }),
          p_int: new DBSQLParameter({ value: 1234 }),
          p_double: new DBSQLParameter({ value: 3.14 }),
          p_bigint_1: new DBSQLParameter({ value: BigInt(1234) }),
          p_bigint_2: new DBSQLParameter({ value: new Int64(1234) }),
          p_date: new DBSQLParameter({ value: new Date('2023-09-06T03:14:27.843Z'), type: 'DATE' }),
          p_timestamp: new DBSQLParameter({ value: new Date('2023-09-06T03:14:27.843Z') }),
          p_str: new DBSQLParameter({ value: 'Hello' }),
        },
      },
    );
    const result = await operation.fetchAll();
    expect(result).to.deep.equal([
      {
        col_bool: true,
        col_int: 1234,
        col_double: 3.14,
        col_bigint_1: 1234,
        col_bigint_2: 1234,
        col_date: new Date('2023-09-06T00:00:00.000Z'),
        col_timestamp: new Date('2023-09-06T03:14:27.843Z'),
        col_str: 'Hello',
      },
    ]);
  });

  it('should accept primitives as values for named parameters', async () => {
    const session = await openSession();
    const operation = await session.executeStatement(
      `
        SELECT
          :p_bool AS col_bool,
          :p_int AS col_int,
          :p_double AS col_double,
          :p_bigint_1 AS col_bigint_1,
          :p_bigint_2 AS col_bigint_2,
          :p_timestamp as col_timestamp,
          :p_str AS col_str
      `,
      {
        namedParameters: {
          p_bool: true,
          p_int: 1234,
          p_double: 3.14,
          p_bigint_1: BigInt(1234),
          p_bigint_2: new Int64(1234),
          p_timestamp: new Date('2023-09-06T03:14:27.843Z'),
          p_str: 'Hello',
        },
      },
    );
    const result = await operation.fetchAll();
    expect(result).to.deep.equal([
      {
        col_bool: true,
        col_int: 1234,
        col_double: 3.14,
        col_bigint_1: 1234,
        col_bigint_2: 1234,
        col_timestamp: new Date('2023-09-06T03:14:27.843Z'),
        col_str: 'Hello',
      },
    ]);
  });
});
