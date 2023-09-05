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
      :p_str AS col_str
    `,
      {
        runAsync: true,
        namedParameters: {
          p_bool: new DBSQLParameter({ value: true }),
          p_int: new DBSQLParameter({ value: 1234 }),
          p_double: new DBSQLParameter({ value: 3.14 }),
          p_bigint_1: new DBSQLParameter({ value: BigInt(1234) }),
          p_bigint_2: new DBSQLParameter({ value: new Int64(1234) }),
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
        col_str: 'Hello',
      },
    ]);
  });
});
