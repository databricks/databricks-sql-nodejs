const { expect } = require('chai');
const arrow = require('apache-arrow');
const config = require('./utils/config');
const logger = require('./utils/logger')(config.logger);
const { DBSQLClient } = require('../..');
const ArrowResult = require('../../dist/result/ArrowResult').default;

async function openSession() {
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
}

async function execute(session, statement) {
  const operation = await session.executeStatement(statement, { runAsync: true });
  const result = await operation.fetchAll();
  await operation.close();
  return result;
}

async function deleteTable(session, tableName) {
  await execute(session, `DROP TABLE IF EXISTS ${tableName}`);
}

async function initializeTable(session, tableName) {
  await deleteTable(session, tableName);
  await execute(
    session,
    `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      bool boolean,
      tiny_int tinyint,
      small_int smallint,
      int_type int,
      flt float,
      dbl double,
      dec decimal(3,2),
      str string,
      ts timestamp,
      bin binary,
      chr char(10),
      vchr varchar(10),
      dat date,
      arr_type array<date>,
      map_type map<string, date>,
      struct_type struct<a:string,b:date>
    )
  `,
  );
  await execute(
    session,
    `
    INSERT INTO ${tableName} (
      bool,
      tiny_int,
      small_int,
      int_type,
      flt,
      dbl,
      dec,
      str,
      ts,
      bin,
      chr,
      vchr,
      dat,
      arr_type,
      map_type,
      struct_type
    ) VALUES (
      true,
      127,
      32000,
      4000000,
      1.2,
      2.2,
      3.2,
      'data',
      '2014-01-17 00:17:13',
      'data',
      'a',
      'b',
      '2014-01-17',
      array('2014-01-17', '2023-01-22'),
      map('key', '1991-01-01'),
      named_struct('a','Test','b','2023-01-22')
    )
  `,
  );
}

describe('Arrow support', () => {
  const tableName = `dbsql_nodejs_sdk_e2e_arrow_${config.tableSuffix}`;

  function createTest(testBody) {
    return async () => {
      const session = await openSession();
      try {
        await initializeTable(session, tableName);
        await testBody(session);
      } catch (error) {
        logger(error);
        await session.close();
        throw error;
      } finally {
        await deleteTable(session, tableName);
      }
    };
  }

  it(
    'should not use arrow if disabled',
    createTest(async (session) => {
      const operation = await session.executeStatement(`SELECT * FROM ${tableName}`, {
        enableArrow: false,
      });
      const result = await operation.fetchAll();
      expect(result.length).to.eq(1);

      const resultHandler = await operation._schema.getResultHandler();
      expect(resultHandler).to.be.not.instanceof(ArrowResult);

      await operation.close();
    }),
  );

  it(
    'should use arrow without native types',
    createTest(async (session) => {
      const operation = await session.executeStatement(`SELECT * FROM ${tableName}`, {
        enableArrow: true,
        arrowOptions: {
          useNativeTimestamps: false,
          useNativeDecimals: false,
          useNativeComplexTypes: false,
          useNativeIntervalTypes: false,
        },
      });
      const result = await operation.fetchAll();
      expect(result).to.deep.equal([
        {
          bool: true,
          tiny_int: 127,
          small_int: 32000,
          int_type: 4000000,
          flt: 1.2000000476837158,
          dbl: 2.2,
          dec: '3.20',
          str: 'data',
          ts: '2014-01-17 00:17:13',
          bin: new Uint8Array([100, 97, 116, 97]),
          chr: 'a',
          vchr: 'b',
          dat: new Date('2014-01-17T00:00:00.000Z'),
          arr_type: '[2014-01-17,2023-01-22]',
          map_type: '{"key":1991-01-01}',
          struct_type: '{"a":"Test","b":2023-01-22}',
        },
      ]);

      const resultHandler = await operation._schema.getResultHandler();
      expect(resultHandler).to.be.instanceof(ArrowResult);

      await operation.close();
    }),
  );

  it(
    'should use arrow with native timestamps',
    createTest(async (session) => {
      const operation = await session.executeStatement(`SELECT * FROM ${tableName}`, {
        enableArrow: true,
        arrowOptions: {
          useNativeTimestamps: true,
          useNativeDecimals: false,
          useNativeComplexTypes: false,
          useNativeIntervalTypes: false,
        },
      });
      const result = await operation.fetchAll();
      expect(result).to.deep.equal([
        {
          bool: true,
          tiny_int: 127,
          small_int: 32000,
          int_type: 4000000,
          flt: 1.2000000476837158,
          dbl: 2.2,
          dec: '3.20',
          str: 'data',
          ts: 1389917833000, // This field is affected
          bin: new Uint8Array([100, 97, 116, 97]),
          chr: 'a',
          vchr: 'b',
          dat: new Date('2014-01-17T00:00:00.000Z'),
          arr_type: '[2014-01-17,2023-01-22]',
          map_type: '{"key":1991-01-01}',
          struct_type: '{"a":"Test","b":2023-01-22}',
        },
      ]);

      const resultHandler = await operation._schema.getResultHandler();
      expect(resultHandler).to.be.instanceof(ArrowResult);

      await operation.close();
      // TODO: Check result
    }),
  );

  it(
    'should use arrow with native decimals',
    createTest(async (session) => {
      const operation = await session.executeStatement(`SELECT * FROM ${tableName}`, {
        enableArrow: true,
        arrowOptions: {
          useNativeTimestamps: false,
          useNativeDecimals: true,
          useNativeComplexTypes: false,
          useNativeIntervalTypes: false,
        },
      });
      const result = await operation.fetchAll();
      expect(result).to.deep.equal([
        {
          bool: true,
          tiny_int: 127,
          small_int: 32000,
          int_type: 4000000,
          flt: 1.2000000476837158,
          dbl: 2.2,
          dec: new arrow.util.BN(new Uint32Array([320, 0, 0, 0])), // This field is affected
          str: 'data',
          ts: '2014-01-17 00:17:13',
          bin: new Uint8Array([100, 97, 116, 97]),
          chr: 'a',
          vchr: 'b',
          dat: new Date('2014-01-17T00:00:00.000Z'),
          arr_type: '[2014-01-17,2023-01-22]',
          map_type: '{"key":1991-01-01}',
          struct_type: '{"a":"Test","b":2023-01-22}',
        },
      ]);

      const resultHandler = await operation._schema.getResultHandler();
      expect(resultHandler).to.be.instanceof(ArrowResult);

      await operation.close();
      // TODO: Check result
    }),
  );

  it(
    'should use arrow with native complex types',
    createTest(async (session) => {
      const operation = await session.executeStatement(`SELECT * FROM ${tableName}`, {
        enableArrow: true,
        arrowOptions: {
          useNativeTimestamps: false,
          useNativeDecimals: false,
          useNativeComplexTypes: true,
          useNativeIntervalTypes: false,
        },
      });
      const result = await operation.fetchAll();

      expect(
        result.map((item) => ({
          ...item,
          arr_type: item.arr_type.toJSON(),
          map_type: item.map_type.toJSON(),
          struct_type: item.struct_type.toJSON(),
        })),
      ).to.deep.equal([
        {
          bool: true,
          tiny_int: 127,
          small_int: 32000,
          int_type: 4000000,
          flt: 1.2000000476837158,
          dbl: 2.2,
          dec: '3.20',
          str: 'data',
          ts: '2014-01-17 00:17:13',
          bin: new Uint8Array([100, 97, 116, 97]),
          chr: 'a',
          vchr: 'b',
          dat: new Date('2014-01-17T00:00:00.000Z'),
          arr_type: [new Date('2014-01-17'), new Date('2023-01-22')],
          map_type: { key: new Date('1991-01-01') },
          struct_type: { a: 'Test', b: new Date('2023-01-22') },
        },
      ]);

      const resultHandler = await operation._schema.getResultHandler();
      expect(resultHandler).to.be.instanceof(ArrowResult);

      await operation.close();
      // TODO: Check result
    }),
  );
});
