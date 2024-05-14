import { expect } from 'chai';
import sinon from 'sinon';
import { DBSQLClient } from '../../lib';
import { ClientConfig } from '../../lib/contracts/IClientContext';
import IDBSQLSession from '../../lib/contracts/IDBSQLSession';

import config from './utils/config';

async function openSession(customConfig: Partial<ClientConfig> = {}) {
  const client = new DBSQLClient();

  const clientConfig = client.getConfig();
  sinon.stub(client, 'getConfig').returns({
    ...clientConfig,
    ...customConfig,
  });

  const connection = await client.connect({
    host: config.host,
    path: config.path,
    token: config.token,
  });

  return connection.openSession({
    initialCatalog: config.catalog,
    initialSchema: config.schema,
  });
}

const execute = async (session: IDBSQLSession, statement: string) => {
  const operation = await session.executeStatement(statement);
  const result = await operation.fetchAll();
  await operation.close();
  return result;
};

function removeTrailingMetadata(columns: Array<any>) {
  const result = [];
  for (let i = 0; i < columns.length; i += 1) {
    const col = columns[i];
    if (col.col_name === '') {
      break;
    }
    result.push(col);
  }
  return result;
}

describe('Data types', () => {
  it('primitive data types should presented correctly', async () => {
    const table = `dbsql_nodejs_sdk_e2e_primitive_types_${config.tableSuffix}`;

    const session = await openSession({ arrowEnabled: false });
    try {
      await execute(session, `DROP TABLE IF EXISTS ${table}`);
      await execute(
        session,
        `
                CREATE TABLE IF NOT EXISTS ${table} (
                    bool
                    boolean,
                    tiny_int
                    tinyint,
                    small_int
                    smallint,
                    int_type
                    int,
                    big_int
                    bigint,
                    flt
                    float,
                    dbl
                    double,
                    dec
                    decimal(3, 2),
                    str string,
                    ts timestamp,
                    bin binary,
                    chr char(10),
                    vchr varchar(10),
                    dat date
                )
            `,
      );

      const columns = await execute(session, `DESCRIBE ${table}`);
      expect(removeTrailingMetadata(columns)).to.be.deep.eq([
        {
          col_name: 'bool',
          data_type: 'boolean',
          comment: null,
        },
        {
          col_name: 'tiny_int',
          data_type: 'tinyint',
          comment: null,
        },
        {
          col_name: 'small_int',
          data_type: 'smallint',
          comment: null,
        },
        {
          col_name: 'int_type',
          data_type: 'int',
          comment: null,
        },
        {
          col_name: 'big_int',
          data_type: 'bigint',
          comment: null,
        },
        {
          col_name: 'flt',
          data_type: 'float',
          comment: null,
        },
        {
          col_name: 'dbl',
          data_type: 'double',
          comment: null,
        },
        {
          col_name: 'dec',
          data_type: 'decimal(3,2)',
          comment: null,
        },
        {
          col_name: 'str',
          data_type: 'string',
          comment: null,
        },
        {
          col_name: 'ts',
          data_type: 'timestamp',
          comment: null,
        },
        {
          col_name: 'bin',
          data_type: 'binary',
          comment: null,
        },
        {
          col_name: 'chr',
          data_type: 'char(10)',
          comment: null,
        },
        {
          col_name: 'vchr',
          data_type: 'varchar(10)',
          comment: null,
        },
        {
          col_name: 'dat',
          data_type: 'date',
          comment: null,
        },
      ]);

      await execute(
        session,
        `
                INSERT INTO ${table} (
                    bool, tiny_int, small_int, int_type, big_int, flt, dbl,
                    dec, str, ts, bin, chr, vchr, dat
                ) VALUES (
                    true, 127, 32000, 4000000, 372036854775807, 1.2, 2.2, 3.2, 'data',
                    '2014-01-17 00:17:13', 'data', 'a', 'b', '2014-01-17'
                )
            `,
      );

      const records = await execute(session, `SELECT * FROM ${table}`);
      expect(records).to.be.deep.eq([
        {
          bool: true,
          tiny_int: 127,
          small_int: 32000,
          int_type: 4000000,
          big_int: 372036854775807,
          flt: 1.2,
          dbl: 2.2,
          dec: 3.2,
          str: 'data',
          ts: '2014-01-17 00:17:13',
          bin: Buffer.from('data'),
          chr: 'a',
          vchr: 'b',
          dat: '2014-01-17',
        },
      ]);
    } finally {
      await execute(session, `DROP TABLE IF EXISTS ${table}`);
      await session.close();
    }
  });

  it('interval types should be presented correctly', async () => {
    const table = `dbsql_nodejs_sdk_e2e_interval_types_${config.tableSuffix}`;

    const session = await openSession({ arrowEnabled: false });
    try {
      await execute(session, `DROP TABLE IF EXISTS ${table}`);
      await execute(
        session,
        `
                CREATE TABLE ${table} AS
                SELECT INTERVAL '1' day AS day_interval, INTERVAL '1' month AS month_interval
            `,
      );

      const columns = await execute(session, `DESCRIBE ${table}`);
      expect(removeTrailingMetadata(columns)).to.be.deep.eq([
        {
          col_name: 'day_interval',
          data_type: 'interval day',
          comment: null,
        },
        {
          col_name: 'month_interval',
          data_type: 'interval month',
          comment: null,
        },
      ]);

      const records = await execute(session, `SELECT * FROM ${table}`);
      expect(records).to.be.deep.eq([
        {
          day_interval: '1 00:00:00.000000000',
          month_interval: '0-1',
        },
      ]);
    } finally {
      await execute(session, `DROP TABLE IF EXISTS ${table}`);
      await session.close();
    }
  });

  it('complex types should be presented correctly', async () => {
    const table = `dbsql_nodejs_sdk_e2e_complex_types_${config.tableSuffix}`;
    const helperTable = `dbsql_nodejs_sdk_e2e_complex_types_helper_${config.tableSuffix}`;

    const session = await openSession({ arrowEnabled: false });
    try {
      await execute(session, `DROP TABLE IF EXISTS ${helperTable}`);
      await execute(session, `DROP TABLE IF EXISTS ${table}`);
      await execute(session, `CREATE TABLE ${helperTable}( id string )`);
      await execute(session, `INSERT INTO ${helperTable} (id) VALUES (1)`);
      await execute(
        session,
        `
                CREATE TABLE ${table} (
                    id int,
                    arr_type array<string>,
                    map_type map<string, int>,
                    struct_type struct<city:string,state:string>
                )
            `,
      );

      const columns = await execute(session, `DESCRIBE ${table}`);
      expect(removeTrailingMetadata(columns)).to.be.deep.eq([
        {
          col_name: 'id',
          data_type: 'int',
          comment: null,
        },
        {
          col_name: 'arr_type',
          data_type: 'array<string>',
          comment: null,
        },
        {
          col_name: 'map_type',
          data_type: 'map<string,int>',
          comment: null,
        },
        {
          col_name: 'struct_type',
          data_type: 'struct<city:string,state:string>',
          comment: null,
        },
      ]);

      await execute(
        session,
        `
                INSERT INTO table ${table} SELECT
                    POSITIVE(1) AS id,
                    array('a', 'b') AS arr_type,
                    map('key', 12) AS map_type,
                    named_struct('city','Tampa','State','FL') AS struct_type
                FROM ${helperTable}
            `,
      );
      await execute(
        session,
        `
                INSERT INTO table ${table} SELECT
                    POSITIVE(2) AS id,
                    array('c', 'd') AS arr_type,
                    map('key2', 12) AS map_type,
                    named_struct('city','Albany','State','NY') AS struct_type
                FROM ${helperTable}
            `,
      );
      await execute(
        session,
        `
                INSERT INTO TABLE ${table} SELECT
                    POSITIVE(3) AS id,
                    array('e', 'd') AS arr_type,
                    map('key2', 13) AS map_type,
                    named_struct('city','Los Angeles','State','CA') AS struct_type
                FROM ${helperTable}
            `,
      );

      const records = await execute(session, `SELECT * FROM ${table} ORDER BY id ASC`);
      expect(records).to.be.deep.eq([
        {
          id: 1,
          arr_type: ['a', 'b'],
          map_type: {
            key: 12,
          },
          struct_type: {
            city: 'Tampa',
            state: 'FL',
          },
        },
        {
          id: 2,
          arr_type: ['c', 'd'],
          map_type: {
            key2: 12,
          },
          struct_type: {
            city: 'Albany',
            state: 'NY',
          },
        },
        {
          id: 3,
          arr_type: ['e', 'd'],
          map_type: {
            key2: 13,
          },
          struct_type: {
            city: 'Los Angeles',
            state: 'CA',
          },
        },
      ]);
    } finally {
      await execute(session, `DROP TABLE IF EXISTS ${table}`);
      await execute(session, `DROP TABLE IF EXISTS ${helperTable}`);
      await session.close();
    }
  });
});
