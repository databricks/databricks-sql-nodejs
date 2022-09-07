const { DBSQLClient, utils } = require('../');

const client = new DBSQLClient();

const host = '****.databricks.com';
const path = '/sql/1.0/endpoints/****';
const token = 'dapi********************************';

client.connect({ host, path, token }).then(async (client) => {
  try {
    client.on('error', (error) => {
      console.error(error);
    });

    const session = await client.openSession();

    await testPrimitiveTypes(session);
    await testComplexTypes(session);
    await testIntervals(session);

    const status = await session.close();
    console.log(status.success());
    await client.close();
  } catch (error) {
    console.error(error);
    await client.close();
  }
});

const testPrimitiveTypes = async (session) => {
  try {
    console.log('[info] create primitiveTypes');
    await execute(
      session,
      `
            CREATE TABLE IF NOT EXISTS primitiveTypes (
                bool boolean,
                tiny_int tinyint,
                small_int smallint,
                int_type int,
                big_int bigint,
                flt float,
                dbl double,
                dec decimal(3,2),
                str string,
                ts timestamp,
                bin binary,
                chr char(10),
                vchr varchar(10),
                dat date
            )`,
    );
    console.log('[info] insert into primitiveTypes');
    await execute(
      session,
      `INSERT INTO primitiveTypes (
            bool,
            tiny_int,
            small_int,
            int_type,
            big_int,
            flt,
            dbl,
            dec,
            str,
            ts,
            bin,
            chr,
            vchr,
            dat
        ) VALUES (
            true,
            127,
            32000,
            4000000,
            372036854775807,
            1.2,
            2.2,
            3.2,
            'data',
            '2014-01-17 00:17:13',
            'data',
            'a',
            'b',
            '2014-01-17'
        )`,
    );
    console.log('[info] fetch primitiveTypes');
    const result = await execute(session, 'SELECT * FROM primitiveTypes');

    console.log(result);
  } finally {
    await execute(session, 'DROP TABLE primitiveTypes');
  }
};

const testIntervals = async (session) => {
  try {
    console.log('[info] create intervalTypes');
    await execute(
      session,
      `
            CREATE TABLE IF NOT EXISTS intervalTypes AS
                SELECT INTERVAL '1' day AS day_interval, 
                INTERVAL '1' month AS month_interval
        `,
    );
    console.log('[info] describe intervalTypes');
    console.log(await execute(session, `DESC intervalTypes`));
    console.log('[info] fetch intervalTypes');
    console.log(await execute(session, 'SELECT * FROM intervalTypes'));
  } finally {
    await execute(session, 'DROP TABLE intervalTypes');
  }
};

const testComplexTypes = async (session) => {
  try {
    console.log('[info] create dummy');
    await execute(session, `DROP TABLE IF EXISTS dummy`);
    await execute(session, `CREATE TABLE dummy( id STRING )`);
    console.log('[info] insert dummy value');
    await execute(session, `INSERT INTO dummy (id) VALUES (1)`);
    console.log('[info] create complexTypes');
    await execute(
      session,
      `
            CREATE TABLE complexTypes (
                arr_type array<string>,
                map_type map<string, int>,
                struct_type struct<city:string,State:string>
            )
        `,
    );
    console.log('[info] 1. insert complexTypes');
    await execute(
      session,
      `
            INSERT INTO TABLE complexTypes SELECT
                array('a', 'b') AS arr_type,
                map('key', 12) AS map_type,
                named_struct('city','Tampa','State','FL') AS struct_type
            FROM dummy
        `,
    );
    console.log('[info] 2. insert complexTypes');
    await execute(
      session,
      `
            INSERT INTO TABLE complexTypes SELECT
                array('c', 'd') AS arr_type,
                map('key2', 12) AS map_type,
                named_struct('city','Albany','State','NY') AS struct_type
            FROM dummy
        `,
    );
    console.log('[info] 3. insert complexTypes');
    await execute(
      session,
      `
            INSERT INTO TABLE complexTypes SELECT
                array('e', 'd') AS arr_type,
                map('key2', 13) AS map_type,
                named_struct('city','Los Angeles','State','CA') AS struct_type
            FROM dummy
        `,
    );
    console.log('[info] fetch complexTypes');
    console.log(await execute(session, 'SELECT * FROM complexTypes'));
  } finally {
    await Promise.all([execute(session, 'DROP TABLE dummy'), execute(session, 'DROP TABLE complexTypes')]);
  }
};

const execute = async (session, statement) => {
  const operation = await session.executeStatement(statement, { runAsync: true });

  const result = await operation.fetchAll({
    progress: true,
    callback: (stateResponse) => {
      return;
      if (stateResponse.taskStatus) {
        console.log(stateResponse.taskStatus);
      } else {
        console.log(utils.formatProgress(stateResponse.progressUpdateResponse));
      }
    },
  });

  await operation.close();

  return result;
};
