const { expect } = require('chai');
const config = require('./utils/config');
const logger = require('./utils/logger')(config.logger);
const { DBSQLClient } = require('../..');
const ArrowResult = require('../../dist/result/ArrowResult').default;
const globalConfig = require('../../dist/globalConfig').default;

const fixtures = require('../fixtures/compatibility');
const { fixArrowResult } = fixtures;

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

  const createTable = fixtures.createTableSql.replaceAll('${table_name}', tableName);
  await execute(session, createTable);

  const insertData = fixtures.insertDataSql.replaceAll('${table_name}', tableName);
  await execute(session, insertData);
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
      globalConfig.arrowEnabled = false;

      const operation = await session.executeStatement(`SELECT * FROM ${tableName}`);
      const result = await operation.fetchAll();
      expect(result).to.deep.equal(fixtures.expected);

      const resultHandler = await operation._schema.getResultHandler();
      expect(resultHandler).to.be.not.instanceof(ArrowResult);

      await operation.close();
    }),
  );

  it(
    'should use arrow with native types disabled',
    createTest(async (session) => {
      globalConfig.arrowEnabled = true;

      const operation = await session.executeStatement(`SELECT * FROM ${tableName}`, {
        useArrowNativeTypes: false,
      });
      const result = await operation.fetchAll();
      expect(fixArrowResult(result)).to.deep.equal(fixtures.expected);

      const resultHandler = await operation._schema.getResultHandler();
      expect(resultHandler).to.be.instanceof(ArrowResult);

      await operation.close();
    }),
  );

  it(
    'should use arrow with native types enabled',
    createTest(async (session) => {
      globalConfig.arrowEnabled = true;

      const operation = await session.executeStatement(`SELECT * FROM ${tableName}`, {
        useArrowNativeTypes: true,
      });
      const result = await operation.fetchAll();
      expect(fixArrowResult(result)).to.deep.equal(fixtures.expected);

      const resultHandler = await operation._schema.getResultHandler();
      expect(resultHandler).to.be.instanceof(ArrowResult);

      await operation.close();
    }),
  );

  it('should handle multiple batches in response', async () => {
    globalConfig.arrowEnabled = true;

    const rowsCount = 10000;

    const session = await openSession();
    const operation = await session.executeStatement(`
      SELECT *
      FROM range(0, ${rowsCount}) AS t1
      LEFT JOIN (SELECT 1) AS t2
    `);

    // We use some internals here to check that server returned response with multiple batches
    const resultHandler = await operation._schema.getResultHandler();
    expect(resultHandler).to.be.instanceof(ArrowResult);

    const rawData = await operation._data.fetch(rowsCount);
    // We don't know exact count of batches returned, it depends on server's configuration,
    // but with much enough rows there should be more than one result batch
    expect(rawData.arrowBatches?.length).to.be.gt(1);

    const result = resultHandler.getValue([rawData]);
    expect(result.length).to.be.eq(rowsCount);
  });
});
