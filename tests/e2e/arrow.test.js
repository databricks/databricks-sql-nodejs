const { expect } = require('chai');
const sinon = require('sinon');
const config = require('./utils/config');
const logger = require('./utils/logger')(config.logger);
const { DBSQLClient } = require('../..');
const ArrowResultHandler = require('../../dist/result/ArrowResultHandler').default;
const ResultSlicer = require('../../dist/result/ResultSlicer').default;
const globalConfig = require('../../dist/globalConfig').default;

const fixtures = require('../fixtures/compatibility');
const { expected: expectedColumn } = require('../fixtures/compatibility/column');
const { expected: expectedArrow } = require('../fixtures/compatibility/arrow');
const { expected: expectedArrowNativeTypes } = require('../fixtures/compatibility/arrow_native_types');
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
  const operation = await session.executeStatement(statement);
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
        throw error;
      } finally {
        await deleteTable(session, tableName);
        await session.close();
      }
    };
  }

  it(
    'should not use arrow if disabled',
    createTest(async (session) => {
      globalConfig.arrowEnabled = false;

      const operation = await session.executeStatement(`SELECT * FROM ${tableName}`);
      const result = await operation.fetchAll();
      expect(result).to.deep.equal(expectedColumn);

      const resultHandler = await operation.getResultHandler();
      expect(resultHandler).to.be.instanceof(ResultSlicer);
      expect(resultHandler.source).to.be.not.instanceof(ArrowResultHandler);

      await operation.close();
    }),
  );

  it(
    'should use arrow with native types disabled',
    createTest(async (session) => {
      globalConfig.arrowEnabled = true;
      globalConfig.useArrowNativeTypes = false;

      const operation = await session.executeStatement(`SELECT * FROM ${tableName}`);
      const result = await operation.fetchAll();
      expect(fixArrowResult(result)).to.deep.equal(expectedArrow);

      const resultHandler = await operation.getResultHandler();
      expect(resultHandler).to.be.instanceof(ResultSlicer);
      expect(resultHandler.source).to.be.instanceof(ArrowResultHandler);

      await operation.close();
    }),
  );

  it(
    'should use arrow with native types enabled',
    createTest(async (session) => {
      globalConfig.arrowEnabled = true;
      globalConfig.useArrowNativeTypes = true;

      const operation = await session.executeStatement(`SELECT * FROM ${tableName}`);
      const result = await operation.fetchAll();
      expect(fixArrowResult(result)).to.deep.equal(expectedArrowNativeTypes);

      const resultHandler = await operation.getResultHandler();
      expect(resultHandler).to.be.instanceof(ResultSlicer);
      expect(resultHandler.source).to.be.instanceof(ArrowResultHandler);

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
    const resultHandler = await operation.getResultHandler();
    expect(resultHandler).to.be.instanceof(ResultSlicer);
    expect(resultHandler.source).to.be.instanceof(ArrowResultHandler);

    sinon.spy(operation._data, 'fetchNext');

    const result = await resultHandler.fetchNext({ limit: rowsCount });

    expect(operation._data.fetchNext.callCount).to.be.eq(1);
    const rawData = await operation._data.fetchNext.firstCall.returnValue;
    // We don't know exact count of batches returned, it depends on server's configuration,
    // but with much enough rows there should be more than one result batch
    expect(rawData.arrowBatches?.length).to.be.gt(1);

    expect(result.length).to.be.eq(rowsCount);
  });
});
