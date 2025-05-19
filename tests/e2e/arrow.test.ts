import { expect } from 'chai';
import sinon from 'sinon';
import { DBSQLClient } from '../../lib';
import { ClientConfig } from '../../lib/contracts/IClientContext';
import IDBSQLSession from '../../lib/contracts/IDBSQLSession';
import ArrowResultHandler from '../../lib/result/ArrowResultHandler';
import ArrowResultConverter from '../../lib/result/ArrowResultConverter';
import ResultSlicer from '../../lib/result/ResultSlicer';

import config from './utils/config';

const fixtures = require('../fixtures/compatibility');
const { expected: expectedColumn } = require('../fixtures/compatibility/column');
const { expected: expectedArrow } = require('../fixtures/compatibility/arrow');
const { expected: expectedArrowNativeTypes } = require('../fixtures/compatibility/arrow_native_types');

const { fixArrowResult } = fixtures;

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

async function execute(session: IDBSQLSession, statement: string) {
  const operation = await session.executeStatement(statement);
  const result = await operation.fetchAll();
  await operation.close();
  return result;
}

async function deleteTable(session: IDBSQLSession, tableName: string) {
  await execute(session, `DROP TABLE IF EXISTS ${tableName}`);
}

async function initializeTable(session: IDBSQLSession, tableName: string) {
  await deleteTable(session, tableName);

  const createTable = fixtures.createTableSql.replace(/\$\{table_name\}/g, tableName);
  await execute(session, createTable);

  const insertData = fixtures.insertDataSql.replace(/\$\{table_name\}/g, tableName);
  await execute(session, insertData);
}

describe('Arrow support', () => {
  const tableName = `dbsql_nodejs_sdk_e2e_arrow_${config.tableSuffix}`;

  function createTest(
    testBody: (session: IDBSQLSession) => void | Promise<void>,
    customConfig: Partial<ClientConfig> = {},
  ) {
    return async () => {
      const session = await openSession(customConfig);
      try {
        await initializeTable(session, tableName);
        await testBody(session);
      } finally {
        await deleteTable(session, tableName);
        await session.close();
      }
    };
  }

  it(
    'should not use arrow if disabled',
    createTest(
      async (session) => {
        const operation = await session.executeStatement(`SELECT * FROM ${tableName}`);
        const result = await operation.fetchAll();
        expect(result).to.deep.equal(expectedColumn);

        // @ts-expect-error TS2339: Property getResultHandler does not exist on type IOperation
        const resultHandler = await operation.getResultHandler();
        expect(resultHandler).to.be.instanceof(ResultSlicer);
        expect(resultHandler.source).to.be.not.instanceof(ArrowResultConverter);

        await operation.close();
      },
      {
        arrowEnabled: false,
        useLZ4Compression: false,
      },
    ),
  );

  it(
    'should use arrow with native types disabled',
    createTest(
      async (session) => {
        const operation = await session.executeStatement(`SELECT * FROM ${tableName}`);
        const result = await operation.fetchAll();
        expect(fixArrowResult(result)).to.deep.equal(expectedArrow);

        // @ts-expect-error TS2339: Property getResultHandler does not exist on type IOperation
        const resultHandler = await operation.getResultHandler();
        expect(resultHandler).to.be.instanceof(ResultSlicer);
        expect(resultHandler.source).to.be.instanceof(ArrowResultConverter);
        expect(resultHandler.source.source).to.be.instanceof(ArrowResultHandler);

        await operation.close();
      },
      {
        arrowEnabled: true,
        useArrowNativeTypes: false,
        useLZ4Compression: false,
      },
    ),
  );

  it(
    'should use arrow with native types enabled',
    createTest(
      async (session) => {
        const operation = await session.executeStatement(`SELECT * FROM ${tableName}`);
        const result = await operation.fetchAll();
        expect(fixArrowResult(result)).to.deep.equal(expectedArrowNativeTypes);

        // @ts-expect-error TS2339: Property getResultHandler does not exist on type IOperation
        const resultHandler = await operation.getResultHandler();
        expect(resultHandler).to.be.instanceof(ResultSlicer);
        expect(resultHandler.source).to.be.instanceof(ArrowResultConverter);
        expect(resultHandler.source.source).to.be.instanceof(ArrowResultHandler);

        await operation.close();
      },
      {
        arrowEnabled: true,
        useArrowNativeTypes: true,
        useLZ4Compression: false,
      },
    ),
  );

  it('should handle multiple batches in response', async () => {
    const rowsCount = 10000;

    const session = await openSession({
      arrowEnabled: true,
      useLZ4Compression: false,
    });
    const operation = await session.executeStatement(`
      SELECT *
      FROM range(0, ${rowsCount}) AS t1
      LEFT JOIN (SELECT 1) AS t2
    `);

    // We use some internals here to check that server returned response with multiple batches
    // @ts-expect-error TS2339: Property getResultHandler does not exist on type IOperation
    const resultHandler = await operation.getResultHandler();
    expect(resultHandler).to.be.instanceof(ResultSlicer);
    expect(resultHandler.source).to.be.instanceof(ArrowResultConverter);
    expect(resultHandler.source.source).to.be.instanceof(ArrowResultHandler);

    // @ts-expect-error TS2339: Property _data does not exist on type IOperation
    sinon.spy(operation._data, 'fetchNext');

    const result = await resultHandler.fetchNext({ limit: rowsCount });

    // @ts-expect-error TS2339: Property _data does not exist on type IOperation
    expect(operation._data.fetchNext.callCount).to.be.eq(1);
    // @ts-expect-error TS2339: Property _data does not exist on type IOperation
    const rawData = await operation._data.fetchNext.firstCall.returnValue;
    // We don't know exact count of batches returned, it depends on server's configuration,
    // but with much enough rows there should be more than one result batch
    expect(rawData.arrowBatches?.length).to.be.gt(1);

    expect(result.length).to.be.eq(rowsCount);
  });

  it(
    'should handle LZ4 compressed data',
    createTest(
      async (session) => {
        const operation = await session.executeStatement(
          `SELECT * FROM ${tableName}`,
          { useCloudFetch: false }, // Explicitly disable cloud fetch to test LZ4 compression
        );
        const result = await operation.fetchAll();
        expect(fixArrowResult(result)).to.deep.equal(expectedArrow);

        // @ts-expect-error TS2339: Property getResultHandler does not exist on type IOperation
        const resultHandler = await operation.getResultHandler();
        expect(resultHandler).to.be.instanceof(ResultSlicer);
        expect(resultHandler.source).to.be.instanceof(ArrowResultConverter);
        expect(resultHandler.source.source).to.be.instanceof(ArrowResultHandler);
        expect(resultHandler.source.source.isLZ4Compressed).to.be.true;

        await operation.close();
      },
      {
        arrowEnabled: true,
        useArrowNativeTypes: false,
        useLZ4Compression: true,
      },
    ),
  );
});
