import { expect } from 'chai';
import sinon from 'sinon';
import { DBSQLClient } from '../../lib';
import { ClientConfig } from '../../lib/contracts/IClientContext';

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

describe('Data fetching', () => {
  const query = `
    SELECT *
    FROM range(0, 1000) AS t1
    LEFT JOIN (SELECT 1) AS t2
  `;

  it('fetch chunks should return a max row set of chunkSize', async () => {
    const session = await openSession({ arrowEnabled: false });
    // @ts-expect-error TS2339: Property context does not exist on type IDBSQLSession
    sinon.spy(session.context.driver, 'fetchResults');
    try {
      // set `maxRows` to null to disable direct results so all the data are fetched through `driver.fetchResults`
      const operation = await session.executeStatement(query, { maxRows: null });
      const chunk = await operation.fetchChunk({ maxRows: 10, disableBuffering: true });
      expect(chunk.length).to.be.equal(10);
      // we explicitly requested only one chunk
      // @ts-expect-error TS2339: Property context does not exist on type IDBSQLSession
      expect(session.context.driver.fetchResults.callCount).to.equal(1);
    } finally {
      await session.close();
    }
  });

  it('fetch chunks should respect maxRows', async () => {
    const session = await openSession({ arrowEnabled: false });

    const chunkSize = 300;
    const lastChunkSize = 100; // 1000 % chunkSize

    try {
      const operation = await session.executeStatement(query, { maxRows: 500 });

      let hasMoreRows = true;
      let chunkCount = 0;

      while (hasMoreRows) {
        // eslint-disable-next-line no-await-in-loop
        const chunkedOp = await operation.fetchChunk({ maxRows: 300 });
        chunkCount += 1;

        // eslint-disable-next-line no-await-in-loop
        hasMoreRows = await operation.hasMoreRows();

        const isLastChunk = !hasMoreRows;
        expect(chunkedOp.length).to.be.equal(isLastChunk ? lastChunkSize : chunkSize);
      }

      expect(chunkCount).to.be.equal(4); // 1000 = 3*300 + 1*100
    } finally {
      await session.close();
    }
  });

  it('fetch all should fetch all records', async () => {
    const session = await openSession({ arrowEnabled: false });
    // @ts-expect-error TS2339: Property context does not exist on type IDBSQLSession
    sinon.spy(session.context.driver, 'fetchResults');
    try {
      // set `maxRows` to null to disable direct results so all the data are fetched through `driver.fetchResults`
      const operation = await session.executeStatement(query, { maxRows: null });
      const all = await operation.fetchAll({ maxRows: 200 });
      expect(all.length).to.be.equal(1000);
      // 1000/200 = 5 chunks + one extra request to ensure that there's no more data
      // @ts-expect-error TS2339: Property context does not exist on type IDBSQLSession
      expect(session.context.driver.fetchResults.callCount).to.equal(6);
    } finally {
      await session.close();
    }
  });

  it('should fetch all records if they fit within directResults response', async () => {
    const session = await openSession({ arrowEnabled: false });
    // @ts-expect-error TS2339: Property context does not exist on type IDBSQLSession
    sinon.spy(session.context.driver, 'fetchResults');
    try {
      // here `maxRows` enables direct results with limit of the first batch
      const operation = await session.executeStatement(query, { maxRows: 1000 });
      const all = await operation.fetchAll();
      expect(all.length).to.be.equal(1000);
      // all the data returned immediately from direct results, so no additional requests
      // @ts-expect-error TS2339: Property context does not exist on type IDBSQLSession
      expect(session.context.driver.fetchResults.callCount).to.equal(0);
    } finally {
      await session.close();
    }
  });

  it('should fetch all records if only part of them fit within directResults response', async () => {
    const session = await openSession({ arrowEnabled: false });
    // @ts-expect-error TS2339: Property context does not exist on type IDBSQLSession
    sinon.spy(session.context.driver, 'fetchResults');
    try {
      // here `maxRows` enables direct results with limit of the first batch
      const operation = await session.executeStatement(query, { maxRows: 200 });
      // here `maxRows` sets limit for `driver.fetchResults`
      const all = await operation.fetchAll({ maxRows: 200 });
      expect(all.length).to.be.equal(1000);
      // 1 chunk returned immediately from direct results + 4 remaining chunks + one extra chunk to ensure
      // that there's no more data
      // @ts-expect-error TS2339: Property context does not exist on type IDBSQLSession
      expect(session.context.driver.fetchResults.callCount).to.equal(5);
    } finally {
      await session.close();
    }
  });
});
