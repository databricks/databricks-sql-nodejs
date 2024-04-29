const { expect } = require('chai');
const sinon = require('sinon');
const config = require('./utils/config');
const { DBSQLClient } = require('../../lib');

async function openSession(customConfig) {
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
    initialCatalog: config.database[0],
    initialSchema: config.database[1],
  });
}

function arrayChunks(arr, chunkSize) {
  const result = [];

  while (arr.length > 0) {
    const chunk = arr.splice(0, chunkSize);
    result.push(chunk);
  }

  return result;
}

describe('Iterators', () => {
  it('should iterate over all chunks', async () => {
    const session = await openSession({ arrowEnabled: false });
    sinon.spy(session.context.driver, 'fetchResults');
    try {
      const expectedRowsCount = 10;

      // set `maxRows` to null to disable direct results so all the data are fetched through `driver.fetchResults`
      const operation = await session.executeStatement(`SELECT * FROM range(0, ${expectedRowsCount})`, {
        maxRows: null,
      });

      const expectedRows = Array.from({ length: expectedRowsCount }, (_, id) => ({ id }));
      const chunkSize = 4;
      const expectedChunks = arrayChunks(expectedRows, chunkSize);

      let index = 0;
      for await (const chunk of operation.iterateChunks({ maxRows: chunkSize })) {
        expect(chunk).to.deep.equal(expectedChunks[index]);
        index += 1;
      }

      expect(index).to.equal(expectedChunks.length);
    } finally {
      await session.close();
    }
  });

  it('should iterate over all rows', async () => {
    const session = await openSession({ arrowEnabled: false });
    sinon.spy(session.context.driver, 'fetchResults');
    try {
      const expectedRowsCount = 10;

      const operation = await session.executeStatement(`SELECT * FROM range(0, ${expectedRowsCount})`);

      const expectedRows = Array.from({ length: expectedRowsCount }, (_, id) => ({ id }));

      let index = 0;
      for await (const row of operation.iterateRows()) {
        expect(row).to.deep.equal(expectedRows[index]);
        index += 1;
      }

      expect(index).to.equal(expectedRows.length);
    } finally {
      await session.close();
    }
  });
});
