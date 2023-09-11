const { expect } = require('chai');
const sinon = require('sinon');
const config = require('./utils/config');
const logger = require('./utils/logger')(config.logger);
const { DBSQLClient } = require('../..');
const globalConfig = require('../../dist/globalConfig').default;

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

describe('Data fetching', () => {
  beforeEach(() => {
    globalConfig.arrowEnabled = false;
  });

  afterEach(() => {
    globalConfig.arrowEnabled = true;
  });

  const query = `
    SELECT *
    FROM range(0, 1000) AS t1
    LEFT JOIN (SELECT 1) AS t2
  `;

  it('fetch chunks should return a max row set of chunkSize', async () => {
    const session = await openSession();
    sinon.spy(session.driver, 'fetchResults');
    try {
      // set `maxRows` to null to disable direct results so all the data are fetched through `driver.fetchResults`
      const operation = await session.executeStatement(query, { maxRows: null });
      let chunkedOp = await operation.fetchChunk({ maxRows: 10 }).catch((error) => logger(error));
      expect(chunkedOp.length).to.be.equal(10);
      // we explicitly requested only one chunk
      expect(session.driver.fetchResults.callCount).to.equal(1);
    } finally {
      await session.close();
    }
  });

  it('fetch all should fetch all records', async () => {
    const session = await openSession();
    sinon.spy(session.driver, 'fetchResults');
    try {
      // set `maxRows` to null to disable direct results so all the data are fetched through `driver.fetchResults`
      const operation = await session.executeStatement(query, { maxRows: null });
      let all = await operation.fetchAll({ maxRows: 200 });
      expect(all.length).to.be.equal(1000);
      // 1000/200 = 5 chunks + one extra request to ensure that there's no more data
      expect(session.driver.fetchResults.callCount).to.equal(6);
    } finally {
      await session.close();
    }
  });

  it('should fetch all records if they fit within directResults response', async () => {
    const session = await openSession();
    sinon.spy(session.driver, 'fetchResults');
    try {
      // here `maxRows` enables direct results with limit of the first batch
      const operation = await session.executeStatement(query, { maxRows: 1000 });
      let all = await operation.fetchAll();
      expect(all.length).to.be.equal(1000);
      // all the data returned immediately from direct results, so no additional requests
      expect(session.driver.fetchResults.callCount).to.equal(0);
    } finally {
      await session.close();
    }
  });

  it('should fetch all records if only part of them fit within directResults response', async () => {
    const session = await openSession();
    sinon.spy(session.driver, 'fetchResults');
    try {
      // here `maxRows` enables direct results with limit of the first batch
      const operation = await session.executeStatement(query, { maxRows: 200 });
      // here `maxRows` sets limit for `driver.fetchResults`
      let all = await operation.fetchAll({ maxRows: 200 });
      expect(all.length).to.be.equal(1000);
      // 1 chunk returned immediately from direct results + 4 remaining chunks + one extra chunk to ensure
      // that there's no more data
      expect(session.driver.fetchResults.callCount).to.equal(5);
    } finally {
      await session.close();
    }
  });
});
