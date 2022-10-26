const { expect } = require('chai');
const config = require('./utils/config');
const logger = require('./utils/logger')(config.logger);
const { DBSQLClient } = require('../..');

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
  const query = `
    SELECT *
    FROM range(0, 1000) AS t1
    LEFT JOIN (SELECT 1) AS t2
  `;

  it('fetch chunks should return a max row set of chunkSize', async () => {
    const session = await openSession();
    const operation = await session.executeStatement(query, { runAsync: true, maxRows: null });
    let chunkedOp = await operation.fetchChunk({ maxRows: 10 }).catch((error) => logger(error));
    expect(chunkedOp.length).to.be.equal(10);
  });

  it('fetch all should fetch all records', async () => {
    const session = await openSession();
    const operation = await session.executeStatement(query, { runAsync: true, maxRows: null });
    let all = await operation.fetchAll();
    expect(all.length).to.be.equal(1000);
  });
});
