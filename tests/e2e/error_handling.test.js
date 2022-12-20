const { expect } = require('chai');
const config = require('./utils/config');
const { DBSQLClient } = require('../..');

const openSession = async () => {
  const client = new DBSQLClient();

  const connection = await client.connect({
    host: config.host,
    path: config.path,
    token: 'hast',
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
    console.log(session);
  });
});