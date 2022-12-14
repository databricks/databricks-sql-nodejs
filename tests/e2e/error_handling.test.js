const { expect } = require('chai');
const config = require('./utils/config');
const { DBSQLClient } = require('../..');

const openSession = async () => {
  const client = new DBSQLClient();

  const connection = await client.connect({
    host: 'blah',
    path: config.path,
    token: config.token,
  });

  return connection.openSession({
    initialCatalog: config.database[0],
    initialSchema: config.database[1],
  });
};

describe('Error handling', () => {
  const query = `
    SELECT *
    FROM something
  `;

  it('fetch chunks should return a max row set of chunkSize', async () => {
    const session = await openSession();
    const operation = await session.executeStatement(query, { runAsync: true, maxRows: null });
    let chunkedOp = await operation.fetchChunk({ maxRows: 10 }).catch((error)=>{console.log(error)});

  });
});
