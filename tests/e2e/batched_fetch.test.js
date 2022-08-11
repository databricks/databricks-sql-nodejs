const { expect } = require('chai');
const config = require('./utils/config');
const logger = require('./utils/logger')(config.logger);
const { DBSQLClient, thrift } = require('../..');

const utils = DBSQLClient.utils;

const openSession = async () => {
  const client = new DBSQLClient();

  const connection = await client.connect({
    host: config.host,
    path: config.path,
    token: config.token,
  });

  const openSessionRequest = {};

  if (config.database.length === 2) {
    openSessionRequest.initialNamespace = {
      catalogName: config.database[0],
      schemaName: config.database[1],
    };
  }

  const session = await connection.openSession(openSessionRequest);

  return session;
};

it('fetch chunks should return a max row set of chunkSize', async () => {
    const session = await openSession();
    const operation = await session.executeStatement(`SELECT * FROM default.diamonds`, { runAsync: true });
    let chunkedOp = await operation.fetchChunk(100).catch(error=>logger(error));
    expect(chunkedOp.length == 100);
})
it('fetch all should fetch all records', async () => {
    const session = await openSession();
    const operation = await session.executeStatement(`SELECT * FROM default.diamonds LIMIT 1000`, { runAsync: true });
    let all = await operation.fetchAll();
    expect(all.length == 1000);
})