const { expect, AssertionError } = require('chai');
const config = require('./utils/config');
const { DBSQLClient } = require('../..');
const globalConfig = require('../../dist/globalConfig').default;

const openSession = async (socketTimeout) => {
  const client = new DBSQLClient();

  const connection = await client.connect({
    host: config.host,
    path: config.path,
    token: config.token,
    socketTimeout,
  });

  return connection.openSession({
    initialCatalog: config.database[0],
    initialSchema: config.database[1],
  });
};

describe('Data fetching', () => {
  const query = `
    SELECT *
    FROM range(0, 100000) AS t1
    LEFT JOIN (SELECT 1) AS t2
    ORDER BY RANDOM() ASC
  `;

  const socketTimeout = 1; // minimum value to make sure any request will time out

  it('should use default socket timeout', async () => {
    const savedTimeout = globalConfig.socketTimeout;
    globalConfig.socketTimeout = socketTimeout;
    try {
      await openSession();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error.message).to.be.eq('Request timed out');
    } finally {
      globalConfig.socketTimeout = savedTimeout;
    }
  });

  it('should use socket timeout from options', async () => {
    try {
      await await openSession(socketTimeout);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error.message).to.be.eq('Request timed out');
    }
  });
});
