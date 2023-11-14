const { expect, AssertionError } = require('chai');
const sinon = require('sinon');
const config = require('./utils/config');
const { DBSQLClient } = require('../..');

async function openSession(socketTimeout, customConfig) {
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
    socketTimeout,
  });

  return connection.openSession({
    initialCatalog: config.database[0],
    initialSchema: config.database[1],
  });
}

describe('Data fetching', () => {
  const socketTimeout = 1; // minimum value to make sure any request will time out

  it('should use default socket timeout', async () => {
    try {
      await openSession(undefined, { socketTimeout });
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error.message).to.be.eq('Request timed out');
    }
  });

  it('should use socket timeout from options', async () => {
    try {
      await openSession(socketTimeout);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error.message).to.be.eq('Request timed out');
    }
  });
});
