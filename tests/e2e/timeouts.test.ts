import { expect, AssertionError } from 'chai';
import sinon from 'sinon';
import { DBSQLClient } from '../../lib';
import { ClientConfig } from '../../lib/contracts/IClientContext';

import config from './utils/config';

async function openSession(socketTimeout: number | undefined, customConfig: Partial<ClientConfig> = {}) {
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
    initialCatalog: config.catalog,
    initialSchema: config.schema,
  });
}

describe('Timeouts', () => {
  const socketTimeout = 1; // minimum value to make sure any request will time out

  it('should use default socket timeout', async () => {
    try {
      await openSession(undefined, { socketTimeout });
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
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
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error.message).to.be.eq('Request timed out');
    }
  });
});
