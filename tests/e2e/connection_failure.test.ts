import { expect, AssertionError } from 'chai';
import { DBSQLClient } from '../../lib';
import { THTTPException } from '../../lib/connection/connections/ThriftHttpConnection';

import config from './utils/config';

const INVALID_TOKEN = 'dapi-invalid-e2e-token';

// Regression for https://github.com/databricks/databricks-sql-nodejs/issues/467 —
// a rejected session open should carry the server's detail in `message`.
describe('Connection failure errors', () => {
  it('Thrift: includes the server detail in the message of a rejected session open', async () => {
    const client = new DBSQLClient();
    try {
      await client.connect({ host: config.host, path: config.path, token: INVALID_TOKEN });
      await client.openSession();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error).to.be.instanceOf(THTTPException);
      const { message, statusCode } = error as THTTPException;
      expect(statusCode).to.be.oneOf([401, 403]);
      expect(message.startsWith(`Received a response with a bad HTTP status code: ${statusCode}: `)).to.equal(true);
      expect(message.toLowerCase()).to.include('token');
      expect(message).to.not.include(INVALID_TOKEN);
    } finally {
      await client.close();
    }
  });
});
