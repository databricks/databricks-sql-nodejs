import { expect, AssertionError } from 'chai';
import { DBSQLClient } from '../../../lib';
import AuthenticationError from '../../../lib/errors/AuthenticationError';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import { InternalConnectionOptions } from '../../../lib/contracts/InternalConnectionOptions';
import { tryGetKernelNative } from '../../../lib/kernel/KernelNativeLoader';
import config from '../utils/config';

const INVALID_TOKEN = 'dapi-invalid-e2e-token';

// Regression for https://github.com/databricks/databricks-sql-nodejs/issues/467 —
// a rejected kernel session open should keep the underlying error as `cause`.
describe('kernel connection failure errors', function suite() {
  this.timeout(60_000);

  if (tryGetKernelNative() === undefined) {
    it.skip('kernel native binding not available on this platform');
    return;
  }

  it('keeps the server detail in the message and the binding error as cause', async () => {
    const client = new DBSQLClient();
    try {
      await client.connect({
        host: config.host,
        path: config.path,
        token: INVALID_TOKEN,
        useKernel: true,
      } as ConnectionOptions & InternalConnectionOptions);
      await client.openSession();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error).to.be.instanceOf(AuthenticationError);
      const { message, cause } = error as AuthenticationError & { cause?: unknown };
      expect(message).to.match(/HTTP 40[13]/);
      expect(message.toLowerCase()).to.include('token');
      expect(message).to.not.include(INVALID_TOKEN);
      expect(cause).to.be.instanceOf(Error);
      expect((cause as Error).message).to.not.include('__databricks_error__');
    } finally {
      await client.close();
    }
  });
});
