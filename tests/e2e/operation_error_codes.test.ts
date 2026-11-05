import { expect, AssertionError } from 'chai';
import { DBSQLClient } from '../../lib';
import IDBSQLSession from '../../lib/contracts/IDBSQLSession';
import OperationStateError, { OperationStateErrorCode } from '../../lib/errors/OperationStateError';

import config from './utils/config';

// Regression for https://github.com/databricks/databricks-sql-nodejs/issues/33 —
// errors thrown for failed operations should carry a machine-readable code
// (SQLSTATE) at the top level, not only a message buried in `response`.
describe('Operation error codes', () => {
  let client: DBSQLClient;
  let session: IDBSQLSession;

  before(async () => {
    client = new DBSQLClient();
    const connection = await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    });
    session = await connection.openSession({
      initialCatalog: config.catalog,
      initialSchema: config.schema,
    });
  });

  after(async () => {
    await session?.close();
    await client?.close();
  });

  async function expectOperationStateError(query: string) {
    try {
      const operation = await session.executeStatement(query);
      await operation.fetchAll();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error).to.be.instanceOf(OperationStateError);
      return error as OperationStateError & { sqlState?: string };
    }
    throw new Error('unreachable');
  }

  it('should expose server SQLSTATE on OperationStateError for runtime failures', async () => {
    const error = await expectOperationStateError("SELECT raise_error('boom')");

    expect(error.errorCode).to.be.eq(OperationStateErrorCode.Error);

    const wireSqlState = error.response?.sqlState ?? error.response?.status?.sqlState;
    expect(wireSqlState, 'server should send a SQLSTATE on the wire').to.be.a('string');
    expect(wireSqlState).to.have.length(5);

    expect(error.sqlState).to.be.eq(wireSqlState);
  });
});
