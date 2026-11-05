import { AssertionError, expect } from 'chai';
import sinon from 'sinon';
import { DBSQLClient } from '../../lib';
import Status from '../../lib/dto/Status';

import config from './utils/config';

// STATEMENT-022: concurrent Operation.cancel() calls must not deadlock or crash,
// and on Thrift must be deduplicated into exactly one CancelOperation RPC
// (parity with databricks-sql-go#419 and the shared driver conformance spec).
describe('Concurrent cancel (STATEMENT-022)', function () {
  this.timeout(120 * 1000);

  it('three simultaneous cancel() calls: no deadlock, single CancelOperation [thrift]', async () => {
    const client = new DBSQLClient();
    const connection = await client.connect({
      host: config.host,
      path: config.path,
      token: config.token,
    });
    const session = await connection.openSession({
      initialCatalog: config.catalog,
      initialSchema: config.schema,
    });

    // @ts-expect-error TS2339: Property context does not exist on type IDBSQLSession
    const cancelSpy = sinon.spy(session.context.driver, 'cancelOperation');

    try {
      const operation = await session.executeStatement(
        'SELECT a.*, b.* FROM range(0, 100000) a CROSS JOIN range(0, 100000) b ORDER BY a.id DESC, b.id DESC',
        { runAsync: true, maxRows: null },
      );

      const results = await Promise.all([operation.cancel(), operation.cancel(), operation.cancel()]);
      results.forEach((status) => {
        expect(status).to.be.instanceOf(Status);
        expect(status.isSuccess).to.be.true;
      });

      expect(cancelSpy.callCount).to.equal(1);

      try {
        await operation.fetchChunk({ disableBuffering: true });
        expect.fail('fetchChunk should be rejected after cancel');
      } catch (error) {
        if (error instanceof AssertionError) {
          throw error;
        }
      }
    } finally {
      cancelSpy.restore();
      await session.close();
      await client.close();
    }
  });
});
