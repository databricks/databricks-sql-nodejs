const { expect, assert } = require('chai');
const { DBSQLClient } = require('../..');

const client = new DBSQLClient();

describe('Example', () => {
  it('runs the example code', async () => {
    const c = await client
      .connect({
        host: 'localhost',
        port: 8087,
        path: '/session',
        https: false,
      })

    const session = await client.openSession();

    const queryOperation = await session.executeStatement('select * from default.diamonds limit 250', {
      runAsync: true,
      maxRows: 100,
    });
    const result = await queryOperation.fetchAll({ maxRows: 100 });
    await queryOperation.close();

    expect(result.length).to.be.equal(250);

    await session.close();
    await client.close();

  });
});
