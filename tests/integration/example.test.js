const { expect } = require('chai');
const { DBSQLClient } = require('../..');

const client = new DBSQLClient();

describe('Example', () => {
  it('runs the example code', async () => {
    client
      .connect(
        {
          host: 'localhost',
          port: 8087,
          path: '/session',
          https: false,
        },
      )
      .then(async (client) => {
        const session = await client.openSession();

        const queryOperation = await session.executeStatement('select * from default.diamonds limit 250', {
          runAsync: true
        });
        const result = await queryOperation.fetchAll({maxRows: 100});
        await queryOperation.close();


        expect(result.length).to.be.equal(250)

        await session.close();
        await client.close();
      })
      .catch((error) => {
        expect(error).to.be.undefined
      })
  });
});