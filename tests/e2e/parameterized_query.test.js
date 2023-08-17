const { expect } = require('chai');
const config = require('./utils/config');
const logger = require('./utils/logger')(config.logger);

const DBSQLClient = require('../../dist/DBSQLClient').default;
const convertToSparkParameters = require('../../dist/utils/ParameterConverter').default;
const { TSparkParameterValue, TSparkParameter } = require('../../thrift/TCLIService_types');
const globalConfig = require('../../dist/globalConfig').default;

describe('Parameterized query converter unit test', () => {
  expect(convertToSparkParameters({ key: null })[0]).to.deep.eq(
    new TSparkParameter({ name: 'key', type: 'VOID', value: new TSparkParameterValue() }),
  );
  expect(convertToSparkParameters({ key: 'value' })[0]).to.deep.eq(
    new TSparkParameter({ name: 'key', type: 'STRING', value: new TSparkParameterValue({ stringValue: 'value' }) }),
  );
  expect(convertToSparkParameters({ key: 1 })[0]).to.deep.eq(
    new TSparkParameter({ name: 'key', type: 'INT', value: new TSparkParameterValue({ doubleValue: 1 }) }),
  );
  expect(convertToSparkParameters({ key: 1.1 })[0]).to.deep.eq(
    new TSparkParameter({ name: 'key', type: 'DOUBLE', value: new TSparkParameterValue({ doubleValue: 1.1 }) }),
  );
  expect(convertToSparkParameters({ key: true })[0]).to.deep.eq(
    new TSparkParameter({ name: 'key', type: 'BOOLEAN', value: new TSparkParameterValue({ booleanValue: true }) }),
  );
});

const openSession = async () => {
  const client = new DBSQLClient();

  const connection = await client.connect({
    host: config.host,
    path: config.path,
    token: config.token,
  });

  return connection.openSession({
    initialCatalog: config.database[0],
    initialSchema: config.database[1],
  });
};

describe('Parameterized Query', async () => {
  it('should use default socket timeout', async () => {
    const query = `
        select * from default.stock_data where open > {{parameter}}
      `;

    let session = await openSession();

    let result = await session.executeStatement(query, { parameters: 2 });
  });
});
