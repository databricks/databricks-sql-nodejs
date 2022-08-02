const { DBSQLClient } = require('../');

const client = new DBSQLClient();

const host = '****.databricks.com';
const path = '/sql/1.0/endpoints/****';
const token = 'dapi********************************';

async function getQueryResult(operation) {
  const utils = DBSQLClient.utils;

  await utils.waitUntilReady(operation, false, () => {});
  console.log('Fetching data...');
  await utils.fetchAll(operation);
  await operation.close();

  return utils.getResult(operation).getValue();
}

async function cancelQuery(operation) {
  return new Promise((resolve, reject) => {
    operation
      .cancel()
      .then((status) => {
        if (status.success()) {
          resolve();
        } else {
          reject(new Error(status.getInfo().join('\n')));
        }
      })
      .catch(reject);
  });
}

client
  .connect({ host, path, token })
  .then(async (client) => {
    const session = await client.openSession();

    // Execute long-running query
    console.log('Running query...');
    const queryOperation = await session.executeStatement(
      `
        SELECT id 
        FROM RANGE(100000000)
        ORDER BY RANDOM() + 2 asc
      `,
      { runAsync: true },
    );
    getQueryResult(queryOperation)
      .then((result) => console.log(`Query returned ${result.length} row(s)`))
      .catch((error) => console.log(`Failed to load data: ${error.message}`));

    // Cancel query
    cancelQuery(queryOperation)
      .then(() => console.log('Query cancelled'))
      .catch((error) => console.log(`Failed to cancel query: ${error.message}`));

    // Expected output in console:
    //
    // > node examples/cancel_operation.js                                                                                                                                  Node 16.13.1
    // Running query...
    // Query cancelled
    // Failed to load data: The operation was canceled by a client

    await session.close();
  })
  .catch((error) => {
    console.log(error);
  });
