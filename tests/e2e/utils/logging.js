const { LOADIPHLPAPI } = require('dns');
const { DBSQLClient, DBSQLLogger, LogLevel } = require('../../../');

// This logger will emit logs to console and log.txt
//
const logger = new DBSQLLogger({ filepath: 'log.txt', level: LogLevel.info });

const client = new DBSQLClient({ logger: logger });

client
  .connect({
    host: 'dbc-75a23358-fd1a.dev.databricks.com',
    path: '/sql/1.0/warehouses/498b41a3ebe92536',
    token: 'dapid2c99f645588fc2642ce495d093638af',
  })
  .then(async (client) => {
    const session = await client.openSession();

    let queryOperation = await session.executeStatement('SELECT "Hello, World!"', { runAsync: true });
    let result = await queryOperation.fetchAll();
    await queryOperation.close();

    console.table(result);

    // Set logger to different level.
    //
    logger.setLevel(LogLevel.debug);

    queryOperation = await session.executeStatement('SELECT "Hello, World!"', { runAsync: true });
    result = await queryOperation.fetchAll();
    await queryOperation.close();

    await session.close();
    await client.close();
  })
  .catch((error) => {
    console.log(error);
  });
