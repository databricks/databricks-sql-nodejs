const { DBSQLClient, DBSQLLogger, LogLevel } = require('../');

// This logger will emit logs to console and log.txt
//
const logger = new DBSQLLogger({ filepath: 'log.txt', level: LogLevel.info });

const client = new DBSQLClient({ logger: logger });

client
  .connect({
    host: '****.cloud.databricks.com',
    path: '/sql/2.0/warehouses/*************',
    token: 'dapi**************************',
  })
  .then(async (client) => {
    const session = await client.openSession();

    let queryOperation = await session.executeStatement('SELECT "Hello, World!"');
    let result = await queryOperation.fetchAll();
    await queryOperation.close();

    console.table(result);

    // Set logger to different level.
    //
    logger.setLevel(LogLevel.debug);

    queryOperation = await session.executeStatement('SELECT "Hello, World!"');
    result = await queryOperation.fetchAll();
    await queryOperation.close();

    await session.close();
    await client.close();
  })
  .catch((error) => {
    console.log(error);
  });
