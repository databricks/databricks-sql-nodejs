const { DBSQLClient, DBSQLLogger, LOGLEVELS } = require('../');

const logger = new DBSQLLogger(LOGLEVELS.info);

const client = new DBSQLClient(logger);

client
  .connect({
    host: '********.databricks.com',
    path: '/sql/1.0/endpoints/****************',
    token: 'dapi********************************',
  })
  .then(async (client) => {
    const session = await client.openSession();

    let queryOperation = await session.executeStatement('SELECT "Hello, World!"', { runAsync: true });
    let result = await queryOperation.fetchAll();
    await queryOperation.close();

    console.table(result);

    // Set logger to different level.
    //
    logger.setLevel(LOGLEVELS.debug);

    queryOperation = await session.executeStatement('SELECT "Hello, World!"', { runAsync: true });
    result = await queryOperation.fetchAll();
    await queryOperation.close();

    await session.close();
    await client.close();
  })
  .catch((error) => {
    console.log(error);
  });
