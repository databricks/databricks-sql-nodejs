const { DBSQLClient } = require('..');

const client = new DBSQLClient();

const host = process.env.DATABRICKS_HOST;
const path = process.env.DATABRICKS_HTTP_PATH;
const token = process.env.DATABRICKS_TOKEN;

client
  .connect({ host, path, token })
  .then(async (client) => {
    const session = await client.openSession({
      configuration: {
        QUERY_TAGS: 'team:engineering,test:session-params,driver:node',
        ansi_mode: 'false',
      },
    });

    const op = await session.executeStatement('SELECT 1');
    const rows = await op.fetchAll();
    console.log(rows);
    await op.close();

    await session.close();
    await client.close();
  })
  .catch((error) => {
    console.log(error);
  });
