// getFunctions + debug-level logging (verify driver debug logs AND kernel logs print).
const { DBSQLClient, DBSQLLogger, LogLevel } = require('@databricks/sql');

const HOST = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
const PATH = process.env.DATABRICKS_PECOTESTING_HTTP_PATH2;
const TOKEN = process.env.DATABRICKS_PECOTESTING_TOKEN;

(async () => {
  const logger = new DBSQLLogger({ level: LogLevel.debug });
  console.log('LOGGER_LEVEL=' + logger.getLevel());
  const client = new DBSQLClient({ logger });
  await client.connect({ host: HOST, path: PATH, token: TOKEN, useKernel: true });
  const session = await client.openSession();

  // A query, to drive kernel-side logging through the unified sink.
  const op = await session.executeStatement('SELECT 1 AS one');
  await op.fetchAll();
  await op.close();

  // getFunctions — the thing under test.
  let rows;
  try {
    const fop = await session.getFunctions({ catalogName: 'samples', schemaName: 'default', functionName: '%' });
    rows = await fop.fetchAll();
    await fop.close();
    console.log('GETFUNCTIONS_OK rows=' + rows.length + (rows.length ? ' sample=' + JSON.stringify(rows[0]) : ''));
  } catch (e) {
    console.log('GETFUNCTIONS_ERR ' + (e.message || String(e)).split('\n')[0]);
  }

  await session.close();
  await client.close();
})().catch((e) => { console.log('FATAL ' + (e.message || String(e)).split('\n')[0]); process.exit(1); });
