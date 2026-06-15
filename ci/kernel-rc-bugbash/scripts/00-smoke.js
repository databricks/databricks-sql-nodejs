// Smoke: kernel mode connects and SELECT 1 works. Run first.
const { kernelConnect, runQuery, scenario } = require('./lib');

scenario('00-smoke: SELECT 1 via useKernel', async () => {
  console.log('kernel binding:', require('@databricks/sql/native/kernel').version());
  const client = await kernelConnect();
  const session = await client.openSession();
  const rows = await runQuery(session, 'SELECT 1 AS one');
  await session.close();
  await client.close();
  if (!(rows.length === 1 && Number(rows[0].one) === 1)) throw new Error('unexpected: ' + JSON.stringify(rows));
  return JSON.stringify(rows);
});
