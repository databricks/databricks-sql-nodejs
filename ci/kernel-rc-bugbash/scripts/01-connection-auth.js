// Category: Connection & Auth (KERNEL MODE)
//   - OAuth M2M (service principal client credentials)
//   - OAuth U2M (interactive browser)        [manual — skipped in automation]
//   - SPOG account-level http path (?o=workspaceId)
//   - HTTP forward proxy (Basic auth)         [needs a proxy server — skipped]
const { DBSQLClient, kernelConnect, runQuery, runSuite, HOST, WORKSPACE_ID, WAREHOUSE_ID } = require('./lib');

(async () => {
  await runSuite('Connection & Auth', [
    {
      name: 'OAuth M2M (client credentials) + query',
      skip: !(process.env.DATABRICKS_PECO_CLIENT_ID_PERSONAL && process.env.DATABRICKS_PECO_CLIENT_SECRET_PERSONAL),
      reason: 'DATABRICKS_PECO_CLIENT_ID_PERSONAL/SECRET_PERSONAL not set',
      fn: async () => {
        const client = new DBSQLClient();
        await client.connect({
          host: HOST,
          path: process.env.DATABRICKS_PECOTESTING_HTTP_PATH2,
          authType: 'databricks-oauth',
          oauthClientId: process.env.DATABRICKS_PECO_CLIENT_ID_PERSONAL,
          oauthClientSecret: process.env.DATABRICKS_PECO_CLIENT_SECRET_PERSONAL,
          useKernel: process.env.BUGBASH_THRIFT !== '1',
        });
        const s = await client.openSession();
        const rows = await runQuery(s, 'SELECT 1 AS one');
        await s.close(); await client.close();
        if (Number(rows[0].one) !== 1) throw new Error('bad result ' + JSON.stringify(rows));
        return 'M2M connect + SELECT 1 ok';
      },
    },
    {
      name: 'OAuth U2M (interactive browser) + query',
      skip: true,
      reason: 'manual: opens a browser for SSO; run scripts/manual/u2m.js by hand',
      fn: async () => {},
    },
    {
      name: 'SPOG account-level http path (?o=workspaceId)',
      skip: !(WORKSPACE_ID && WAREHOUSE_ID),
      reason: 'could not derive workspaceId/warehouseId from host/path (e.g. non-Azure host) — set BUGBASH_WORKSPACE_ID/BUGBASH_WAREHOUSE_ID to force',
      fn: async () => {
        const client = await kernelConnect({ path: `/sql/1.0/warehouses/${WAREHOUSE_ID}?o=${WORKSPACE_ID}` });
        const s = await client.openSession();
        const rows = await runQuery(s, 'SELECT 1 AS one');
        await s.close(); await client.close();
        if (Number(rows[0].one) !== 1) throw new Error('bad result ' + JSON.stringify(rows));
        return 'SPOG ?o= path connect + SELECT 1 ok';
      },
    },
    {
      name: 'HTTP forward proxy (Basic auth) + query',
      skip: true,
      reason: 'needs a running forward proxy (e.g. squid w/ basic auth) — not available in this env',
      fn: async () => {},
    },
  ]);
  process.exit(0);
})();
