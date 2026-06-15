// Shared helpers for the Kernel Bug Blitz scenario scripts.
// Connects through the npm-published RC (@databricks/sql@1.16.0-rc.1) in
// KERNEL MODE (useKernel:true) against the pecotesting warehouse.
//
// Creds come from env (source ~/.zshrc): DATABRICKS_PECOTESTING_*.
const { DBSQLClient } = require('@databricks/sql');

const HOST = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
const PATH = process.env.DATABRICKS_PECOTESTING_HTTP_PATH2;
const TOKEN = process.env.DATABRICKS_PECOTESTING_TOKEN;

// Derive warehouse / workspace ids from the connection inputs so the suite is
// portable across warehouses (CI maps repo secrets into the env above). Order:
// explicit override env -> parse from PATH/HOST -> undefined.
//   WAREHOUSE_ID: last segment of the http path (/sql/1.0/warehouses/<id>[?...])
//   WORKSPACE_ID: Azure host adb-<workspaceId>.<n>.azuredatabricks.net, or ?o=<id> in the path
function parseWarehouseId(p) {
  const m = /\/warehouses\/([^/?]+)/.exec(p || '');
  return m ? m[1] : undefined;
}
function parseWorkspaceId(host, p) {
  const o = /[?&]o=([^&]+)/.exec(p || '');
  if (o) return o[1];
  const m = /^adb-(\d+)\./.exec(host || '');
  return m ? m[1] : undefined;
}
const WAREHOUSE_ID = process.env.BUGBASH_WAREHOUSE_ID || parseWarehouseId(PATH);
const WORKSPACE_ID = process.env.BUGBASH_WORKSPACE_ID || parseWorkspaceId(HOST, PATH);

function assertEnv() {
  if (!HOST || !PATH || !TOKEN) {
    throw new Error('Set DATABRICKS_PECOTESTING_SERVER_HOSTNAME / _HTTP_PATH2 / _TOKEN');
  }
}

// Primary client. Kernel mode by default; set BUGBASH_THRIFT=1 to run the
// SAME scenarios over the Thrift backend (for parity comparison).
const USE_KERNEL = process.env.BUGBASH_THRIFT !== '1';
async function kernelConnect(overrides = {}) {
  assertEnv();
  const client = new DBSQLClient();
  await client.connect({ host: HOST, path: PATH, token: TOKEN, useKernel: USE_KERNEL, ...overrides });
  return client;
}

// Thrift client (kernel off) — for parity comparisons.
async function thriftConnect(overrides = {}) {
  assertEnv();
  const client = new DBSQLClient();
  await client.connect({ host: HOST, path: PATH, token: TOKEN, useKernel: false, ...overrides });
  return client;
}

// Run a statement and return all rows. execOpts passes through (params, etc.).
async function runQuery(session, sql, execOpts = {}) {
  const op = await session.executeStatement(sql, execOpts);
  const rows = await op.fetchAll();
  await op.close();
  return rows;
}

// Thin pass/fail reporter for a scenario script.
async function scenario(name, fn) {
  const t0 = Date.now();
  try {
    const note = await fn();
    console.log(`PASS | ${name} | ${note || ''} | ${Date.now() - t0}ms`);
    process.exit(0);
  } catch (err) {
    console.log(`FAIL | ${name} | ${(err && err.message ? err.message : String(err)).split('\n')[0]} | ${Date.now() - t0}ms`);
    process.exit(1);
  }
}

// Run a list of scenarios sequentially: [{ name, skip?, reason?, fn }].
// Each fn returns a short note (or throws). Prints one line per scenario
// and a final summary. Returns {pass, fail, skip}.
async function runSuite(title, scenarios) {
  console.log(`\n===== ${title} =====`);
  let pass = 0, fail = 0, skip = 0;
  for (const s of scenarios) {
    if (s.skip) { console.log(`SKIP | ${s.name} | ${s.reason || ''}`); skip++; continue; }
    const t0 = Date.now();
    try {
      const note = await s.fn();
      console.log(`PASS | ${s.name} | ${note || ''} | ${Date.now() - t0}ms`);
      pass++;
    } catch (err) {
      const msg = (err && err.message ? err.message : String(err)).split('\n')[0];
      console.log(`FAIL | ${s.name} | ${msg} | ${Date.now() - t0}ms`);
      fail++;
    }
  }
  console.log(`----- ${title}: ${pass} pass, ${fail} fail, ${skip} skip -----`);
  return { pass, fail, skip };
}

module.exports = {
  DBSQLClient, kernelConnect, thriftConnect, runQuery, scenario, runSuite,
  HOST, PATH, TOKEN, WORKSPACE_ID, WAREHOUSE_ID, USE_KERNEL,
};
