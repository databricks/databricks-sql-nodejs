// Kernel-vs-Thrift BEHAVIOR diff (non-metadata): fetch/exec semantics.
const { DBSQLClient } = require('@databricks/sql');
const H = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
const P = process.env.DATABRICKS_PECOTESTING_HTTP_PATH2;
const T = process.env.DATABRICKS_PECOTESTING_TOKEN;

async function withSession(useKernel, fn) {
  const c = new DBSQLClient();
  await c.connect({ host: H, path: P, token: T, useKernel });
  try { const s = await c.openSession(); const r = await fn(s); await s.close(); return r; }
  finally { await c.close(); }
}

const PROBES = [
  ['maxRows caps fetchAll (maxRows=10 on 1000)', async (s) => {
    const op = await s.executeStatement('SELECT id FROM range(1000)', { maxRows: 10 });
    const rows = await op.fetchAll(); await op.close();
    return 'rows=' + rows.length;
  }],
  ['fetchChunk sizes + hasMoreRows (2500 by 1000)', async (s) => {
    const op = await s.executeStatement('SELECT id FROM range(2500)');
    const sizes = []; let guard = 0;
    do { const ch = await op.fetchChunk({ maxRows: 1000 }); sizes.push(ch.length); } while ((await op.hasMoreRows()) && ++guard < 20);
    await op.close();
    return 'chunks=' + JSON.stringify(sizes);
  }],
  ['queryTimeout aborts long query (queryTimeout=3)', async (s) => {
    const op = await s.executeStatement("SELECT count(*) FROM range(5000000000) WHERE sha2(cast(id AS string),256) LIKE '%fffff%'", { queryTimeout: 3 });
    try { await op.fetchAll(); await op.close(); return 'NOT-aborted'; }
    catch (e) { return 'aborted:' + /timeout|timed out|exceeded|cancel|deadline/i.test(e.message || ''); }
  }],
  ['getSchema column names + count', async (s) => {
    const op = await s.executeStatement('SELECT 1 AS a, CAST(2 AS BIGINT) AS b, true AS c');
    const sc = await op.getSchema();
    await op.fetchAll(); await op.close();
    const cols = (sc && sc.columns ? sc.columns : sc || []).map((x) => x.columnName || x.name).filter(Boolean);
    return 'ncols=' + (Array.isArray(sc) ? sc.length : (sc && sc.columns ? sc.columns.length : '?')) + ' names=' + JSON.stringify(cols);
  }],
  ['empty result still has schema (range 0)', async (s) => {
    const op = await s.executeStatement('SELECT id, CAST(id AS STRING) sid FROM range(0)');
    const sc = await op.getSchema();
    const rows = await op.fetchAll(); await op.close();
    const n = Array.isArray(sc) ? sc.length : (sc && sc.columns ? sc.columns.length : 0);
    return 'rows=' + rows.length + ' schemaCols=' + n;
  }],
  ['hasMoreRows before any fetch', async (s) => {
    const op = await s.executeStatement('SELECT id FROM range(5)');
    const before = await op.hasMoreRows();
    const rows = await op.fetchAll();
    const after = await op.hasMoreRows();
    await op.close();
    return 'before=' + before + ' rows=' + rows.length + ' after=' + after;
  }],
  ['maxRows larger than result (maxRows=100 on 5)', async (s) => {
    const op = await s.executeStatement('SELECT id FROM range(5)', { maxRows: 100 });
    const rows = await op.fetchAll(); await op.close();
    return 'rows=' + rows.length;
  }],
];

(async () => {
  let diffs = 0;
  for (const [label, fn] of PROBES) {
    let k, t;
    try { k = await withSession(true, fn); } catch (e) { k = 'ERR:' + (e.message || e).split('\n')[0].slice(0, 80); }
    try { t = await withSession(false, fn); } catch (e) { t = 'ERR:' + (e.message || e).split('\n')[0].slice(0, 80); }
    if (k === t) console.log(`MATCH  | ${label} | ${k}`);
    else { diffs++; console.log(`DIFFER | ${label}\n         kernel: ${k}\n         thrift: ${t}`); }
  }
  console.log(`\nDIFFERENCES: ${diffs} / ${PROBES.length}`);
  process.exit(0);
})();
