// Kernel-vs-Thrift behavior diff harness (non-metadata).
// Runs each probe query on BOTH backends, deep-normalizes the rows, and
// reports MATCH or DIFFER. A DIFFER (or one-side error) is a candidate bug.
const { DBSQLClient } = require('@databricks/sql');
const H = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
const P = process.env.DATABRICKS_PECOTESTING_HTTP_PATH2;
const T = process.env.DATABRICKS_PECOTESTING_TOKEN;

const PROBES = [
  ['interval-daytime',        "SELECT INTERVAL '1 2:3:4' DAY TO SECOND AS v"],
  ['interval-daytime-frac',   "SELECT INTERVAL '1 2:3:4.567891' DAY TO SECOND AS v"],
  ['interval-daytime-neg',    "SELECT INTERVAL '-1 2:3:4' DAY TO SECOND AS v"],
  ['interval-hour-second',    "SELECT INTERVAL '10:20:30' HOUR TO SECOND AS v"],
  ['interval-seconds',        "SELECT INTERVAL '90.5' SECOND AS v"],
  ['interval-large-days',     "SELECT INTERVAL '1000000 0:0:0' DAY TO SECOND AS v"],
  ['interval-yearmonth',      "SELECT INTERVAL '2-3' YEAR TO MONTH AS v"],
  ['interval-yearmonth-neg',  "SELECT INTERVAL '-2-3' YEAR TO MONTH AS v"],
  ['interval-month',          "SELECT INTERVAL '14' MONTH AS v"],
  ['bigint-max',              'SELECT 9223372036854775807 AS v'],
  ['bigint-2to53plus1',       'SELECT 9007199254740993 AS v'],
  ['bigint-neg',              'SELECT -9223372036854775808 AS v'],
  ['decimal-highprec',        "SELECT CAST('1234567890123456789.123456789' AS DECIMAL(38,9)) AS v"],
  ['decimal-tiny-neg',        "SELECT CAST('-0.0000000001' AS DECIMAL(20,10)) AS v"],
  ['double-nan',              "SELECT CAST('NaN' AS DOUBLE) AS v"],
  ['double-pinf',             "SELECT CAST('Infinity' AS DOUBLE) AS v"],
  ['double-ninf',             "SELECT CAST('-Infinity' AS DOUBLE) AS v"],
  ['double-negzero',          "SELECT CAST('-0.0' AS DOUBLE) AS v"],
  ['float-precision',         'SELECT CAST(0.1 AS FLOAT) AS v'],
  ['binary',                  "SELECT X'00DEADBEEFFF' AS v"],
  ['ts-micros',               "SELECT TIMESTAMP '2026-01-02 03:04:05.123456' AS v"],
  ['ts-preepoch',             "SELECT TIMESTAMP '1900-06-15 12:00:00' AS v"],
  ['ntz-micros',              "SELECT CAST(TIMESTAMP '2026-01-02 03:04:05.123456' AS TIMESTAMP_NTZ) AS v"],
  ['date-farfuture',          "SELECT DATE '9999-12-31' AS v"],
  ['date-year1',              "SELECT DATE '0001-01-01' AS v"],
  ['null-in-array',           'SELECT array(1, CAST(NULL AS INT), 3) AS v'],
  ['map-null-value',          "SELECT map('a', CAST(NULL AS INT), 'b', 2) AS v"],
  ['struct-null-field',       "SELECT named_struct('x', CAST(NULL AS INT), 'y', 'z') AS v"],
  ['nested-deep',             "SELECT array(map('k', array(1,2)), map('j', array(3))) AS v"],
  ['char-padded',             "SELECT CAST('ab' AS CHAR(5)) AS v"],
  ['tinyint-min',             'SELECT CAST(-128 AS TINYINT) AS v'],
  ['unnamed-col',             'SELECT 1'],
  ['dup-col-names',           'SELECT 1 AS x, 2 AS x'],
  ['empty-string-vs-null',    "SELECT '' AS empty, CAST(NULL AS STRING) AS nul"],
];

function norm(v) {
  if (v === null || v === undefined) return v;
  if (typeof v === 'bigint') return 'BIGINT:' + v.toString();
  if (typeof v === 'number') return Number.isNaN(v) ? 'NUM:NaN' : (v === Infinity ? 'NUM:Inf' : (v === -Infinity ? 'NUM:-Inf' : v));
  if (Buffer.isBuffer(v)) return 'BUF:' + v.toString('hex');
  if (v instanceof Date) return 'DATE:' + v.toISOString();
  if (Array.isArray(v)) return v.map(norm);
  if (typeof v === 'object') { const o = {}; for (const k of Object.keys(v)) o[k] = norm(v[k]); return o; }
  return v;
}

async function run(useKernel, sql) {
  const c = new DBSQLClient();
  await c.connect({ host: H, path: P, token: T, useKernel });
  try {
    const s = await c.openSession();
    const op = await s.executeStatement(sql);
    const rows = await op.fetchAll();
    await op.close(); await s.close();
    return { ok: true, rows: norm(rows) };
  } catch (e) {
    return { ok: false, err: (e.message || String(e)).split('\n')[0].slice(0, 100) };
  } finally { await c.close(); }
}

(async () => {
  let diffs = 0;
  for (const [label, sql] of PROBES) {
    const k = await run(true, sql);
    const t = await run(false, sql);
    const ks = JSON.stringify(k.ok ? k.rows : 'ERR:' + k.err);
    const ts = JSON.stringify(t.ok ? t.rows : 'ERR:' + t.err);
    if (ks === ts) {
      console.log(`MATCH  | ${label} | ${ks.slice(0, 70)}`);
    } else {
      diffs++;
      console.log(`DIFFER | ${label}`);
      console.log(`         kernel: ${ks.slice(0, 160)}`);
      console.log(`         thrift: ${ts.slice(0, 160)}`);
    }
  }
  console.log(`\nDIFFERENCES: ${diffs} / ${PROBES.length}`);
  process.exit(0);
})();
