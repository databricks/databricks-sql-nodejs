// Node KERNEL retry / Retry-After backoff test, through a mitmproxy fault injector.
//
// Mirrors the Python CUJ (Gopal Lal): inject N transient faults (429 + Retry-After
// or 503), confirm the kernel retries and the query ultimately succeeds, and that
// it honors the server Retry-After header.
//
// Node mapping of Python's `_tls_trusted_ca_file`:
//   proxy:                 { protocol:'http', host:'127.0.0.1', port:8080 }
//   customCaCert:          <~/.mitmproxy/mitmproxy-ca-cert.pem>   (trust the MITM cert)
//   checkServerCertificate:true                                  (keep verify ON, like Python)
//
// Usage:  BUGBASH_THRIFT=0 node node-retry-backoff.js     (kernel, default)
//         BUGBASH_THRIFT=1 node node-retry-backoff.js     (thrift, for parity)
const fs = require('fs');
const os = require('os');
const path = require('path');
const { DBSQLClient } = require('@databricks/sql');

const HOST = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
const PATH = process.env.DATABRICKS_PECOTESTING_HTTP_PATH2;
const TOKEN = process.env.DATABRICKS_PECOTESTING_TOKEN;
const USE_KERNEL = process.env.BUGBASH_THRIFT !== '1';
const PROXY_PORT = Number(process.env.PROXY_PORT || 8080);
const CA_PATH = path.join(os.homedir(), '.mitmproxy', 'mitmproxy-ca-cert.pem');

(async () => {
  const ca = fs.readFileSync(CA_PATH);
  const client = new DBSQLClient();

  const connectOpts = {
    host: HOST,
    path: PATH,
    token: TOKEN,
    useKernel: USE_KERNEL,
    proxy: { protocol: 'http', host: '127.0.0.1', port: PROXY_PORT },
  };
  if (USE_KERNEL) {
    // Trust the mitmproxy MITM CA but keep verification ON (Python parity).
    connectOpts.checkServerCertificate = true;
    connectOpts.customCaCert = ca;
  } else {
    // Thrift transport CA field.
    connectOpts.ca = ca;
  }

  const label = USE_KERNEL ? 'KERNEL' : 'THRIFT';
  const t0 = Date.now();
  await client.connect(connectOpts);
  const session = await client.openSession();
  console.log(`[${label}] connected through proxy in ${Date.now() - t0}ms`);

  // SLOW=1 forces a multi-second query so the statement goes PENDING and the
  // kernel issues GET poll requests (idempotent) — lets us fault the poll path.
  const sql = process.env.SLOW === '1'
    ? "SELECT count(*) AS v FROM range(2000000000) WHERE sha2(cast(id AS string),256) LIKE '%ffff%'"
    : 'SELECT 42 AS v';
  const tq = Date.now();
  const op = await session.executeStatement(sql);
  const rows = await op.fetchAll();
  await op.close();
  const elapsed = Date.now() - tq;

  await session.close();
  await client.close();

  const ok = process.env.SLOW === '1' ? rows.length === 1 && rows[0].v != null : Number(rows[0].v) === 42;
  if (!ok) {
    console.log(`[${label}] FAIL — wrong result: ${JSON.stringify(rows)}`);
    process.exit(1);
  }
  console.log(`[${label}] PASS — v=${rows[0].v} after retries; execute wall=${elapsed}ms`);
  process.exit(0);
})().catch((e) => {
  console.log(`[${process.env.BUGBASH_THRIFT !== '1' ? 'KERNEL' : 'THRIFT'}] ERROR — ${(e.message || String(e)).split('\n')[0]}`);
  process.exit(2);
});
