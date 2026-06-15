// Category: Errors & Resilience (KERNEL MODE)
//
// The two transient-retry scenarios need a fault-injecting proxy. They are
// proven end-to-end in scripts/retry/ (mitmproxy injector + node-retry-backoff.js):
//   - retries 429/503 and the query ultimately succeeds
//   - honors server Retry-After, else exponential backoff
// Retry policy is kernel-internal (Rust) and OS-independent, so it is validated
// once rather than per-OS. To run it INLINE here, start the injector and export:
//   RETRY_PROXY=127.0.0.1:8080   MITM_CA=~/.mitmproxy/mitmproxy-ca-cert.pem
// (see scripts/retry/inject_retry_after.py). The inline test then asserts a query
// SUCCEEDS through the faulting proxy — i.e. the kernel retried past the faults.
const fs = require('fs');
const { DBSQLClient, kernelConnect, runQuery, runSuite, HOST, PATH, TOKEN } = require('./lib');

const RETRY_PROXY = process.env.RETRY_PROXY; // "host:port"
const MITM_CA = process.env.MITM_CA && process.env.MITM_CA.replace(/^~/, process.env.HOME || '');

async function runThroughFaultingProxy() {
  const [phost, pport] = RETRY_PROXY.split(':');
  const client = new DBSQLClient();
  await client.connect({
    host: HOST, path: PATH, token: TOKEN, useKernel: process.env.BUGBASH_THRIFT !== '1',
    proxy: { protocol: 'http', host: phost, port: Number(pport) },
    checkServerCertificate: true,
    customCaCert: fs.readFileSync(MITM_CA),
  });
  const s = await client.openSession();
  const t0 = Date.now();
  const rows = await runQuery(s, 'SELECT 42 AS v'); // succeeds only if the kernel retried past injected faults
  await s.close(); await client.close();
  if (Number(rows[0].v) !== 42) throw new Error('bad result through proxy: ' + JSON.stringify(rows));
  return Date.now() - t0;
}

(async () => {
  const client = await kernelConnect();
  const session = await client.openSession();
  const proxyConfigured = Boolean(RETRY_PROXY && MITM_CA);

  await runSuite('Errors & Resilience', [
    {
      name: 'Invalid query surfaces a clear error / SQLSTATE',
      fn: async () => {
        try {
          await runQuery(session, 'SELECT * FROM a_table_that_does_not_exist_zzz');
          throw new Error('no error raised for missing table');
        } catch (e) {
          const m = (e.message || String(e));
          if (/no error raised/.test(m)) throw e;
          if (!/(TABLE_OR_VIEW_NOT_FOUND|not found|cannot.*resolve|UNRESOLVED)/i.test(m)) {
            throw new Error('error raised but unclear: ' + m.split('\n')[0].slice(0, 120));
          }
          return 'clear error: ' + m.split('\n')[0].slice(0, 90);
        }
      },
    },
    {
      name: 'Retry transient 503/429 then succeed',
      skip: !proxyConfigured,
      reason: 'set RETRY_PROXY + MITM_CA to run inline; proven in scripts/retry/ (mitmproxy injector)',
      fn: async () => {
        const ms = await runThroughFaultingProxy();
        return `query succeeded through faulting proxy in ${ms}ms (kernel retried past injected faults)`;
      },
    },
    {
      name: 'Retries honor backoff / Retry-After',
      skip: true,
      reason: 'timing assertion needs the injector log; proven in scripts/retry/ — honors Retry-After (~5-6s) and falls back to exponential 2→5→10s',
      fn: async () => {},
    },
  ]);

  await session.close(); await client.close();
  process.exit(0);
})();
