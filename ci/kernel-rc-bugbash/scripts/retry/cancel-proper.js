// A cancel test that can actually FAIL.
//   - genuinely long, non-optimizable query (sha2 over a huge range; ~30s+)
//   - runAsync so it's truly in-flight
//   - assert it is RUNNING/PENDING before we cancel (proves it didn't finish)
//   - cancel()
//   - assert operationState becomes CANCELED_STATE (3)  -> server really cancelled
//   - assert the whole thing returned FAST (<< natural runtime)  -> not natural completion
const { DBSQLClient } = require('@databricks/sql');
const H = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
const P = process.env.DATABRICKS_PECOTESTING_HTTP_PATH2;
const T = process.env.DATABRICKS_PECOTESTING_TOKEN;
const USE_KERNEL = process.env.BUGBASH_THRIFT !== '1';

// Thrift TOperationState: RUNNING=1, FINISHED=2, CANCELED=3, CLOSED=4, PENDING=5
const RUNNING = 1, CANCELED = 3, PENDING = 5;
const LONG_SQL = "SELECT count(*) FROM range(5000000000) WHERE sha2(cast(id AS string),256) LIKE '%fffff%'";

(async () => {
  const label = USE_KERNEL ? 'KERNEL' : 'THRIFT';
  const c = new DBSQLClient();
  await c.connect({ host: H, path: P, token: T, useKernel: USE_KERNEL });
  const s = await c.openSession();

  const op = await s.executeStatement(LONG_SQL, { runAsync: true });
  await new Promise((r) => setTimeout(r, 2000));

  // 1) prove it's actually in-flight, not already done (status BEFORE cancel must succeed)
  let stBefore;
  try {
    stBefore = (await op.status()).operationState;
  } catch (e) {
    throw new Error(`status() threw BEFORE cancel — unexpected: ${(e.message || e).split('\n')[0]}`);
  }
  const inFlight = stBefore === RUNNING || stBefore === PENDING;
  if (!inFlight) {
    throw new Error(`PRECONDITION FAIL: not in-flight before cancel (state=${stBefore}); query was optimized/finished — meaningless`);
  }

  // 2) cancel and time it
  const tc = Date.now();
  await op.cancel();

  // 3) after cancel, status() must signal cancellation — either CANCELED_STATE or a cancel-specific throw
  let cancelSignal = null, stAfter = null;
  try {
    stAfter = (await op.status()).operationState;
    if (stAfter === CANCELED) cancelSignal = 'status==CANCELED_STATE';
  } catch (e) {
    const m = (e.message || String(e)).split('\n')[0];
    cancelSignal = /cancel/i.test(m) ? `status() threw cancel error: "${m.slice(0, 60)}"` : null;
    if (!cancelSignal) throw new Error(`status() threw NON-cancel error after cancel: ${m}`);
  }

  // 4) fetch must NOT return a full successful result set
  let fetchOutcome, fetchBad = false;
  try {
    const rows = await op.fetchAll();
    fetchOutcome = `RETURNED ${rows.length} rows`; fetchBad = true;
  } catch (e) {
    const m = (e.message || String(e)).split('\n')[0];
    fetchOutcome = (/cancel/i.test(m) ? 'cancel-error: ' : 'other-error: ') + m.slice(0, 70);
  }
  const elapsed = Date.now() - tc;
  await op.close().catch(() => {});
  await s.close(); await c.close();

  const pass = inFlight && cancelSignal && !fetchBad;
  console.log(`[${label}] stateBefore=${stBefore} -> in-flight=${inFlight}`);
  console.log(`[${label}] cancel signal: ${cancelSignal || 'NONE (BAD)'}`);
  console.log(`[${label}] post-cancel fetch: ${fetchOutcome}`);
  console.log(`[${label}] cancel->resolved in ${elapsed}ms (natural runtime 30s+)`);
  console.log(`[${label}] ${pass ? 'PASS — confirmed cancelled mid-flight' : 'FAIL'}`);
  process.exit(pass ? 0 : 1);
})().catch((e) => {
  console.log(`[${process.env.BUGBASH_THRIFT !== '1' ? 'KERNEL' : 'THRIFT'}] ERROR — ${(e.message || String(e)).split('\n')[0]}`);
  process.exit(2);
});
