// Category: Query Execution (KERNEL MODE)
const { kernelConnect, runQuery, runSuite } = require('./lib');

(async () => {
  const client = await kernelConnect();
  const session = await client.openSession();

  await runSuite('Query Execution', [
    {
      name: 'Small inline results (no CloudFetch)',
      fn: async () => {
        const rows = await runQuery(session, 'SELECT id, id*2 AS dbl FROM range(5) ORDER BY id');
        if (rows.length !== 5 || Number(rows[4].dbl) !== 8) throw new Error('bad: ' + JSON.stringify(rows));
        return `${rows.length} rows inline`;
      },
    },
    {
      name: 'Large query triggers CloudFetch; verify full row count',
      fn: async () => {
        const N = 2_000_000;
        const op = await session.executeStatement(`SELECT id FROM range(${N})`);
        const rows = await op.fetchAll();
        await op.close();
        if (rows.length !== N) throw new Error(`expected ${N}, got ${rows.length}`);
        return `${rows.length} rows fetched`;
      },
    },
    {
      name: 'SET STATEMENT_TIMEOUT aborts a long query',
      fn: async () => {
        await runQuery(session, 'SET STATEMENT_TIMEOUT = 5');
        try {
          // per-row sha2 over billions of rows — not optimizable, reliably > 5s
          await runQuery(session, "SELECT count(*) FROM range(5000000000) WHERE sha2(cast(id AS string), 256) LIKE '%fffff%'");
          throw new Error('query was NOT aborted (no timeout error)');
        } catch (e) {
          const m = (e.message || String(e));
          if (/NOT aborted/.test(m)) throw e;
          if (!/timeout|timed out|exceeded|cancel|deadline/i.test(m)) throw new Error('aborted but unexpected error: ' + m.split('\n')[0]);
          return 'aborted with: ' + m.split('\n')[0].slice(0, 90);
        } finally {
          await runQuery(session, 'SET STATEMENT_TIMEOUT = 0').catch(() => {});
        }
      },
    },
    {
      name: 'Cancel a long-running query mid-flight',
      fn: async () => {
        // Non-optimizable long query (per-row sha2 over billions of rows). A
        // CROSS JOIN count folds to a constant and finishes in <2s, which would
        // make the cancel race a near-instant query — useless. runAsync so it is
        // genuinely in-flight when we cancel.
        const RUNNING = 1, PENDING = 5;
        const op = await session.executeStatement(
          "SELECT count(*) FROM range(5000000000) WHERE sha2(cast(id AS string),256) LIKE '%fffff%'",
          { runAsync: true },
        );
        await new Promise((r) => setTimeout(r, 2000));
        // Precondition: the query must really be in-flight, else the test proves nothing.
        const stBefore = (await op.status()).operationState;
        if (stBefore !== RUNNING && stBefore !== PENDING) {
          throw new Error(`not in-flight before cancel (state=${stBefore}); query finished/optimized — test meaningless`);
        }
        const tc = Date.now();
        await op.cancel();
        // After cancel the driver must signal cancellation (throws a cancel error
        // on status()/fetch). A full result set returning would be a FAIL.
        let signalled = false;
        try { await op.status(); } catch (e) { if (/cancel/i.test(e.message || '')) signalled = true; }
        let fetchReturned = false;
        try { await op.fetchAll(); fetchReturned = true; }
        catch (e) { if (/cancel/i.test(e.message || '')) signalled = true; }
        const elapsed = Date.now() - tc;
        await op.close().catch(() => {});
        if (fetchReturned) throw new Error('fetch returned a full result set after cancel — NOT cancelled');
        if (!signalled) throw new Error('no cancellation signal after cancel()');
        return `cancelled mid-flight (was RUNNING), resolved in ${elapsed}ms vs 30s+ natural`;
      },
    },
    {
      name: 'Many concurrent queries all complete',
      fn: async () => {
        const C = 80;
        const results = await Promise.all(
          Array.from({ length: C }, (_, i) =>
            (async () => {
              const op = await session.executeStatement(`SELECT ${i} AS k, count(*) AS c FROM range(100)`);
              const r = await op.fetchAll(); await op.close();
              // identity check: result i must carry its own k (no crossed responses);
              // count(*) of range(100) is 100.
              if (Number(r[0].k) !== i || Number(r[0].c) !== 100) throw new Error('bad concurrent result ' + JSON.stringify(r));
              return Number(r[0].k);
            })()
          )
        );
        const distinctComplete = results.length === C && results.slice().sort((a, b) => a - b).every((v, idx) => v === idx);
        if (!distinctComplete) throw new Error('missing/duplicate results: ' + JSON.stringify(results));
        return `${C} concurrent queries ok (all distinct k, count=100)`;
      },
    },
  ]);

  await session.close(); await client.close();
  process.exit(0);
})();
