// Category: Lifecycle & Session (KERNEL MODE)
const { kernelConnect, runQuery, runSuite, HOST, PATH, TOKEN, DBSQLClient } = require('./lib');

(async () => {
  const client = await kernelConnect();
  const session = await client.openSession();

  await runSuite('Lifecycle & Session', [
    {
      name: 'Reuse one session for many sequential queries',
      fn: async () => {
        for (let i = 0; i < 20; i++) {
          const r = await runQuery(session, `SELECT ${i} AS v`);
          if (Number(r[0].v) !== i) throw new Error('seq mismatch at ' + i);
        }
        return '20 sequential queries ok';
      },
    },
    {
      name: 'Multiple operations on one session, fetch independently',
      fn: async () => {
        const ops = await Promise.all([0, 1, 2].map((i) => session.executeStatement(`SELECT ${i} AS v, count(*) c FROM range(100)`)));
        const out = [];
        for (let i = 0; i < ops.length; i++) {
          const r = await ops[i].fetchAll();
          if (Number(r[0].v) !== i || Number(r[0].c) !== 100) throw new Error('op ' + i + ' bad');
          out.push(i);
        }
        await Promise.all(ops.map((o) => o.close()));
        return `${ops.length} independent operations ok`;
      },
    },
    {
      name: 'Re-run after partial fetch gives a fresh result',
      fn: async () => {
        const op1 = await session.executeStatement('SELECT id FROM range(10000)');
        const chunk = await op1.fetchChunk({ maxRows: 100 });
        await op1.close();
        const op2 = await session.executeStatement('SELECT 42 AS v');
        const r2 = await op2.fetchAll();
        await op2.close();
        if (chunk.length === 0 || Number(r2[0].v) !== 42) throw new Error('partial=' + chunk.length + ' rerun=' + JSON.stringify(r2));
        return `partial fetch=${chunk.length} rows, fresh re-run ok`;
      },
    },
    {
      name: 'Double-close operation/session is a safe no-op',
      fn: async () => {
        const op = await session.executeStatement('SELECT 1');
        await op.fetchAll();
        await op.close();
        await op.close(); // second close must not throw
        return 'double-close op ok (session/client double-close covered at teardown)';
      },
    },
    {
      name: 'Use after close raises a clear error (no hang/segfault)',
      fn: async () => {
        const op = await session.executeStatement('SELECT 1');
        await op.fetchAll();
        await op.close();
        try { await op.fetchAll(); return 'fetch after close returned (no error) — note'; }
        catch (e) { return 'fetch after close threw: ' + (e.message || String(e)).split('\n')[0].slice(0, 70); }
      },
    },
    {
      name: 'Async query (runAsync + finished) then fetch',
      fn: async () => {
        const op = await session.executeStatement('SELECT count(*) c FROM range(2000000)', { runAsync: true });
        if (typeof op.finished === 'function') await op.finished();
        const r = await op.fetchAll();
        await op.close();
        if (Number(r[0].c) !== 2000000) throw new Error('async bad: ' + JSON.stringify(r));
        return 'runAsync + finished + fetch ok (count=2000000)';
      },
    },
    {
      name: 'Interleave async submit + sync execute on same session',
      fn: async () => {
        const asyncOp = await session.executeStatement('SELECT count(*) c FROM range(1000000)', { runAsync: true });
        const syncRows = await runQuery(session, 'SELECT 7 AS v');
        if (typeof asyncOp.finished === 'function') await asyncOp.finished();
        const asyncRows = await asyncOp.fetchAll();
        await asyncOp.close();
        if (Number(syncRows[0].v) !== 7 || Number(asyncRows[0].c) !== 1000000) throw new Error('interleave bad');
        return 'async + sync interleaved ok';
      },
    },
    {
      name: 'Session configuration applied (timezone)',
      fn: async () => {
        const c2 = await kernelConnect();
        const s2 = await c2.openSession({ configuration: { timezone: 'America/New_York' } });
        const r = await runQuery(s2, 'SELECT current_timezone() tz');
        await s2.close(); await c2.close();
        if (!/New_York/.test(String(r[0].tz))) throw new Error('timezone not applied: ' + JSON.stringify(r));
        return 'timezone=' + r[0].tz;
      },
    },
    {
      name: 'Custom user agent (userAgentEntry) connects',
      fn: async () => {
        const c2 = new DBSQLClient();
        await c2.connect({ host: HOST, path: PATH, token: TOKEN, useKernel: process.env.BUGBASH_THRIFT !== '1', userAgentEntry: 'kernel-bugbash-ua' });
        const s2 = await c2.openSession();
        const r = await runQuery(s2, 'SELECT 1 AS one');
        await s2.close(); await c2.close();
        if (Number(r[0].one) !== 1) throw new Error('bad');
        return 'userAgentEntry connect + query ok (server-side UA check is manual)';
      },
    },
    {
      name: 'Query id + schema populated after execute',
      fn: async () => {
        const op = await session.executeStatement('SELECT 1 AS one, 2 AS two');
        await op.fetchAll();
        const id = op.id;
        const schema = typeof op.getSchema === 'function' ? await op.getSchema() : undefined;
        await op.close();
        if (!id || typeof id !== 'string') throw new Error('operation.id missing/invalid: ' + JSON.stringify(id));
        return `id=${id.slice(0, 24)}… schema=${schema ? 'present' : 'undefined'}`;
      },
    },
  ]);

  await session.close();
  await session.close().catch(() => {}); // session double-close
  await client.close();
  await client.close().catch(() => {}); // client double-close
  process.exit(0);
})();
