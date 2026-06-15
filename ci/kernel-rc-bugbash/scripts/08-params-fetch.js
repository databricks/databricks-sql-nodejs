// Category: Params & Fetch (KERNEL MODE)
const { kernelConnect, runQuery, runSuite } = require('./lib');

(async () => {
  const client = await kernelConnect();
  const session = await client.openSession();

  await runSuite('Params & Fetch', [
    {
      name: 'Named params (:p) bind correctly',
      fn: async () => {
        const r = await runQuery(session, 'SELECT :p AS v, :q AS w', { namedParameters: { p: 42, q: 'hi' } });
        if (Number(r[0].v) !== 42 || r[0].w !== 'hi') throw new Error('bad: ' + JSON.stringify(r));
        return JSON.stringify(r[0]);
      },
    },
    {
      name: 'Positional params (?) bind correctly',
      fn: async () => {
        const r = await runQuery(session, 'SELECT ? AS a, ? AS b', { ordinalParameters: [7, 'x'] });
        if (Number(r[0].a) !== 7 || r[0].b !== 'x') throw new Error('bad: ' + JSON.stringify(r));
        return JSON.stringify(r[0]);
      },
    },
    {
      name: 'Bind scalar params of each type (no precision loss)',
      fn: async () => {
        const r = await runQuery(session,
          'SELECT :i AS i, :f AS f, :s AS s, :b AS b, :n AS n',
          { namedParameters: { i: 9007199254740991, f: 1.25, s: 'str', b: true, n: null } });
        const row = r[0];
        const ok = String(row.i) === '9007199254740991' && Math.abs(Number(row.f) - 1.25) < 1e-9 &&
          row.s === 'str' && (row.b === true || row.b === 'true') && row.n === null;
        if (!ok) throw new Error('precision/type loss: ' + JSON.stringify(row));
        return 'int/float/string/bool/null bound ok';
      },
    },
    {
      name: 'Zero-row result returns empty (no concat/arrow error)',
      fn: async () => {
        const op = await session.executeStatement('SELECT id FROM range(0)');
        const rows = await op.fetchAll();
        await op.close();
        if (!Array.isArray(rows) || rows.length !== 0) throw new Error('not empty: ' + JSON.stringify(rows));
        return 'empty array, no error';
      },
    },
    {
      name: 'Wide row (~200 columns) reads correctly',
      fn: async () => {
        const cols = Array.from({ length: 200 }, (_, i) => `${i} AS c${i}`).join(', ');
        const r = await runQuery(session, `SELECT ${cols}`);
        const keys = Object.keys(r[0]);
        if (keys.length !== 200 || Number(r[0].c199) !== 199) throw new Error('cols=' + keys.length);
        return `${keys.length} columns, c199=${r[0].c199}`;
      },
    },
    {
      name: 'NULL-heavy result maps NULLs to null',
      fn: async () => {
        const r = await runQuery(session,
          'SELECT CAST(NULL AS INT) a, CAST(NULL AS STRING) b, CAST(NULL AS DOUBLE) c, CAST(NULL AS BOOLEAN) d ' +
          'UNION ALL SELECT 1, \'x\', 2.0, true');
        if (r[0].a !== null || r[0].b !== null || r[0].c !== null || r[0].d !== null) throw new Error('nulls not null: ' + JSON.stringify(r[0]));
        if (Number(r[1].a) !== 1) throw new Error('second row bad');
        return 'NULLs → null across types';
      },
    },
    {
      name: 'Unicode / quotes / special chars roundtrip via param',
      fn: async () => {
        const vals = { a: "O'Brien", b: '😀🚀', c: '中文字符' };
        const r = await runQuery(session, 'SELECT :a AS a, :b AS b, :c AS c', { namedParameters: vals });
        if (r[0].a !== vals.a || r[0].b !== vals.b || r[0].c !== vals.c) throw new Error('mismatch: ' + JSON.stringify(r[0]));
        return 'exact roundtrip of quote/emoji/CJK';
      },
    },
    {
      name: 'Fetch large result in chunks; verify total across chunks',
      fn: async () => {
        const N = 5000;
        const op = await session.executeStatement(`SELECT id FROM range(${N})`);
        let total = 0, guard = 0;
        do {
          const chunk = await op.fetchChunk({ maxRows: 1000 });
          total += chunk.length;
        } while ((await op.hasMoreRows()) && ++guard < 100);
        await op.close();
        if (total !== N) throw new Error(`chunk total ${total} != ${N}`);
        return `${total} rows across chunks of 1000`;
      },
    },
    {
      name: 'Multi-statement script (SELECT 1; SELECT 2) behavior',
      fn: async () => {
        try {
          const r = await runQuery(session, 'SELECT 1 AS one; SELECT 2 AS two');
          return 'accepted; returned: ' + JSON.stringify(r);
        } catch (e) {
          return 'rejected with clear error: ' + (e.message || String(e)).split('\n')[0].slice(0, 80);
        }
      },
    },
  ]);

  await session.close(); await client.close();
  process.exit(0);
})();
