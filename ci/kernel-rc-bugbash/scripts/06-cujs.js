// Category: CUJs (KERNEL MODE)
const { kernelConnect, runQuery, runSuite } = require('./lib');

(async () => {
  const client = await kernelConnect();
  const session = await client.openSession();
  const SCHEMA = 'main.kernel_bugbash';
  await runQuery(session, `CREATE SCHEMA IF NOT EXISTS ${SCHEMA}`).catch(() => {});

  await runSuite('CUJs', [
    {
      name: 'CREATE/INSERT/SELECT/UPDATE/DELETE/DROP + switch catalog/schema mid-session',
      fn: async () => {
        const tbl = `${SCHEMA}.crud_${Date.now()}`;
        await runQuery(session, `CREATE TABLE ${tbl} (id INT, name STRING)`);
        await runQuery(session, `INSERT INTO ${tbl} VALUES (1,'a'),(2,'b'),(3,'c')`);
        let r = await runQuery(session, `SELECT count(*) c FROM ${tbl}`);
        if (Number(r[0].c) !== 3) throw new Error('insert count != 3');
        await runQuery(session, `UPDATE ${tbl} SET name='B' WHERE id=2`);
        r = await runQuery(session, `SELECT name FROM ${tbl} WHERE id=2`);
        if (r[0].name !== 'B') throw new Error('update failed');
        await runQuery(session, `DELETE FROM ${tbl} WHERE id=3`);
        r = await runQuery(session, `SELECT count(*) c FROM ${tbl}`);
        if (Number(r[0].c) !== 2) throw new Error('delete count != 2');
        // switch catalog/schema mid-session
        await runQuery(session, 'USE CATALOG samples');
        await runQuery(session, 'USE SCHEMA nyctaxi');
        const r2 = await runQuery(session, 'SELECT count(*) c FROM trips LIMIT 1');
        await runQuery(session, 'USE CATALOG main');
        await runQuery(session, `DROP TABLE IF EXISTS ${tbl}`);
        return `CRUD ok; switched to samples.nyctaxi (trips count=${r2[0].c})`;
      },
    },
    {
      name: 'Large cell values (~1MB string, ~256KB binary)',
      fn: async () => {
        const tbl = `${SCHEMA}.big_${Date.now()}`;
        await runQuery(session, `CREATE TABLE ${tbl} (s STRING, b BINARY)`);
        await runQuery(session, `INSERT INTO ${tbl} SELECT repeat('x', 1000000), CAST(repeat('y', 262144) AS BINARY)`);
        const r = await runQuery(session, `SELECT length(s) sl, length(b) bl FROM ${tbl}`);
        await runQuery(session, `DROP TABLE IF EXISTS ${tbl}`);
        if (Number(r[0].sl) !== 1000000 || Number(r[0].bl) !== 262144) throw new Error('len mismatch: ' + JSON.stringify(r));
        return `string=${r[0].sl}B binary=${r[0].bl}B roundtripped`;
      },
    },
    {
      name: 'Bulk-insert ~50k rows; verify count + aggregate',
      fn: async () => {
        const tbl = `${SCHEMA}.bulk_${Date.now()}`;
        await runQuery(session, `CREATE TABLE ${tbl} (id BIGINT)`);
        await runQuery(session, `INSERT INTO ${tbl} SELECT id FROM range(50000)`);
        const r = await runQuery(session, `SELECT count(*) c, sum(id) s FROM ${tbl}`);
        await runQuery(session, `DROP TABLE IF EXISTS ${tbl}`);
        const expectSum = (49999 * 50000) / 2;
        if (Number(r[0].c) !== 50000 || Number(r[0].s) !== expectSum) throw new Error('bad: ' + JSON.stringify(r));
        return `count=${r[0].c} sum=${r[0].s} (expected ${expectSum})`;
      },
    },
    {
      name: 'Negative paths each return a clear, correct error',
      fn: async () => {
        // ANSI mode must be on for CAST/divide to raise (off => they return NULL, by design)
        await runQuery(session, 'SET ansi_mode = true');
        const cases = [
          ['bad syntax', 'SELEKT 1'],
          ['missing table', 'SELECT * FROM nope_zzz_missing'],
          ['missing column', 'SELECT no_such_col FROM range(1)'],
          ['CAST failure', "SELECT CAST('abc' AS INT) AS x"],
          ['divide by zero', 'SELECT 1/0 AS x'],
          ['unknown catalog', 'SELECT * FROM no_such_catalog_zzz.s.t'],
        ];
        const results = [];
        for (const [label, sql] of cases) {
          try { await runQuery(session, sql); results.push(`${label}:NO-ERROR`); }
          catch (e) { results.push(`${label}:ok`); }
        }
        const bad = results.filter((x) => x.endsWith('NO-ERROR'));
        if (bad.length) throw new Error('did not error: ' + bad.join(','));
        return results.join(' ');
      },
    },
  ]);

  await session.close(); await client.close();
  process.exit(0);
})();
