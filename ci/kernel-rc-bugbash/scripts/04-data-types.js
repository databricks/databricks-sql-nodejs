// Category: Data Types (KERNEL MODE). Reads values and verifies them.
const { kernelConnect, runQuery, runSuite } = require('./lib');

(async () => {
  const client = await kernelConnect();
  const session = await client.openSession();

  await runSuite('Data Types', [
    {
      name: 'Primitive types (tinyint/int/bigint/float/double/decimal/string/bool/date)',
      fn: async () => {
        const r = (await runQuery(session,
          "SELECT CAST(1 AS TINYINT) t, CAST(2 AS INT) i, CAST(3 AS BIGINT) b, " +
          "CAST(1.5 AS FLOAT) f, CAST(2.25 AS DOUBLE) d, CAST(3.14 AS DECIMAL(6,2)) dec, " +
          "'hello' s, true bl, DATE'2026-01-01' dt"))[0];
        const ok = Number(r.t) === 1 && Number(r.i) === 2 && Number(r.b) === 3 &&
          Math.abs(Number(r.f) - 1.5) < 1e-6 && Math.abs(Number(r.d) - 2.25) < 1e-9 &&
          Number(r.dec) === 3.14 && r.s === 'hello' && (r.bl === true || r.bl === 'true');
        if (!ok) throw new Error('value mismatch: ' + JSON.stringify(r));
        return 'date=' + JSON.stringify(r.dt);
      },
    },
    {
      name: 'Complex types ARRAY / MAP / STRUCT (incl. nested)',
      fn: async () => {
        const r = (await runQuery(session,
          "SELECT array(1,2,3) arr, map('a',1,'b',2) m, named_struct('x',1,'y','z') st, " +
          "array(named_struct('n',1), named_struct('n',2)) nested"))[0];
        // Actually verify the values, not just that the query ran.
        const arrOk = Array.isArray(r.arr) && r.arr.length === 3 && r.arr.map(Number).join(',') === '1,2,3';
        const mapOk = r.m != null && Number(r.m.a) === 1 && Number(r.m.b) === 2;
        const stOk = r.st != null && Number(r.st.x) === 1 && r.st.y === 'z';
        const nestedOk = Array.isArray(r.nested) && r.nested.length === 2 &&
          Number(r.nested[0].n) === 1 && Number(r.nested[1].n) === 2;
        if (!(arrOk && mapOk && stOk && nestedOk)) {
          throw new Error('complex value mismatch: ' + JSON.stringify(r));
        }
        return 'arr=' + JSON.stringify(r.arr) + ' map=' + JSON.stringify(r.m) +
          ' struct=' + JSON.stringify(r.st) + ' nested=' + JSON.stringify(r.nested);
      },
    },
    {
      name: 'TIMESTAMP / TIMESTAMP_NTZ / DATE',
      fn: async () => {
        const r = (await runQuery(session,
          "SELECT TIMESTAMP'2026-01-02 03:04:05' ts, CAST(TIMESTAMP'2026-01-02 03:04:05' AS TIMESTAMP_NTZ) ntz, DATE'2026-02-03' dt"))[0];
        if (r.ts == null || r.ntz == null || r.dt == null) throw new Error('null ts/ntz/dt: ' + JSON.stringify(r));
        // Verify the actual instant/date, not just non-null.
        const iso = (v) => (v instanceof Date ? v.toISOString() : String(v));
        if (!/2026-01-02T03:04:05/.test(iso(r.ts))) throw new Error('ts wrong: ' + iso(r.ts));
        if (!/2026-01-02T03:04:05/.test(iso(r.ntz))) throw new Error('ntz wrong: ' + iso(r.ntz));
        if (!/2026-02-03/.test(iso(r.dt))) throw new Error('dt wrong: ' + iso(r.dt));
        return 'ts=' + JSON.stringify(r.ts) + ' ntz=' + JSON.stringify(r.ntz) + ' dt=' + JSON.stringify(r.dt);
      },
    },
    {
      name: 'VARIANT (parse_json / semi-structured)',
      fn: async () => {
        const r = (await runQuery(session, "SELECT parse_json('{\"a\":1,\"b\":[2,3]}') v"))[0];
        if (r.v == null) throw new Error('null variant');
        // VARIANT surfaces as a JSON string; verify it parses to the right value.
        const v = typeof r.v === 'string' ? JSON.parse(r.v) : r.v;
        if (Number(v.a) !== 1 || JSON.stringify(v.b) !== '[2,3]') throw new Error('variant value mismatch: ' + JSON.stringify(r.v));
        return 'variant=' + JSON.stringify(r.v);
      },
    },
    {
      name: 'GEOMETRY / GEOGRAPHY (WKT/WKB)',
      fn: async () => {
        try {
          const r = (await runQuery(session, "SELECT st_geomfromtext('POINT(1 2)') g"))[0];
          return 'geom=' + JSON.stringify(r.g);
        } catch (e) {
          // GEOMETRY/GEOGRAPHY may not be enabled on this warehouse/DBR
          throw new Error('GEO not available: ' + (e.message || String(e)).split('\n')[0].slice(0, 80));
        }
      },
    },
  ]);

  await session.close(); await client.close();
  process.exit(0);
})();
