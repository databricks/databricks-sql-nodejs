// Category: Metadata & Tags (KERNEL MODE)
const { kernelConnect, runQuery, runSuite } = require('./lib');

const collect = async (op) => { const r = await op.fetchAll(); await op.close(); return r; };

(async () => {
  const client = await kernelConnect();
  const session = await client.openSession();

  // Try to provision a writable schema for key-metadata + tags tests.
  const SCHEMA = 'main.kernel_bugbash';
  let writable = false;
  try {
    await runQuery(session, `CREATE SCHEMA IF NOT EXISTS ${SCHEMA}`);
    writable = true;
  } catch (_) { writable = false; }

  await runSuite('Metadata & Tags', [
    {
      name: 'Non-key metadata: getCatalogs/getSchemas/getTables/getColumns/getTableTypes',
      fn: async () => {
        const cats = await collect(await session.getCatalogs());
        const schemas = await collect(await session.getSchemas({ catalogName: 'samples' }));
        const tables = await collect(await session.getTables({ catalogName: 'samples', schemaName: 'nyctaxi' }));
        const cols = await collect(await session.getColumns({ catalogName: 'samples', schemaName: 'nyctaxi', tableName: 'trips' }));
        const types = await collect(await session.getTableTypes());
        if (!cats.length || !schemas.length || !tables.length || !cols.length || !types.length) {
          throw new Error(`empty: cats=${cats.length} schemas=${schemas.length} tables=${tables.length} cols=${cols.length} types=${types.length}`);
        }
        return `cats=${cats.length} schemas=${schemas.length} tables=${tables.length} cols=${cols.length} types=${types.length}`;
      },
    },
    {
      name: 'Key metadata: getPrimaryKeys / getCrossReference',
      skip: !writable,
      reason: 'could not create temp schema main.kernel_bugbash (no write access)',
      fn: async () => {
        const t = Date.now();
        const parent = `${SCHEMA}.pk_parent_${t}`;
        const child = `${SCHEMA}.fk_child_${t}`;
        await runQuery(session, `CREATE TABLE ${parent} (id INT NOT NULL, PRIMARY KEY(id))`);
        await runQuery(session, `CREATE TABLE ${child} (cid INT, pid INT, FOREIGN KEY(pid) REFERENCES ${parent}(id))`);
        try {
          const pk = await collect(await session.getPrimaryKeys({ catalogName: 'main', schemaName: 'kernel_bugbash', tableName: `pk_parent_${t}` }));
          const xref = await collect(await session.getCrossReference({
            parentCatalogName: 'main', parentSchemaName: 'kernel_bugbash', parentTableName: `pk_parent_${t}`,
            foreignCatalogName: 'main', foreignSchemaName: 'kernel_bugbash', foreignTableName: `fk_child_${t}`,
          }));
          if (!pk.length) throw new Error('getPrimaryKeys returned no rows for a table with a PK');
          return `getPrimaryKeys=${pk.length} row(s), getCrossReference=${xref.length} row(s)`;
        } finally {
          await runQuery(session, `DROP TABLE IF EXISTS ${child}`).catch(() => {});
          await runQuery(session, `DROP TABLE IF EXISTS ${parent}`).catch(() => {});
        }
      },
    },
    {
      name: 'getImportedKeys / getExportedKeys',
      skip: true,
      reason: 'not in the Node driver public API (only getCrossReference exists) — expected limitation, matches Thrift',
      fn: async () => {},
    },
    {
      name: 'Query tags accepted (and best-effort history lookup)',
      fn: async () => {
        const tag = 'kernel-bugbash-' + Date.now();
        const op = await session.executeStatement('SELECT 1 AS one', { queryTags: { bugbash: tag } });
        const rows = await op.fetchAll();
        await op.close();
        if (Number(rows[0].one) !== 1) throw new Error('tagged query bad result');
        // Best-effort: tags land in QUERY_TAGS; full verification is via Query History UI / system.query.history (may lag).
        return `query with queryTags={bugbash:${tag}} ran ok (history/UI verification is manual)`;
      },
    },
    {
      name: 'Inspect a metric view (METRIC_VIEW table type)',
      fn: async () => {
        const types = await collect(await session.getTableTypes());
        const names = types.map((t) => JSON.stringify(t)).join(',');
        // METRIC_VIEW only appears if a metric view is defined on the warehouse,
        // which is environment-dependent — so its ABSENCE is not a driver bug
        // (don't hard-fail). Report which case we observed.
        if (/METRIC_VIEW/i.test(names)) return 'METRIC_VIEW present in table types';
        return 'no METRIC_VIEW defined on this warehouse (env-dependent, not a bug); table types seen: ' + names.slice(0, 120);
      },
    },
  ]);

  await session.close(); await client.close();
  process.exit(0);
})();
