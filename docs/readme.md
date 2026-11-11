# Getting started

The Databricks SQL Driver for Node.js (`@databricks/sql`) runs SQL on Databricks SQL warehouses and clusters. This page is a short tour; the [README](../README.md) covers installation and [CONNECTION_PARAMETERS.md](../CONNECTION_PARAMETERS.md) lists every option. If something goes wrong, see [Troubleshooting](troubleshooting.md).

## Requirements and installation

The driver needs Node.js 22.23.4+, 24.22.1+, or 26.12.1+ (see `engines` in [package.json](../package.json)).

```bash
npm i @databricks/sql
```

## Connect and run a query

```javascript
const { DBSQLClient } = require('@databricks/sql');

async function main() {
  const client = new DBSQLClient();
  await client.connect({
    host: process.env.DATABRICKS_HOST, // e.g. 'dbc-a1b2345c-d6e7.cloud.databricks.com', no 'https://'
    path: process.env.DATABRICKS_HTTP_PATH, // e.g. '/sql/1.0/warehouses/a1b234c567d8e9fa'
    token: process.env.DATABRICKS_TOKEN,
  });

  const session = await client.openSession();
  const operation = await session.executeStatement('SELECT :greeting AS greeting', {
    namedParameters: { greeting: 'Hello, World!' },
  });
  const rows = await operation.fetchAll(); // [{ greeting: 'Hello, World!' }]
  await operation.close();

  console.table(rows);

  await session.close();
  await client.close();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
```

Copy `host` and `path` from the **Connection details** tab of your SQL warehouse or cluster.

## Authentication

A personal access token in `token` is the default (`authType: 'access-token'`). For OAuth, set `authType: 'databricks-oauth'`:

- With `oauthClientId` and `oauthClientSecret`, the driver uses machine-to-machine OAuth with a service principal.
- Without a secret, the driver uses user-to-machine OAuth: it opens a browser and listens on `localhost:8030` for the redirect.

Token-based modes (`'static-token'`, `'external-token'`, `'token-provider'`, and token federation) are shown in [examples/tokenFederation](../examples/tokenFederation/README.md). The [authentication table](../CONNECTION_PARAMETERS.md#authentication) lists every auth option and which backend supports it.

## Sessions

`client.openSession()` accepts `initialCatalog`, `initialSchema`, `configuration` (session configuration such as `STATEMENT_TIMEOUT`), and `queryTags`. One client can open many sessions. Besides `executeStatement`, a session has metadata methods: `getCatalogs`, `getSchemas`, `getTables`, `getTableTypes`, `getColumns`, `getFunctions`, `getPrimaryKeys`, `getCrossReference`, `getTypeInfo`, and `getInfo`. See [examples/session.js](../examples/session.js) and [examples/session_params.js](../examples/session_params.js).

## Query parameters

Pass values with `namedParameters` (`:name` markers) or `ordinalParameters` (`?` markers), but not both in one statement. The driver infers each parameter's SQL type from its JavaScript value. To set the type explicitly, wrap the value:

```javascript
const { DBSQLParameter, DBSQLParameterType } = require('@databricks/sql');

await session.executeStatement('SELECT * FROM orders WHERE order_date >= ?', {
  ordinalParameters: [new DBSQLParameter({ type: DBSQLParameterType.DATE, value: '2024-01-31' })],
});
```

## Reading results

Each statement returns an operation. Rows are plain objects keyed by column name.

- `fetchAll()` returns every row.
- `fetchChunk({ maxRows })` with `hasMoreRows()` reads the result in pages.
- `iterateRows()` / `iterateChunks()` are async iterators, and `toNodeStream()` returns a Node.js `Readable`.
- `getResultMetadata()` returns the result schema.
- `cancel()` stops a running statement ([examples/cancel_operation.js](../examples/cancel_operation.js)).

By default, DECIMAL and BIGINT values are converted to JavaScript `number`, which can lose precision. Set `preserveBigNumericPrecision: true` in `connect()` to get exact strings for DECIMAL and `bigint` for BIGINT. [examples/data_types.js](../examples/data_types.js) shows how other SQL types are returned.

## Closing

Close operations, sessions, and the client when you're done. `session.close()` closes that session's open operations, and `client.close()` closes the client's open sessions. Long-running applications should `await client.close()` at shutdown so buffered [telemetry](TELEMETRY.md) is flushed.

## Backends

By default the driver uses the Thrift protocol. Passing `useKernel: true` to `connect()` opts into an experimental backend built on the Databricks SQL kernel (a native module installed through the driver's optional dependencies). This option is internal and unstable: it isn't in the published TypeScript types and may change or be removed without notice. Some options behave differently on each backend; [CONNECTION_PARAMETERS.md](../CONNECTION_PARAMETERS.md) has a Thrift and a Kernel column for every option.

## More resources

- [Troubleshooting](troubleshooting.md): logging, errors, proxies, TLS, and timeouts.
- [examples](../examples): runnable scripts, including [logging](../examples/logging.js) and [query tags](../examples/query_tags.js).
- [CHANGELOG.md](../CHANGELOG.md): release notes.
- [Databricks documentation for the Node.js driver](https://docs.databricks.com/en/dev-tools/nodejs-sql-driver.html).
