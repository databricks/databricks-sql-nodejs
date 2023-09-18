# Release History

## 1.5.0

### Highlights

- Added OAuth M2M support (databricks/databricks-sql-nodejs#168, databricks/databricks-sql-nodejs#177)
- Added named query parameters support (databricks/databricks-sql-nodejs#162, databricks/databricks-sql-nodejs#175)
- `runAsync` options is now deprecated (databricks/databricks-sql-nodejs#176)
- Added staging ingestion support (databricks/databricks-sql-nodejs#164)

### Databricks OAuth support

Databricks OAuth support added in v1.4.0 is now extended with M2M flow. To use OAuth instead of PAT, pass
a corresponding auth provider type and options to `DBSQL.connect`:

```ts
// instantiate DBSQLClient as usual

client.connect({
  // other mandatory options - e.g. host, path, etc.
  authType: 'databricks-oauth',
  oauthClientId: '...', // optional - overwrite default OAuth client ID
  azureTenantId: '...', // optional - provide custom Azure tenant ID
  persistence: ...,     // optional; user-provided storage for OAuth tokens, should implement OAuthPersistence interface
})
```

U2M flow involves user interaction - the library will open a browser tab asking user to log in. To use this flow,
no other options are required except of selecting auth provider type.

M2M flow does not require any user interaction, and therefore may be a good option, say, for scripting. To use this
flow, two extra options are required for `DBSQLClient.connect`: `oauthClientId` and `oauthClientSecret`.

Also see [Databricks docs](https://docs.databricks.com/en/dev-tools/auth.html#oauth-machine-to-machine-m2m-authentication)
for more details about Databricks OAuth.

### Named query parameters

v1.5.0 adds a support of [query parameters](https://docs.databricks.com/en/sql/language-manual/sql-ref-parameter-marker.html).
Currently only named parameters are supported.

Basic usage example:

```ts
// obtain session object as usual

const operation = session.executeStatement('SELECT :p1 AS "str_param", :p2 AS "number_param"', {
  namedParameters: {
    p1: 'Hello, World',
    p2: 3.14,
  },
});
```

The library will infer parameter types from passed primitive objects. Supported data types include booleans, various
numeric types (including native `BigInt` and `Int64` from `node-int64`), native `Date` type, and string.

It's also possible to explicitly specify parameter type by passing a `DBSQLParameter` instances instead of primitive
values. It also allows to use values that don't have a corresponding primitive representation:

```ts
import { ..., DBSQLParameter, DBSQLParameterType } from '@databricks/sql';

// obtain session object as usual

const operation = session.executeStatement('SELECT :p1 AS "date_param", :p2 AS "interval_type"', {
  namedParameters: {
    p1: new DBSQLParameter({
      value: new DBSQLParameter({
        value: new Date('2023-09-06T03:14:27.843Z'),
        type: DBSQLParameterType.DATE, // by default, Date objects are inferred as TIMESTAMP, this allows to override the type
      }),
    }),
    p2: new DBSQLParameter({
      value: new DBSQLParameter({
        value: 5, // INTERVAL '5' DAY
        type: DBSQLParameterType.INTERVALDAY
      }),
    }),
  },
});
```

Of course, you can mix primitive values and `DBSQLParameter` instances.

### `runAsync` deprecation

The `runAsync` is going to become unsupported soon, and we're deprecating it. It will remain available for the next
few releases, but from now it will be ignored and behave like it's always `true`. From user's point, the library
behaviour won't change, so if you used `runAsync` anywhere in your code - you can now just remove it.

### Data ingestion support

This feature allows to upload, retrieve and remove unity catalog volume files using SQL `PUT`, `GET` and `REMOVE` commands.

## 1.4.0

- Added Cloud Fetch support (databricks/databricks-sql-nodejs#158)
- Improved handling of closed sessions and operations (databricks/databricks-sql-nodejs#129).
  Now, when session gets closed, all operations associated with it are immediately closed.
  Similarly, if client gets closed - all associated sessions (and their operations) are closed as well.

**Notes**:

Cloud Fetch is disabled by default. To use it, pass `useCloudFetch: true`
to `IDBSQLSession.executeStatement()`. For example:

```ts
// obtain session object as usual
const operation = session.executeStatement(query, {
  runAsync: true,
  useCloudFetch: true,
});
```

Note that Cloud Fetch is effectively enabled only for really large datasets, so if
the query returns only few thousands records, Cloud Fetch won't be enabled no matter
what `useCloudFetch` setting is. Also gentle reminder that for large datasets
it's better to use `fetchChunk` instead of `fetchAll` to avoid OOM errors:

```ts
do {
  const chunk = await operation.fetchChunk({ maxRows: 100000 });
  // process chunk here
} while (await operation.hasMoreRows());
```

## 1.3.0

- Implemented automatic retry for some HTTP errors (429, 503) (databricks/databricks-sql-nodejs#127)
- Implemented request timeout + added option to configure it (databricks/databricks-sql-nodejs#148)
- Added OAuth (U2M) support for AWS and Azure (databricks/databricks-sql-nodejs#147 and databricks/databricks-sql-nodejs#154)
- Fixed bug: for Arrow results, `null` values were ignored (@ivan-parada databricks/databricks-sql-nodejs#151)

## 1.2.1

- Added Azure AD support (databricks/databricks-sql-nodejs#126)
- Improved direct results handling (databricks/databricks-sql-nodejs#134)
- Updated API endpoint references in docs and samples (databricks/databricks-sql-nodejs#137)
- Code refactoring to improve maintainability

## 1.2.0

- Added Apache Arrow support (databricks/databricks-sql-nodejs#94)
- Auth provider is now configurable (databricks/databricks-sql-nodejs#120)

## 1.1.1

- Fix: patch needed for improved error handling wasn't applied when installing 1.1.0

## 1.1.0

- Fix: now library will not attempt to parse column names and will use ones provided by server
  (databricks/databricks-sql-nodejs#84)
- Better error handling: more errors can now be handled in specific `.catch()` handlers instead of being
  emitted as a generic `error` event (databricks/databricks-sql-nodejs#99)
- Fixed error logging bug (attempt to serialize circular structures) (databricks/databricks-sql-nodejs#89)
- Fixed some minor bugs and regressions

## 1.0.0

- `DBSQLClient.openSession` now takes a limited set of options (`OpenSessionRequest` instead of Thrift's `TOpenSessionReq`)
- `DBSQLClient.openSession` now uses the latest protocol version by default
- Direct results feature is now available for all IOperation methods which support it. To enable direct results feature,
  `maxRows` option should be used
- Direct results became enabled by default. If `maxRows` is omitted - it will default to `100000`. To disable direct
  results, set `maxRows` to `null`
- `FunctionNameRequest` type renamed to `FunctionsRequest`
- `IDBSQLConnectionOptions` type renamed to `ConnectionOptions`
- `IFetchOptions` renamed to `FetchOptions`
- `DBSQLOperation.getSchema` will wait for operation completion, like `DBSQLOperation.fetchChunk`/`DBSQLOperation.fetchAll`.
  It also supports the same progress reporting options
- `runAsync` option is now available for all operations that support it
- Added logging functionality for logging on client side and added new optional logger param for DBSQLClient constructor
- Turned on Direct results feature by default
- Removed legacy Kerberos auth APIs

## 0.1.8-beta.2 (2022-09-08)

- Operations will wait for cluster to start instead of failing
- Added support for DirectResults, which speeds up data fetches by reducing the number of server roundtrips when possible
- `DBSQLOperation` interface simplified: `HiveUtils` were removed and replaced with new methods
  `DBSQLOperation.fetchChunk`/`DBSQLOperation.fetchAll`. New API implements all necessary waiting
  and data conversion routines internally
- Better TypeScript support
- Thrift definitions updated to support additional Databricks features
- User-agent string updated; a part of user-agent string is configurable through `DBSQLClient`'s `clientId` option
- Connection now uses keep-alive (not configurable at this moment)
- `DBSQLClient` now prepends slash to path when needed
- `DBSQLOperation`: default chunk size for data fetching increased from 100 to 100.000

### Upgrading

`DBSQLClient.utils` was permanently removed. Code which used `utils.waitUntilReady`, `utils.fetchAll`
and `utils.getResult` to get data should now be replaced with the single `DBSQLOperation.fetchAll` method.
Progress reporting, previously supported by `utils.waitUntilReady`, is now configurable via
`DBSQLOperation.fetchChunk`/`DBSQLOperation.fetchAll` options. `DBSQLOperation.setMaxRows` also became
an option of methods mentioned above.

## 0.1.8-beta.1 (2022-06-24)

- Initial release
