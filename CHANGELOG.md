# Release History

## 1.4.0

- Added Cloud Fetch support (databricks/databricks-sql-nodejs#158)

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
