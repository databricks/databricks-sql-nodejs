# Release History

## 1.0.0 (Unreleased)

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
