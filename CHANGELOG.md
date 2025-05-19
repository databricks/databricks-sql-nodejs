# Release History

## 1.11.0

- Enable cloud fetch by default ([databricks/databricks-sql-nodejs#287](https://github.com/databricks/databricks-sql-nodejs/issues/287))
- Added `useLZ4Compression` in `ExecuteStatementOptions` making compression configurable ([databricks/databricks-sql-nodejs#288](https://github.com/databricks/databricks-sql-nodejs/issues/288))
- Improve URL handling. Fix [#284](https://github.com/databricks/databricks-sql-nodejs/issues/284) ([databricks/databricks-sql-nodejs#290](https://github.com/databricks/databricks-sql-nodejs/issues/290))
- Add thrift protocol version handling for driver features ([databricks/databricks-sql-nodejs#292](https://github.com/databricks/databricks-sql-nodejs/issues/292))
- Cleanup deprecated/removed fields in thrift files ([databricks/databricks-sql-nodejs#295](https://github.com/databricks/databricks-sql-nodejs/issues/295))
- Add lenient LZ4 check to handle dependecy errors gracefully. Fixes [#289](https://github.com/databricks/databricks-sql-nodejs/issues/289) [#275](https://github.com/databricks/databricks-sql-nodejs/issues/275) [#266](https://github.com/databricks/databricks-sql-nodejs/issues/266) [#270](https://github.com/databricks/databricks-sql-nodejs/issues/270) ([databricks/databricks-sql-nodejs#298](https://github.com/databricks/databricks-sql-nodejs/issues/298))

## 1.10.0

- Rename `clientId` parameter to `userAgentEntry` in connect call to standardize across sql drivers (databricks/databricks-sql-nodejs#281)

## 1.9.0

- Support iterable interface for IOperation (databricks/databricks-sql-nodejs#252)
- Allow any number type (number, bigint, Int64) for `maxRows` and `queryTimeout` (databricks/databricks-sql-nodejs#255)
- Support streaming query results via Node.js streams (databricks/databricks-sql-nodejs#262)
- Add custom auth headers into cloud fetch request (databricks/databricks-sql-nodejs#267)
- Support OAuth on databricks.azure.cn (databricks/databricks-sql-nodejs#271)
- Fix: Fix the type check in polyfills.ts (databricks/databricks-sql-nodejs#254)

## 1.8.4

- Fix: proxy agent unintentionally overwrites protocol in URL (databricks/databricks-sql-nodejs#241)
- Improve `Array.at`/`TypedArray.at` polyfill (databricks/databricks-sql-nodejs#242 by @barelyhuman)
- UC Volume ingestion: stream files instead of loading them into memory (databricks/databricks-sql-nodejs#247)
- UC Volume ingestion: improve behavior on SQL `REMOVE` (databricks/databricks-sql-nodejs#249)
- Expose session and query ID (databricks/databricks-sql-nodejs#250)
- Make `lz4` module optional so package manager can skip it when cannot install (databricks/databricks-sql-nodejs#246)

## 1.8.3

- Improved retry behavior (databricks/databricks-sql-nodejs#230)
- Fix: in some cases library returned too many results (databricks/databricks-sql-nodejs#239)

## 1.8.2

Improved results handling when running queries against older DBR versions (databricks/databricks-sql-nodejs#232)

## 1.8.1

Security fixes:

> An issue in all published versions of the NPM package ip allows an attacker to execute arbitrary code and
> obtain sensitive information via the isPublic() function. This can lead to potential Server-Side Request
> Forgery (SSRF) attacks. The core issue is the function's failure to accurately distinguish between
> public and private IP addresses.

## 1.8.0

### Highlights

- Retry failed CloudFetch requests (databricks/databricks-sql-nodejs#211)
- Fixed compatibility issues with Node@14 (databricks/databricks-sql-nodejs#219)
- Support Databricks OAuth on Azure (databricks/databricks-sql-nodejs#223)
- Support Databricks OAuth on GCP (databricks/databricks-sql-nodejs#224)
- Support LZ4 compression for Arrow and CloudFetch results (databricks/databricks-sql-nodejs#216)
- Fix OAuth M2M flow on Azure (databricks/databricks-sql-nodejs#228)

### OAuth on Azure

Some Azure instances now support Databricks native OAuth flow (in addition to AAD OAuth). For a backward
compatibility, library will continue using AAD OAuth flow by default. To use Databricks native OAuth,
pass `useDatabricksOAuthInAzure: true` to `client.connect()`:

```ts
client.connect({
  // other options - host, port, etc.
  authType: 'databricks-oauth',
  useDatabricksOAuthInAzure: true,
  // other OAuth options if needed
});
```

Also, we fixed issue with AAD OAuth when wrong scopes were passed for M2M flow.

### OAuth on GCP

We enabled OAuth support on GCP instances. Since it uses Databricks native OAuth,
all the options are the same as for OAuth on AWS instances.

### CloudFetch improvements

Now library will automatically attempt to retry failed CloudFetch requests. Currently, the retry strategy
is quite basic, but it is going to be improved in the future.

Also, we implemented a support for LZ4-compressed results (Arrow- and CloudFetch-based). It is enabled by default,
and compression will be used if server supports it.

## 1.7.1

- Fix "Premature close" error which happened due to socket limit when intensively using library
  (databricks/databricks-sql-nodejs#217)

## 1.7.0

- Fixed behavior of `maxRows` option of `IOperation.fetchChunk()`. Now it will return chunks
  of requested size (databricks/databricks-sql-nodejs#200)
- Improved CloudFetch memory usage and overall performance (databricks/databricks-sql-nodejs#204,
  databricks/databricks-sql-nodejs#207, databricks/databricks-sql-nodejs#209)
- Remove protocol version check when using query parameters (databricks/databricks-sql-nodejs#213)
- Fix `IOperation.hasMoreRows()` behavior to avoid fetching data beyond the end of dataset.
  Also, now it will work properly prior to fetching first chunk (databricks/databricks-sql-nodejs#205)

## 1.6.1

- Make default logger singleton (databricks/databricks-sql-nodejs#199)
- Enable `canUseMultipleCatalogs` option when creating session (databricks/databricks-sql-nodejs#203)

## 1.6.0

### Highlights

- Added proxy support (databricks/databricks-sql-nodejs#193)
- Added support for inferring NULL values passed as query parameters (databricks/databricks-sql-nodejs#189)
- Fixed bug with NULL handling for Arrow results (databricks/databricks-sql-nodejs#195)

### Proxy support

This feature allows to pass through proxy all the requests library makes. By default, proxy is disabled.
To enable proxy, pass a configuration object to `DBSQLClient.connect`:

```ts
client.connect({
    // pass host, path, auth options as usual
    proxy: {
      protocol: 'http',  // supported protocols: 'http', 'https', 'socks', 'socks4', 'socks4a', 'socks5', 'socks5h'
      host: 'localhost', // proxy host (string)
      port: 8070,        // proxy port (number)
      auth: {            // optional proxy basic auth config
        username: ...
        password: ...
      },
    },
  })
```

**Note**: using proxy settings from environment variables is currently not supported

## 1.5.0

### Highlights

- Added OAuth M2M support (databricks/databricks-sql-nodejs#168, databricks/databricks-sql-nodejs#177)
- Added named query parameters support (databricks/databricks-sql-nodejs#162, databricks/databricks-sql-nodejs#175)
- `runAsync` options is now deprecated (databricks/databricks-sql-nodejs#176)
- Added staging ingestion support (databricks/databricks-sql-nodejs#164)

### Databricks OAuth support

Databricks OAuth support added in v1.4.0 is now extended with M2M flow. To use OAuth instead of PAT, pass
a corresponding auth provider type and options to `DBSQLClient.connect`:

```ts
// instantiate DBSQLClient as usual

client.connect({
  // provide other mandatory options as usual - e.g. host, path, etc.
  authType: 'databricks-oauth',
  oauthClientId: '...', // optional - overwrite default OAuth client ID
  azureTenantId: '...', // optional - provide custom Azure tenant ID
  persistence: ...,     // optional - user-provided storage for OAuth tokens, should implement OAuthPersistence interface
})
```

U2M flow involves user interaction - the library will open a browser tab asking user to log in. To use this flow,
no other options are required except for `authType`.

M2M flow does not require any user interaction, and therefore is a good option, say, for scripting. To use this
flow, two extra options are required for `DBSQLClient.connect`: `oauthClientId` and `oauthClientSecret`.

Also see [Databricks docs](https://docs.databricks.com/en/dev-tools/auth.html#oauth-machine-to-machine-m2m-authentication)
for more details about Databricks OAuth.

### Named query parameters

v1.5.0 adds a support for [query parameters](https://docs.databricks.com/en/sql/language-manual/sql-ref-parameter-marker.html).
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

It's also possible to explicitly specify the parameter type by passing `DBSQLParameter` instances instead of primitive
values. It also allows one to use values that don't have a corresponding primitive representation:

```ts
import { ..., DBSQLParameter, DBSQLParameterType } from '@databricks/sql';

// obtain session object as usual

const operation = session.executeStatement('SELECT :p1 AS "date_param", :p2 AS "interval_type"', {
  namedParameters: {
    p1: new DBSQLParameter({
      value: new Date('2023-09-06T03:14:27.843Z'),
      type: DBSQLParameterType.DATE, // by default, Date objects are inferred as TIMESTAMP, this allows to override the type
    }),
    p2: new DBSQLParameter({
      value: 5, // INTERVAL '5' DAY
      type: DBSQLParameterType.INTERVALDAY
    }),
  },
});
```

Of course, you can mix primitive values and `DBSQLParameter` instances.

### `runAsync` deprecation

Starting with this release, the library will execute all queries asynchronously, so we have deprecated
the `runAsync` option. It will be completely removed in v2. So you should not use it going forward and remove all
the usages from your code before version 2 is released. From user's perspective the library behaviour won't change.

### Data ingestion support

This feature allows you to upload, retrieve, and remove unity catalog volume files using SQL `PUT`, `GET` and `REMOVE` commands.

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
