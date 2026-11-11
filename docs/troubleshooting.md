# Troubleshooting

This page covers common problems with the Databricks SQL Driver for Node.js. [Getting started](readme.md) is the overview, and [CONNECTION_PARAMETERS.md](../CONNECTION_PARAMETERS.md) is the full option reference. "Kernel" below means the experimental backend you get with `useKernel: true`; everything else applies to the default Thrift backend.

## Turn on debug logging

Start here. Pass a `DBSQLLogger` to the client:

```javascript
const { DBSQLClient, DBSQLLogger, LogLevel } = require('@databricks/sql');

const logger = new DBSQLLogger({ level: LogLevel.debug, filepath: 'databricks-sql.log' });
const client = new DBSQLClient({ logger });
```

- The levels are `error`, `warn`, `info` (the default), and `debug`. Without a logger, the driver logs JSON lines to the console at `info`. To quiet it, pass `new DBSQLLogger({ level: LogLevel.error })` as the client's logger.
- `filepath` also writes the log to that file. Point it at a writable file path, not a directory.
- `logger.setLevel(LogLevel.debug)` changes the level at runtime ([examples/logging.js](../examples/logging.js)).
- A custom logger is any object with a `log(level, message)` method, so you can route driver logs into your application's logger.
- On Kernel, native kernel logs go to the same logger, prefixed with `[kernel <target>]`, at the logger's level. The kernel log sink is shared by the whole process, so with several clients the most recently connected client's logger receives them.

Debug logs include host names and session and statement IDs. Review them before sharing.

## Reading errors

The driver's error classes aren't exported, so check error properties (`name`, `message`, `statusCode`, `sqlState`, `errorCode`) rather than using `instanceof`. These are the errors you'll typically see:

| Situation                       | Thrift                                                                                                                                    | Kernel                                                                                                                 |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Invalid or expired token        | `Received a response with a bad HTTP status code: 401` (or `403`), with `statusCode` set                                                  | `name: 'AuthenticationError'`, a message such as `HTTP 403: ... Invalid access token`, and `kernelMetadata.httpStatus` |
| Wrong `path`                    | `Received a response with a bad HTTP status code: 404`                                                                                    | `HTTP 404: The warehouse <id> was not found.`                                                                          |
| Unknown host or network failure | `FetchError` with a `code` such as `ENOTFOUND`, `ECONNREFUSED`, or `ECONNRESET`                                                           | `HTTP request failed after N attempts: error sending request for url (...)`                                            |
| Untrusted TLS certificate       | `FetchError` with a `code` such as `SELF_SIGNED_CERT_IN_CHAIN`, `UNABLE_TO_GET_ISSUER_CERT_LOCALLY`, or `UNABLE_TO_VERIFY_LEAF_SIGNATURE` | The same message as a network failure; see [TLS and certificates](#tls-and-certificates)                               |
| SQL error from the server       | The server's message, `errorCode: 'ERROR'`, and `sqlState` (for example `42P01` for a missing table)¹                                     | Same as Thrift                                                                                                         |
| Statement timeout               | `Statement has timed out after N seconds.` with `sqlState` `57KD0`¹                                                                       | Same as Thrift                                                                                                         |
| Retries exhausted               | `Hive driver: <status> when connecting to resource. Max retry count exceeded.` (or `Retry timeout exceeded.`)                             | Kernel's own error message                                                                                             |

¹ In 3.0.0 and earlier, Thrift errors expose the SQLSTATE only as `error.response.sqlState`.

The client is an `EventEmitter`. Thrift transport errors that aren't tied to a pending call are logged and emitted as `error` events, so register `client.on('error', ...)` in long-running applications. Older versions of this page recommended reconnecting on the client's `close` event. That's no longer needed: the driver uses independent HTTPS requests, so there is no persistent connection to re-establish and the transport doesn't emit `close`.

## Connection settings

- `host` is the bare host name from **Connection details**, without `https://` or a path.
- `path` is the HTTP path, for example `/sql/1.0/warehouses/<id>`. A wrong path returns HTTP 404.
- Thrift reads the port from `port` (default `443`). Kernel ignores `port`; put a non-default port in `host` instead (`host: 'example.com:8443'`).

## Authentication

- **Personal access tokens:** check that the token hasn't expired or been revoked, and that it belongs to the workspace in `host`.
- **OAuth user-to-machine** (`authType: 'databricks-oauth'` without `oauthClientSecret`) opens a browser and waits for the redirect on `localhost:8030`. If that port is taken, Thrift fails with `Failed to start server: all ports are in use`. On servers and in CI, use machine-to-machine OAuth (`oauthClientId` plus `oauthClientSecret`) instead.
- **Azure:** Thrift uses Microsoft Entra ID OAuth by default. Set `useDatabricksOAuthInAzure: true` to use Databricks OAuth instead. Thrift also throws `OAuth is not supported for <host>` when `host` isn't a recognized Databricks domain, for example a custom DNS alias.
- **Kernel** supports `'access-token'`, `'static-token'`, and `'databricks-oauth'`. It rejects `'custom'`, `'token-provider'`, and `'external-token'` with `kernel backend: unsupported auth mode`. See [authentication](../CONNECTION_PARAMETERS.md#authentication) for the per-backend details.

## Proxies

Configure the proxy explicitly with the `proxy` option:

```javascript
await client.connect({
  host,
  path,
  token,
  proxy: {
    protocol: 'http',
    host: 'proxy.example.com',
    port: 8080,
    auth: { username: 'user', password: 'pass' }, // optional
  },
});
```

- Thrift accepts `http`, `https`, `socks`, `socks4`, `socks4a`, `socks5`, and `socks5h`. Kernel supports only HTTP and HTTPS proxies.
- Thrift ignores the `HTTPS_PROXY` and `HTTP_PROXY` environment variables. Kernel currently picks up `HTTPS_PROXY` when `proxy` isn't set.
- If the proxy inspects TLS traffic, also follow the next section.

## TLS and certificates

Both backends verify the server certificate by default. Certificate errors usually mean a TLS-inspecting proxy or a private CA sits between you and Databricks.

- Pass the CA certificate (PEM, as a string or `Buffer`) that issued the proxy's or server's certificate as `customCaCert`. Both backends add it to their default trust roots.
- Thrift also honors the `NODE_EXTRA_CA_CERTS` environment variable. When `customCaCert` is set, Thrift builds its trust store from Node's bundled roots, `NODE_EXTRA_CA_CERTS`, and `customCaCert`, so roots that Node would otherwise load from the operating system (`--use-system-ca`) aren't included.
- Kernel has its own TLS stack and doesn't read `NODE_EXTRA_CA_CERTS` or `--use-system-ca`. Use `customCaCert`. Because Kernel reports certificate failures as `error sending request for url`, try the same connection on Thrift (without `useKernel`) to see the certificate error code.
- For mutual TLS, pass `clientCert` and `clientKey` together. The driver rejects one without the other. PKCS#8 keys (`BEGIN PRIVATE KEY`) are recommended for Kernel.
- `checkServerCertificate: false` turns verification off completely and exposes your token to man-in-the-middle attacks. Use it only for local debugging, never in production.

## Timeouts and retries

- **Long-running queries:** on SQL warehouses, limit run time with `STATEMENT_TIMEOUT`, in seconds. Set it per session with `openSession({ configuration: { STATEMENT_TIMEOUT: '600' } })`, or run `SET STATEMENT_TIMEOUT = 600`. Both backends honor it. The `queryTimeout` statement option (seconds) applies only to clusters on Thrift. To stop a query from the client, call `operation.cancel()`.
- **`socketTimeout`** (milliseconds) limits how long the driver waits on a single HTTP request. It defaults to 15 minutes on Thrift and 120 seconds on Kernel. On Thrift a timeout surfaces as `Request timed out`; `0` disables the timeout on Thrift, while Kernel ignores `0` and keeps its default.
- **Retries** are controlled by `retryMaxAttempts` (total attempts, default `5`), `retriesTimeout` (default 15 minutes), `retryDelayMin` (default 1 second), and `retryDelayMax` (default 60 seconds); all times are in milliseconds. Thrift retries HTTP 429, 5xx (except 501), and transient network errors with exponential backoff, but only for requests that are safe to repeat: status polling, metadata calls, closing operations and sessions, and CloudFetch downloads. Opening a session and executing a statement aren't retried. Kernel runs its own retry loop using the same settings.

## Kernel native module

Kernel loads a native module from the platform-specific optional dependency `@databricks/databricks-sql-kernel-<platform>`. Published builds cover Linux x64 and arm64 (glibc and musl), macOS x64 and arm64, and Windows x64 and arm64.

- `kernel native binding not installed for platform ...`: the optional dependency is missing. Don't install with `--omit=optional` (or `--no-optional`), and run `npm install` on the target OS and architecture. For example, don't copy `node_modules` from a macOS host into a Linux container.
- `kernel native binding present but failed to dlopen ...`: usually a glibc/musl or CPU architecture mismatch. Reinstall on the target platform.
- Without `useKernel`, Thrift still switches to Kernel automatically when a warehouse rejects Thrift sessions with SQLSTATE `KP001`. That fallback also needs the native module.

## Results

If DECIMAL or BIGINT values lose precision, set `preserveBigNumericPrecision: true` in `connect()`. DECIMAL values then come back as strings and BIGINT values as `bigint`.

## Telemetry

Set `telemetryEnabled: false` in `connect()`, or set the environment variable `DATABRICKS_TELEMETRY_DISABLED=1`, to turn telemetry off. See [TELEMETRY.md](TELEMETRY.md) for details and its [troubleshooting section](TELEMETRY.md#troubleshooting).

## Reporting a problem

[Open an issue](https://github.com/databricks/databricks-sql-nodejs/issues) and include:

- the driver version (`npm ls @databricks/sql`), the Node.js version, and the OS
- whether `useKernel` is set
- a minimal script that reproduces the problem
- a debug log with tokens and other secrets removed
