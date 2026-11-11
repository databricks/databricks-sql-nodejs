# Driver pitfalls

Contract and platform facts that bot PRs have tripped over. The engineer bot
(author and follow-up) and the reviewer bot load this file as repo conventions.

## Public contracts

- `RetryError.payload` holds the node-fetch `Request`/`Response`, and
  `lib/hive/Commands/BaseCommand.ts` checks `payload.response instanceof Response`.
  Keep that shape; replacing it is a breaking change (#567 did, and it was
  reverted).
- Callers match warning and error codes, so export new public codes and types
  from `lib/index.ts`, as with `CIRCUIT_BREAKER_OPEN_CODE` (#565 review).

## Credentials

- node-fetch `FetchError` messages embed the request URL, and CloudFetch links
  are presigned with credentials in the query string. Never copy a network
  error's message into an error the caller sees; use its `code` or `type` (#567).

## Logging and local paths

- winston's File transport swallows errors opening its write stream. With an
  unwritable `filepath` the file never opens and, once the buffer fills, the
  logger stops writing to every transport, including the console. Test that many
  lines still reach the console, and treat the failure as covering every
  unwritable path, not only the reported one (#565).
- In 3.0.0, `filepath: 'logs/'` created a file named `logs`; treating a trailing
  separator as a directory crashes upgrades with ENOTDIR (#565).
- A synchronous open of a FIFO blocks until a reader appears, and Windows `mkdir`
  of a drive root throws EPERM (#565).
- Read-only file (EACCES) tests must skip when running as root, which ignores
  file permissions.
