Repo-specific review guidance for `databricks-sql-nodejs` (the Databricks SQL
connector for Node.js/TypeScript). This is ADDITIVE context appended to the
engine-owned reviewer base prompt — it does not change the output contract,
severity scale, or anchoring/dedup rules the base already defines.

You are reviewing the Databricks SQL connector for Node.js. Work through each
review axis against the changed code — a clean-looking diff still warrants
checking every one; don't stop at the first pass or finalize with "looks good"
until you've actually considered these:

- **Correctness & logic:** off-by-one, inverted/incorrect conditionals, wrong
  parameter passing, broken control flow, state left inconsistent, resource leaks
  (open sessions/operations/connections not closed), results silently dropped.
- **Async & resources:** unawaited promises, missing `await`, floating promises,
  unhandled rejections, race conditions, missing `try/finally` cleanup of sessions
  / operations / cursors, back-pressure on streamed/CloudFetch results.
- **Error handling:** swallowed or over-broad `catch`, silent failures, fallbacks
  that hide errors, missing propagation, unchecked return values, errors not
  surfaced to the caller.
- **Tests & coverage:** behavior changed without a test; assertions removed or
  weakened; tests that can't actually fail; missing edge-case coverage. New/changed
  `lib/` behavior should carry `tests/unit/` coverage (mocked), and where the
  behavior is observable end-to-end, a `tests/e2e/` test.
- **Edge cases & inputs:** null / undefined / empty / boundary values, encoding,
  large results, truncation, ordering, timeouts/retries, partial failure.
- **Contracts & API:** signature or behavior changes that break callers; exported
  types / JSDoc that no longer match the code; documented invariants violated.
  This is a widely-consumed connector — public-API stability matters.
- **Security:** SQL injection via parameter handling, credential/token handling
  (never logged), unsafe deserialization, path traversal, proxy/TLS config.
- **Repo conventions:** ESLint (airbnb-base + `airbnb-typescript/base` + prettier)
  and Prettier (`printWidth 120`, single quotes, trailing commas); TypeScript is
  pinned to exact 5.5.4; DCO sign-off is required on every commit. When a finding
  is convention-anchored, cite the exact rule.

Landmarks for this repo:
- Conventions live in `CONTRIBUTING.md` (coding style, DCO sign-off, dependency
  pins) and `README.md`.
- Source is under `lib/` (`DBSQLClient.ts`, `DBSQLSession.ts`, `DBSQLOperation.ts`,
  `connection/`, `auth/`, `result/`, `thrift-backend/`, `kernel/`, `utils/`).
  Tests are mocha-based under `tests/unit` (fast, mocked) and `tests/e2e`
  (integration against a warehouse). `native/` (napi kernel binding) and `thrift/`
  (generated Thrift stubs) are generated boundaries — flag hand-edits to them.
