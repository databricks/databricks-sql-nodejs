# Lessons

<!-- distilled-through: none -->

## Configuration and authentication
- L-0001 (3×, #457): Trace option support through DBSQLClient, auth adapters and each transport. Type declarations and class comments do not establish forwarding; shared preparation can precede backend-specific filtering.
- L-0002 (2×, #501 #505): New auth branches must preserve ambiguity, credential-exclusion and persistence guards, including checks bypassed by early returns.
- L-0003 (2×, #501 #505): Normalize native-bound strings with isBlankOrReserved so blank, placeholder and whitespace values do not select unintended auth modes.
- L-0004 (1×, #501): Kernel federation behavior differs from Thrift flags: verify native support and document always-on/ignored flags and federationClientId routing explicitly.
- L-0005 (2×, #505): Fully normalize hosts before domain matching, including scheme, path and port. Keep Azure suffix lists and auth flag semantics aligned across backends or document deliberate differences.
- L-0006 (1×, #509): Mark partially honored backend options as partial support; full-support labels require consistent forwarding and behavior across all relevant flows.
- L-0007 (1×, #509): Verify native capabilities against generated declarations and shipped bindings. Do not assert unobservable published-binary behavior from connector source alone.
- L-0008 (1×, #506): Resolve runtime telemetry identity identically on both backends, including POSIX locale precedence LC_ALL, LC_MESSAGES, then LANG.
- L-0009 (1×, #506): Verify omitted/default branches are reachable; upstream default injection may prevent native defaults from ever taking effect.
- L-0010 (1×, #519): Kernel requires nondefault ports embedded in host; a separate port option is forwarded only by Thrift.
- L-0011 (1×, #528): Empty metadata filters have method/backend-specific semantics. Normalize empty catalog to undefined only where wildcard behavior is intended and verified.
- L-0012 (1×, #533): Required string validation must distinguish non-string values from legitimate empty strings.
- L-0013 (1×, #536): Metadata rewrites must preserve server values for omitted/wildcard request patterns rather than stamping the request pattern onto every row.
- L-0014 (1×, #537): Forward exact namespace identifiers, including null, undefined and empty values, through metadata methods that delegate default resolution to the backend.
- L-0015 (2×, #543): Parser replacements must retain accepted upstream encodings. Exercise the real parser with representative server payloads instead of mocking the method under test.
- L-0016 (1×, #543): Failed cache refreshes retain prior values and their old fetched timestamp so the next access retries promptly.
- L-0017 (2×, #545): Separate LIKE-pattern arguments from exact identifiers per metadata method; wildcard escaping must handle backslash-run parity and already-escaped inputs.
- L-0018 (1×, #568): Share bind-time allowlist validation across backends. Validate DBSQLParameter.value, reject nested wrappers, and permit only supported primitives, Date, Int64, null/undefined or explicit VOID.
- L-0019 (1×, #585): Normalize parameters in shared toSparkParameter. Send explicit DECIMAL(p,s) for decimal aliases and derive Int64 values from the buffer as bigint.
- L-0020 (1×, #589): Reject missing required napi arguments in TypeScript with matching Thrift message/SQLSTATE before opaque native conversion errors.
- L-0021 (1×, #583): Activating formerly ignored configuration changes existing users' behavior. Document consequences and invalidate persisted caches whose inputs no longer match, including OAuth scopes.
- L-0022 (1×, #588): Match host/URL normalization across backends for slashes, embedded ports and IPv6; compare edge cases on both before changing either.

## Native packages and public compatibility
- L-0023 (4×, #505 #513 #514): New native fields need published binding support. Land router capability/version checks and all platform optionalDependency pins together; source-only KERNEL_REV bumps or accepted TypeScript fields do not ship support.
- L-0024 (2×, #514 #506 #528): Regenerate native index.d.ts from the pinned build and review its contract changes. Mocked JS tests cannot verify ABI behavior or live-server compatibility.
- L-0025 (1×, #514): Changelog entries for source-only native features must state the required follow-up published package bump and reference the kernel change.
- L-0026 (3×, #516 #522 #538): Public package-lock resolved URLs must use registry.npmjs.org after internal-mirror installs; integrity hashes remain content-addressed.
- L-0027 (1×, #523): Check sibling Python/Go driver semantics when changing cross-cutting SQLSTATE, caching or recovery behavior.
- L-0028 (2×, #543 #535): Keep native regeneration/pin changes and unrelated observable behavior changes focused and document each user-facing change separately.
- L-0029 (2×, #535 #594): Published-library overrides and lockfiles do not constrain downstream runtime dependencies. Fix declared ranges and test those ranges instead of claiming local overrides protect consumers.
- L-0030 (1×, #535): Security pins must not downgrade an already patched resolution; verify the advisory applies to the exact package.
- L-0031 (1×, #568): Put backend-shared code in neutral utilities. Preserve deep-import compatibility with deprecated re-export shims and smoke tests when moving modules.

## Errors, logging and telemetry
- L-0032 (1×, #514): Negative tests must assert the intended error shape or message so unrelated warehouse/auth failures cannot satisfy them.
- L-0033 (2×, #523 #592 #587): StatusError structurally implements Error without extending it. Preserve its instance and custom fields; inspect message/sqlState structurally rather than requiring instanceof Error.
- L-0034 (1×, #553): Synthetic Thrift error results must cover initial, directResults and polled status paths, allocate per-operation schemas, and close only real server handles.
- L-0035 (1×, #553): Synthetic metadata must copy exact server column names, including KEQ_SEQ quirks. Cite intentionally matched JDBC behavior and document swallowed cases.
- L-0036 (1×, #563): Thrift error metadata descriptors must match kernel descriptors: non-enumerable, writable and configurable. Read OperationStateError SQLSTATE from response.sqlState.
- L-0037 (2×, #565): Winston File may swallow open errors. Probe writability before adding a transport; tests attach open/error listeners immediately and await setup before closing/removing files.
- L-0038 (1×, #567): Treat presigned URLs as credentials. CloudFetch errors may expose status/code/type only; assert secrets are absent from message, stack and every enumerable payload field.
- L-0039 (1×, #567): Add context by mutating the original error and updating its stack header, preserving identity, metadata, payload and original frames.
- L-0040 (1×, #596): Error telemetry may carry class, SQLSTATE and terminal/retryable status only; exclude messages, stacks, URLs and other free-form error text.
- L-0041 (1×, #608): Decode every napi error, including status/hasMore calls. Any call that may fetch must share fetchChunk's best-effort failure cleanup, with rejecting-fake tests per call site.
- L-0042 (1×, #619): Detect user close/cancel with dedicated persistent intent flags read before cleanup, not lifecycle.isClosed. Fetch-failure cleanup must still close cancelled operations.

## Resources, networking and performance
- L-0043 (1×, #497): Portable Bash with nounset must guard empty-array expansion on older shells and distinguish unset-only defaults from unset-or-empty defaults.
- L-0044 (1×, #513): Skipped before hooks still allow after hooks; gate destructive teardown on completed setup/backup.
- L-0045 (2×, #523): Memoize backend resources per connection and close them explicitly. Close must await pending initialization so late-created resources cannot be orphaned.
- L-0046 (1×, #523): Test exact cache TTL boundaries with fake clocks, including proactive sweeping separately from lazy expiration.
- L-0047 (2×, #529): E2E resources close in finally. Bound both operations and cleanup in hang tests, using timers that do not keep the process alive.
- L-0048 (1×, #529): Configuration tests must assert observable downstream effects, not merely absence of an exception.
- L-0049 (1×, #554): Sanitize CI values entering unquoted SQL identifiers to letters, digits and underscores.
- L-0050 (1×, #569): Type parity fixes must cover both metadata type_name and Arrow fallback mappings; test both through the native-free StatementStub harness.
- L-0051 (1×, log:2026-11-07): Local HTTP download tests must avoid using a connection's HTTPS agent so they exercise the intended response failure.
- L-0052 (1×, #568): Client-side pre-RPC validation needs direct rejection assertions and a unit check that no RPC occurred, rather than an E2E hang harness.
- L-0053 (1×, log:2026-11-11): Thrift complex strings may contain bare date/timestamp literals and invalid JSON. Investigate swallowed parse failures with Arrow disabled when containers become empty.
- L-0054 (2×, #600): Clamp Node timers to 2**31-1 ms. End-to-end cancellation must stop attempts and backoff sleeps, clearing timers and listeners.
- L-0055 (1×, #598): Retry non-idempotent execution only for proven pre-connection errors; connected-socket resets, broken pipes and timeouts may follow server delivery.
- L-0056 (1×, #590): Filter framing, auth and user-agent headers from caller overrides and keep managed-header lists identical across transports.
- L-0057 (1×, #595): ProxyAgent needs TLS options at its top level. Verify proxy TLS/mTLS with a real CONNECT proxy and generated test certificate.
- L-0058 (1×, #577): Merge Arrow result rows with indexed concatChunks copies. Preserve provider batches and track owned leftover slices with offsets.
- L-0059 (1×, #611): Every auxiliary HTTP request, including OAuth, exchange and CloudFetch, must use the connection provider's getAgent to honor proxy/TLS settings.

## Bot workflow and CI
- L-0060 (1×, #499): In bot workflows with both schedule and workflow_dispatch triggers, keep numeric input knobs string-typed and parse them downstream.
- L-0061 (2×, #499): Omit unused prompt-file keys to use engine defaults; MODEL_ENDPOINT must name concrete serving-endpoints/model/invocations, not anthropic/invocations.
- L-0062 (2×, #499): Respect author-tool restrictions on workflow edits and route blocked fixes to a human. Verify actual remote branch HEAD before reporting a change as pushed.
- L-0063 (1×, #538): osv-scanner ignoreUntil must be an unquoted TOML date literal.
- L-0064 (1×, #566): Scrub JFrog and OIDC request credentials inside the model-driven step; modifying GITHUB_ENV does not unset its inherited environment.
- L-0065 (1×, #566): Build after installs that omit prepare so generated lib/version.ts exists before tests/type checks.
- L-0066 (1×, #566 #562): Serialize branch-editing bot flows with one per-PR concurrency group and restore dependency-setup lockfile rewrites before agent edits.
