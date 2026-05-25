<!--
Copyright (c) 2025 Databricks Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Node.js SQL Driver: Event-Based Telemetry

## 1. Executive Summary

The driver collects usage and reliability metrics (connection establishment, statement latency, CloudFetch chunk download stats, errors) and ships them to the Databricks telemetry ingestion endpoint. Instrumentation sites emit typed events, an in-process aggregator groups them by `sql_statement_id`, and a batched HTTP exporter ships them on a timer, on batch-size threshold, or on connection close. The pipeline is gated by a server-side feature flag, is per-host isolated, and is wrapped end-to-end so that no telemetry failure ever propagates into the user's application path.

The code landed in PR #327 under `lib/telemetry/`. This document describes the design as built.

## 2. Background & Motivation

Prior to this work the Node.js driver had no first-class telemetry: support cases relied on customer-supplied logs, and product decisions on features like CloudFetch / Arrow / LZ4 lacked usage signal. The JDBC driver had already proven out a per-host, event-aggregated, circuit-broken telemetry pipeline against the same ingestion endpoint, so the Node.js design mirrors its shape (per-host clients, feature-flag cache, terminal-vs-retryable exception classification, swallowed exceptions, ref-counted shutdown) rather than inventing a new one.

The motivating constraints were (a) zero observable cost when disabled, (b) bounded cost when enabled — especially in multi-tenant SaaS deployments that open hundreds of concurrent connections to the same workspace host — and (c) no possibility of the telemetry subsystem breaking the caller's app.

## 3. Architecture Overview

```
  driver call sites              telemetry pipeline                    network
  ───────────────────            ──────────────────────────────────    ─────────────
  DBSQLClient.openSession ──┐
  DBSQLOperation start/end ─┤    TelemetryEventEmitter
  CloudFetch chunk download ┼──> (typed emit + redact)  ──┐
  DBSQLOperation error ─────┘                             │
                                                          v
                                  MetricsAggregator
                                  (per-statement aggregation,
                                   pending batch, flush timer,
                                   idle eviction)
                                                          │
                                                          v
                                  TelemetryClient (per host, ref-counted)
                                  via TelemetryClientProvider
                                                          │
                                                          v
                                  DatabricksTelemetryExporter
                                  (auth vs unauth endpoint,
                                   CircuitBreaker, retry w/ jitter,
                                   exception swallow)  ──────────────> /telemetry-ext
                                                                       /telemetry-unauth

  FeatureFlagCache (per host, 15-min TTL, ref-counted) gates the whole pipeline.
```

Data flow: instrumentation site -> `TelemetryEventEmitter.emit*` (redacts sensitive strings) -> `MetricsAggregator.processEvent` (groups by `statementId`, buffers retryable errors, immediately flushes terminal ones) -> batch flush (size threshold, 5s timer, or explicit) -> `TelemetryClient` -> `DatabricksTelemetryExporter.export` (circuit-breaker-wrapped HTTP POST). Both `FeatureFlagCache` and `TelemetryClientProvider` are keyed by host and use reference counting so multiple `DBSQLClient` instances that connect to the same workspace share state and tear down only when the last connection closes.

## 4. Core Components

Source files all live under `lib/telemetry/`.

**TelemetryClient** (`TelemetryClient.ts`) — Per-host facade owned by the provider. Holds the host-scoped `MetricsAggregator` + `DatabricksTelemetryExporter` pair and exposes `emit*` shims plus `close()`. It is the unit of sharing across `DBSQLClient` instances pointed at the same host, which is what prevents N parallel connections from creating N export pipelines.

**TelemetryClientProvider** (`TelemetryClientProvider.ts`) — `Map<host, { client, refCount }>`. `getOrCreateClient(host)` increments the count; `releaseClient(host)` decrements and, on zero, awaits `client.close()` and evicts the entry. The provider is instance-scoped on `DBSQLClient` rather than process-global so that test isolation and multi-tenant embedding work cleanly.

**TelemetryEventEmitter** (`TelemetryEventEmitter.ts`) — Thin wrapper around Node's `EventEmitter`. Each public `emit*` method (`emitConnectionOpen`, `emitStatementStart`, `emitStatementComplete`, `emitCloudFetchChunk`, `emitError`) builds the typed event payload, runs `redactSensitive` over any free-form strings (notably `errorMessage` and `errorStack`), and emits it on a named channel. Every method is wrapped in try/catch; failures log at `LogLevel.debug` and are swallowed. If `telemetryEnabled` is false the methods are no-ops.

**MetricsAggregator** (`MetricsAggregator.ts`) — The core stateful component. Keeps a `Map<statementId, StatementTelemetryDetails>` for in-flight statements and a flat `pendingMetrics[]` for ready-to-export records. `processEvent` dispatches on event type: connection events flush as a single metric; statement-start opens an aggregation slot; chunk events update counters; statement-complete fills in latency/result-format and calls `completeStatement(id)` which materializes the aggregated metric onto the batch. Retryable errors are buffered on the statement and emitted at completion; terminal errors emit immediately (see Section 6). The 5s periodic flush timer is `unref()`'d so it never holds the event loop open. An idle-eviction sweep on each tick reaps statements whose aggregation slot has gone stale (typically because `complete` was never emitted).

**FeatureFlagCache** (`FeatureFlagCache.ts`) — Per-host cache of the `enableTelemetryForNodeJs` flag with a 15-minute TTL and reference counting matching the client provider. A single fetch per host per TTL window protects the flag endpoint from being hammered by high-connection-rate clients. `isTelemetryEnabled(host)` returns the cached boolean (default false on fetch failure).

**DatabricksTelemetryExporter** (`DatabricksTelemetryExporter.ts`) — Owns the HTTP shape. Picks `/telemetry-ext` (authenticated) or `/telemetry-unauth` based on config, builds the `{ uploadTime, items: [], protoLogs: string[] }` payload (each entry is a JSON-stringified `OssSqlDriverTelemetryLog`), wraps the POST in the host's `CircuitBreaker`, and applies retry-with-jittered-exponential-backoff to retryable failures only. Exception classification uses `ExceptionClassifier`. The class is contractually no-throw: `export()` catches everything and logs at debug.

**CircuitBreaker / CircuitBreakerRegistry** (`CircuitBreaker.ts`) — Standard three-state breaker (CLOSED -> OPEN after N consecutive failures, OPEN -> HALF_OPEN after timeout, HALF_OPEN -> CLOSED after M consecutive successes). Defaults: 5 failures, 60s timeout, 2 successes. Registry hands out one breaker per host so a flapping host can't poison telemetry to healthy ones.

**ExceptionClassifier** (`ExceptionClassifier.ts`) — Two static predicates, `isTerminal(err)` and `isRetryable(err)`. Terminal: `AuthenticationError`, HTTP 400/401/403/404. Retryable: `RetryError`, network timeouts (by name or message), HTTP 429/500/502/503/504. Unknown shapes return false from both — fail-safe.

**telemetryTypeMappers** (`telemetryTypeMappers.ts`) — Pure functions that translate internal `TelemetryMetric` records into the wire-format `OssSqlDriverTelemetryLog` proto shape. See the file for the exact field mapping; the design choice worth noting is that we deliberately do not populate JDBC-specific connection-param fields (proxy / SSL / Azure-GCP-specific settings) — only the subset that has a Node.js analogue is emitted.

**telemetryUtils** (`telemetryUtils.ts`) — `redactSensitive`, `sanitizeProcessName`, `buildTelemetryUrl` (which enforces `BLOCKED_HOST_PATTERNS` so a tampered host config can't redirect bearer-bearing requests to an attacker), and the `SECRET_PATTERNS` regex set used for redaction.

## 5. Export Lifecycle

**Endpoint selection.** `telemetryAuthenticatedExport` (default true) picks `/telemetry-ext` with the connection's auth headers; false picks `/telemetry-unauth` which still goes over HTTPS but carries no credentials. Unauth mode exists for the bootstrap window before a session has authenticated and for environments where the workspace explicitly disallows authenticated telemetry from the driver.

**Flush triggers.** A metric is added to `pendingMetrics` when (a) a statement is completed via `completeStatement`, (b) a connection-open event is processed, or (c) a terminal error fires. An actual HTTP export happens on any of:

1. **Batch size threshold** — `pendingMetrics.length >= telemetryBatchSize` (default 100). Fire-and-forget; subsequent `addPendingMetric` calls are suppressed via `closing` to prevent overlapping flushes during shutdown.
2. **Periodic timer** — every `telemetryFlushIntervalMs` (default 5s). Timer is `unref()`'d.
3. **Connection close** — `DBSQLClient.close()` awaits `MetricsAggregator.close()` which completes any in-flight statements, then runs a final drain.
4. **Terminal error** — flushed immediately as a single-record batch.

**Retry and circuit breaker.** Inside `DatabricksTelemetryExporter.export`, the POST is wrapped by `circuitBreaker.execute(...)`. If the breaker is OPEN, the call rejects with `Circuit breaker OPEN`; the exporter catches that and drops the batch (no retry, no log noise above debug). Otherwise the operation runs; on a retryable failure the exporter retries up to `telemetryMaxRetries` (default 3) with jittered exponential backoff (100ms–1000ms). On a terminal failure it gives up immediately. Every failure path counts toward the breaker, so a sustained-failing endpoint will open the breaker after 5 consecutive failures and stop wasting wall-clock time on retries until the 60s cooldown elapses.

## 6. Privacy & Redaction

No SQL text, no result rows, no table/column identifiers, and no user identities are ever collected — only operation latency, counts/bytes, result-format enum, error name + (redacted) stack, and IDs (workspace, session, statement). `redactSensitive` is applied at emit time on any free-form string (`errorMessage`, `errorStack`, and the user-agent's `userAgentEntry`) and again as a defence-in-depth pass at export time. It strips `Authorization: Bearer`/`Basic` headers, Databricks PAT prefixes (`dapi…`, `dose…`, etc.), JWTs, OAuth `client_secret` values, JSON-encoded credentials, URL userinfo, and home-directory path prefixes. `sanitizeProcessName` additionally redacts the home-dir tail from any process-name string before it appears in `system_configuration.process_name`. `buildTelemetryUrl`'s `BLOCKED_HOST_PATTERNS` prevents a tampered or malicious `host` config from redirecting authenticated telemetry POSTs to a non-Databricks host.

## 7. Error Handling

The hard invariant is: **telemetry must never break the user's app, and must never appear noisy in customer logs.** Every entry point into the telemetry subsystem (`emit*`, `processEvent`, `flush`, `export`, `close`, periodic timer callbacks) is wrapped in try/catch. Every catch logs at `LogLevel.debug` only — never `info`/`warn`/`error` — and swallows. No `console.*` calls anywhere in the telemetry tree; all logging routes through `IDBSQLLogger`.

Two structural protections back the invariant. First, the per-host `CircuitBreaker` cuts off HTTP traffic to an unhealthy endpoint after a small number of consecutive failures, so a sustained outage degenerates from "every request errors and retries" to "every request fast-fails inside the breaker" — bounded CPU and zero network. Second, the `MetricsAggregator.close()` final flush is wall-clock-capped by `telemetryCloseTimeoutMs` (default 2000ms) — if the export pipeline is hung on a flapping endpoint at process-shutdown time, the in-flight POST is abandoned and the user's `process.exit(0)` proceeds. Data loss is preferable to a hung exit.

## 8. Graceful Shutdown

`DBSQLClient.close()` awaits `MetricsAggregator.close()` -> `telemetryClientProvider.releaseClient(host)` -> `featureFlagCache.releaseContext(host)`. `MetricsAggregator.close()` (a) detaches its `beforeExit` handler so long-lived hosts that open and close many clients don't leak listeners on `process`, (b) clears the periodic flush interval, (c) walks `statementMetrics` and calls `completeStatement` on each remaining in-flight statement (so close-time aggregations make it into the batch), and (d) awaits a `Promise.race([drain, timeout])` where `drain` waits on any in-flight flush and then issues a fresh one. The bounded race is what makes the close safe to `await` in a SIGINT/SIGTERM handler.

Because the periodic timer is `unref()`'d, a process that calls `process.exit()` (or whose event loop empties) without calling `client.close()` will drop pending telemetry. This is intentional — the alternative is keeping the process alive on the user's behalf, which is worse than dropping a few metrics. Callers that want at-least-once delivery should `await client.close()` in a `finally` block or signal handler.

## 9. Testing Strategy

Each component under `lib/telemetry/` has a unit test in `tests/unit/telemetry/` exercising state machines (circuit breaker transitions, aggregator buffering, ref-count cycles), exception swallowing (every throwing path verified to log at debug and return cleanly), and shape correctness (proto-mapper output, redaction). Shared stubs live in `tests/unit/.stubs/` — `ClientContextStub`, `CircuitBreakerStub`, `TelemetryExporterStub` — so dependent components can be tested with deterministic behavior from their collaborators. End-to-end coverage lives in `tests/e2e/telemetry/telemetry-integration.test.ts` and asserts the full path: feature-flag respected, client sharing across multiple `DBSQLClient` instances, ref-counted cleanup, no exceptions escaping into the application, and configuration overrides applied via `ConnectionOptions`.

## 10. Configuration

Telemetry config lives on `ClientConfig` (`lib/contracts/IClientContext.ts`) and can be overridden per-connection through `ConnectionOptions.telemetryEnabled`. Defaults (see `DEFAULT_TELEMETRY_CONFIG` in `lib/telemetry/types.ts`): enabled true (still gated by the server feature flag), batch size 100, flush interval 5000ms, max retries 3, authenticated export true, close timeout 2000ms, circuit-breaker threshold 5, circuit-breaker timeout 60000ms.

## 11. Proto Field Coverage

The driver populates the subset of `OssSqlDriverTelemetryLog` that has a Node.js analogue — session/statement IDs, `system_configuration` (driver name/version, runtime, OS, locale, charset, process name, auth type), `driver_connection_params` (http_path, socket_timeout, enable_arrow, enable_direct_results, enable_metric_view_metadata), `sql_operation` (statement_type, is_compressed, execution_result, chunk_details.total_chunks_present/iterated), `operation_latency_ms`, and `error_info`. JDBC-specific fields (proxy/SSL config, Azure/GCP-specific settings, per-chunk timing, operation-detail polling counters, result-latency breakdown) are deliberately not populated. See `lib/telemetry/telemetryTypeMappers.ts` for the exact mapping.
