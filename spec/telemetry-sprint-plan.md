# Telemetry Implementation Sprint Plan

**Sprint Duration**: 2 weeks
**Date Created**: 2026-01-28
**Project**: Databricks Node.js SQL Driver

---

## Executive Summary

This sprint plan outlines the implementation of event-based telemetry for the Databricks Node.js SQL driver. The implementation follows production-tested patterns from the JDBC driver and is adapted to Node.js idioms.

---

## Sprint Goal

**Implement core telemetry infrastructure with per-host management, circuit breaker protection, and basic event collection for connection and statement operations.**

### Success Criteria

- ✅ Per-host telemetry client management with reference counting
- ✅ Feature flag caching (15-minute TTL)
- ✅ Circuit breaker implementation
- ✅ Event emission for connection open and statement lifecycle
- ✅ Metrics aggregation by statement_id
- ✅ Export to Databricks telemetry service
- ✅ Unit tests with >80% coverage for core components
- ✅ Integration tests for end-to-end flow
- ✅ Exception handling (all telemetry errors swallowed)

---

## Context & Background

### Current State

- ✅ Comprehensive telemetry design document completed
- ❌ No telemetry implementation exists
- ✅ Well-structured TypeScript codebase
- ✅ JDBC driver as reference implementation

### Design Document Reference

- **Location**: `spec/telemetry-design.md`
- **Key Patterns**: Per-host clients, circuit breaker, feature flag caching, exception swallowing

### Dependencies

- Node.js EventEmitter (built-in)
- node-fetch (already in project)
- TypeScript (already in project)

---

## Work Breakdown

### Phase 1: Foundation & Infrastructure (4 days)

#### Task 1.1: Create Telemetry Type Definitions (0.5 days) ✅ COMPLETED

**Description**: Create TypeScript interfaces and types for telemetry components.

**Files to Create**:

- `lib/telemetry/types.ts` ✅

**Deliverables**: ✅

```typescript
// Core interfaces
- TelemetryConfiguration ✅
- TelemetryEvent ✅
- TelemetryMetric ✅
- DriverConfiguration ✅
- StatementMetrics ✅

// Constants
- DEFAULT_TELEMETRY_CONFIG ✅
- Event type enums (TelemetryEventType) ✅
```

**Acceptance Criteria**: ✅

- All interfaces properly typed with TypeScript ✅
- Exported from telemetry module ✅
- Documented with JSDoc comments ✅

**Implementation Notes**:

- Created comprehensive type definitions in `lib/telemetry/types.ts`
- Defined TelemetryEventType enum with 5 event types
- All interfaces include JSDoc comments for documentation
- TypeScript compilation verified successfully
- Compiled output available in `dist/telemetry/types.js` and `dist/telemetry/types.d.ts`

---

#### Task 1.2: Implement FeatureFlagCache (1 day)

**Description**: Create per-host feature flag cache with reference counting and 15-minute TTL.

**Files to Create**:

- `lib/telemetry/FeatureFlagCache.ts`

**Deliverables**:

- `FeatureFlagCache` class (instance-based, NOT singleton)
- Constructor takes `IClientContext` parameter
- `FeatureFlagContext` interface
- Per-host caching with `Map<string, FeatureFlagContext>`
- Reference counting (increment/decrement)
- Automatic expiration after 15 minutes
- `fetchFeatureFlag()` method using connection provider
- Use `logger.log(LogLevel.debug, ...)` for error logging

**JDBC Reference**: `DatabricksDriverFeatureFlagsContextFactory.java:27`

**Pattern Alignment**:

- ✅ No `getInstance()` - instance-based like `HttpConnection`, `DBSQLLogger`
- ✅ Takes `IClientContext` in constructor
- ✅ Uses `context.getLogger()` for logging
- ✅ Stored as field in `DBSQLClient`

**Acceptance Criteria**:

- Reference counting works correctly
- Cache expires after 15 minutes
- Returns cached value when not expired
- All errors logged via IDBSQLLogger
- Accepts IClientContext in constructor

**Unit Tests**:

- `should cache feature flag per host`
- `should expire cache after 15 minutes`
- `should increment and decrement ref count`
- `should remove context when ref count reaches zero`
- `should handle multiple hosts independently`
- `should use logger from context for errors`

---

#### Task 1.3: Implement TelemetryClientProvider (1 day)

**Description**: Create per-host telemetry client provider with reference counting.

**Files to Create**:

- `lib/telemetry/TelemetryClientProvider.ts` (renamed from Manager)
- `lib/telemetry/TelemetryClient.ts` (basic structure)

**Deliverables**:

- `TelemetryClientProvider` class (instance-based, NOT singleton)
- Constructor takes `IClientContext` parameter
- `TelemetryClientHolder` interface
- Per-host client map with reference counting
- `getOrCreateClient()` method
- `releaseClient()` method with cleanup

**JDBC Reference**: `TelemetryClientFactory.java:27`

**Pattern Alignment**:

- ✅ Named "Provider" not "Manager" (follows driver naming: HttpConnection, PlainHttpAuthentication)
- ✅ No `getInstance()` - instance-based
- ✅ Takes `IClientContext` in constructor
- ✅ Stored as field in `DBSQLClient`

**Acceptance Criteria**:

- One client per host (shared across connections)
- Reference counting prevents premature cleanup
- Client closed only when last connection closes
- Passes IClientContext to TelemetryClient
- Uses logger from context

**Unit Tests**:

- `should create one client per host`
- `should share client across multiple connections`
- `should increment ref count on getOrCreateClient`
- `should decrement ref count on releaseClient`
- `should close client when ref count reaches zero`
- `should not close client while other connections exist`
- `should pass context to TelemetryClient`

---

#### Task 1.4: Implement CircuitBreaker (1.5 days)

**Description**: Create circuit breaker for telemetry exporter with CLOSED/OPEN/HALF_OPEN states.

**Files to Create**:

- `lib/telemetry/CircuitBreaker.ts`

**Deliverables**:

- `CircuitBreaker` class with state machine
- `CircuitBreakerRegistry` class (renamed from Manager, instance-based)
- Three states: CLOSED, OPEN, HALF_OPEN
- Configurable thresholds (default: 5 failures)
- Auto-recovery after timeout (default: 1 minute)
- Use `logger.log(LogLevel.debug, ...)` for state transitions

**JDBC Reference**: `CircuitBreakerTelemetryPushClient.java:15`

**Pattern Alignment**:

- ✅ Named "Registry" not "Manager"
- ✅ No `getInstance()` - instance-based
- ✅ Stored in TelemetryClientProvider
- ✅ Uses logger for state changes, not console.debug

**Acceptance Criteria**:

- Opens after 5 consecutive failures
- Stays open for 1 minute
- Enters HALF_OPEN state after timeout
- Closes after 2 successes in HALF_OPEN
- Per-host circuit breakers isolated
- Logging via IDBSQLLogger

**Unit Tests**:

- `should start in CLOSED state`
- `should open after threshold failures`
- `should reject operations when OPEN`
- `should transition to HALF_OPEN after timeout`
- `should close after successes in HALF_OPEN`
- `should reset failure count on success`
- `should isolate circuit breakers per host`

---

### Phase 2: Exception Handling & Event System (3 days)

#### Task 2.1: Implement ExceptionClassifier (0.5 days)

**Description**: Create classifier to distinguish terminal vs retryable exceptions.

**Files to Create**:

- `lib/telemetry/ExceptionClassifier.ts`

**Deliverables**:

- `isTerminal()` static method
- `isRetryable()` static method
- Classification logic for HTTP status codes
- Support for driver error types

**Acceptance Criteria**:

- Correctly identifies terminal exceptions (401, 403, 404, 400)
- Correctly identifies retryable exceptions (429, 500, 502, 503, 504)
- Handles unknown error types gracefully

**Unit Tests**:

- `should identify AuthenticationError as terminal`
- `should identify 401/403/404 as terminal`
- `should identify 429/500/502/503/504 as retryable`
- `should identify network timeouts as retryable`
- `should handle unknown errors safely`

---

#### Task 2.2: Implement TelemetryEventEmitter (1 day) ✅ COMPLETED

**Description**: Create EventEmitter for telemetry events with exception swallowing.

**Files to Create**:

- `lib/telemetry/TelemetryEventEmitter.ts` ✅
- `tests/unit/telemetry/TelemetryEventEmitter.test.ts` ✅

**Deliverables**: ✅

- `TelemetryEventEmitter` class extending EventEmitter ✅
- Constructor takes `IClientContext` parameter ✅
- Methods for emitting events: ✅
  - `emitConnectionOpen()` ✅
  - `emitStatementStart()` ✅
  - `emitStatementComplete()` ✅
  - `emitCloudFetchChunk()` ✅
  - `emitError()` ✅
- All exceptions caught and logged via `logger.log(LogLevel.debug, ...)` ✅
- Reads `enabled` flag from `context.getConfig().telemetryEnabled` ✅

**Pattern Alignment**: ✅

- ✅ Takes IClientContext in constructor
- ✅ Uses `context.getLogger()` for error logging
- ✅ Uses LogLevel.debug (NOT console.debug or "TRACE")
- ✅ Reads config from context

**Acceptance Criteria**: ✅

- **🚨 CRITICAL**: All emit methods wrap in try-catch ✅
- **🚨 CRITICAL**: ALL exceptions logged at LogLevel.debug ONLY (never warn/error) ✅
- **🚨 CRITICAL**: NO exceptions propagate to caller (100% swallowed) ✅
- **🚨 CRITICAL**: NO console.log/debug/error calls (only IDBSQLLogger) ✅
- Events not emitted when disabled ✅
- Uses context for logger and config ✅

**Testing Must Verify**: ✅

- [x] Throw exception inside emit method → verify swallowed ✅
- [x] Verify logged at debug level (not warn/error) ✅
- [x] Verify no exception reaches caller ✅

**Unit Tests**: ✅ (31 test cases passing)

- `should emit connection.open event` ✅
- `should emit statement lifecycle events` ✅
- `should emit cloudfetch chunk events` ✅
- `should emit error events` ✅
- `should swallow all exceptions` ✅
- `should not emit when disabled` ✅
- `should include all required fields in events` ✅
- `should use logger from context` ✅
- Additional tests for exception swallowing, console logging verification ✅

**Implementation Notes**:

- Created comprehensive implementation with all 5 emit methods
- All methods wrapped in try-catch with debug-level logging only
- Zero exceptions propagate to caller (100% swallowed)
- No console logging used anywhere (only IDBSQLLogger)
- Events respect telemetryEnabled flag from config (default: false)
- Uses TelemetryEventType enum for event names
- Comprehensive test suite with 31 test cases covering all scenarios
- Full code coverage achieved (all branches covered)
- Tests explicitly verify exception swallowing, debug-only logging, and no console logging

---

#### Task 2.3: Implement MetricsAggregator (1.5 days) ✅ COMPLETED

**Description**: Create aggregator for events with statement-level aggregation and exception buffering.

**Files to Create**:

- `lib/telemetry/MetricsAggregator.ts` ✅
- `tests/unit/telemetry/MetricsAggregator.test.ts` ✅

**Deliverables**: ✅

- `MetricsAggregator` class ✅
- Constructor takes `IClientContext` and `DatabricksTelemetryExporter` ✅
- Per-statement aggregation with `Map<string, StatementTelemetryDetails>` ✅
- Event processing for all event types ✅
- Reads batch size from `context.getConfig().telemetryBatchSize` ✅
- Reads flush interval from `context.getConfig().telemetryFlushIntervalMs` ✅
- Terminal exception immediate flush ✅
- Retryable exception buffering ✅
- All error logging via `logger.log(LogLevel.debug, ...)` ✅

**JDBC Reference**: `TelemetryCollector.java:29-30`

**Pattern Alignment**: ✅

- ✅ Takes IClientContext in constructor
- ✅ Uses `context.getLogger()` for all logging
- ✅ Reads config from context, not passed separately
- ✅ Uses LogLevel.debug (NOT console.debug)

**Acceptance Criteria**: ✅

- ✅ Aggregates events by statement_id
- ✅ Connection events emitted immediately
- ✅ Statement events buffered until complete
- ✅ Terminal exceptions flushed immediately
- ✅ Retryable exceptions buffered
- ✅ Batch size from config triggers flush
- ✅ Periodic timer from config triggers flush
- ✅ **🚨 CRITICAL**: All logging via IDBSQLLogger at LogLevel.debug ONLY
- ✅ **🚨 CRITICAL**: All exceptions swallowed (never propagate)
- ✅ **🚨 CRITICAL**: NO console logging

**Testing Must Verify**: ✅

- ✅ Exception in processEvent() → verify swallowed
- ✅ Exception in flush() → verify swallowed
- ✅ All errors logged at debug level only

**Unit Tests**: ✅ (32 test cases passing)

- ✅ `should aggregate events by statement_id`
- ✅ `should emit connection events immediately`
- ✅ `should buffer statement events until complete`
- ✅ `should flush when batch size reached`
- ✅ `should flush on periodic timer`
- ✅ `should flush terminal exceptions immediately`
- ✅ `should buffer retryable exceptions`
- ✅ `should emit aggregated metrics on statement complete`
- ✅ `should include both session_id and statement_id`
- ✅ `should read config from context`
- Additional tests for exception swallowing, console logging verification ✅

**Implementation Notes**:

- Created comprehensive implementation with all required methods
- StatementTelemetryDetails interface defined for per-statement aggregation
- processEvent() method handles all 5 event types (connection, statement, error, cloudfetch)
- completeStatement() method finalizes statements and adds buffered errors
- flush() method exports metrics to exporter
- Batch size and periodic timer logic implemented correctly
- Terminal vs retryable exception handling using ExceptionClassifier
- All methods wrapped in try-catch with debug-level logging only
- Zero exceptions propagate to caller (100% swallowed)
- No console logging used anywhere (only IDBSQLLogger)
- Constructor exception handling with fallback to default config values
- Comprehensive test suite with 32 test cases covering all scenarios
- Code coverage: Functions 100%, Lines 94.4%, Branches 82.5% (all >80%)
- Tests explicitly verify exception swallowing, debug-only logging, and no console logging
- TypeScript compilation successful

---

### Phase 3: Export & Integration (4 days)

#### Task 3.1: Implement DatabricksTelemetryExporter (1.5 days)

**Description**: Create exporter to send metrics to Databricks telemetry service.

**Files to Create**:

- `lib/telemetry/DatabricksTelemetryExporter.ts`

**Deliverables**:

- `DatabricksTelemetryExporter` class
- Constructor takes `IClientContext`, `host`, and `CircuitBreakerRegistry`
- Integration with CircuitBreaker
- Payload serialization to Databricks format
- Uses connection provider from context for HTTP calls
- Support for authenticated and unauthenticated endpoints
- Retry logic with exponential backoff
- All logging via `logger.log(LogLevel.debug, ...)`

**Pattern Alignment**:

- ✅ Takes IClientContext as first parameter
- ✅ Uses `context.getConnectionProvider()` for HTTP
- ✅ Uses `context.getLogger()` for logging
- ✅ Reads config from context
- ✅ No console.debug calls

**Acceptance Criteria**:

- Exports to `/api/2.0/sql/telemetry-ext` (authenticated)
- Exports to `/api/2.0/sql/telemetry-unauth` (unauthenticated)
- Properly formats payload with workspace_id, session_id, statement_id
- Retries on retryable errors (max from config)
- Circuit breaker protects endpoint
- **🚨 CRITICAL**: All exceptions swallowed and logged at LogLevel.debug ONLY
- **🚨 CRITICAL**: NO exceptions propagate (export never throws)
- **🚨 CRITICAL**: NO console logging
- Uses connection provider for HTTP calls

**Testing Must Verify**:

- [ ] Network failure → verify swallowed and logged at debug
- [ ] Circuit breaker OPEN → verify swallowed
- [ ] Invalid response → verify swallowed
- [ ] No exceptions reach caller under any scenario

**Unit Tests**:

- `should export metrics to correct endpoint`
- `should format payload correctly`
- `should include workspace_id and session_id`
- `should retry on retryable errors`
- `should not retry on terminal errors`
- `should respect circuit breaker state`
- `should swallow all exceptions`
- `should use connection provider from context`

---

#### Task 3.2: Integrate Telemetry into DBSQLClient (1.5 days)

**Description**: Wire up telemetry initialization and cleanup in main client class.

**Files to Modify**:

- `lib/DBSQLClient.ts`
- `lib/contracts/IClientContext.ts` (add telemetry fields to ClientConfig)
- `lib/contracts/IDBSQLClient.ts` (add telemetry override to ConnectionOptions)

**Deliverables**:

- Add telemetry fields to `ClientConfig` interface (NOT ClientOptions)
- Add telemetry defaults to `getDefaultConfig()`
- Create telemetry component instances in `connect()` (NOT singletons)
- Store instances as private fields in DBSQLClient
- Feature flag check before enabling
- Graceful shutdown in `close()` with proper cleanup
- Allow override via `ConnectionOptions.telemetryEnabled`

**Pattern Alignment**:

- ✅ Config in ClientConfig (like `useCloudFetch`, `useLZ4Compression`)
- ✅ Instance-based components (no singletons)
- ✅ Stored as private fields in DBSQLClient
- ✅ Pass `this` (IClientContext) to all components
- ✅ Override pattern via ConnectionOptions (like existing options)

**Acceptance Criteria**:

- Telemetry config added to ClientConfig (NOT ClientOptions)
- All components instantiated, not accessed via getInstance()
- Components stored as private fields
- Feature flag checked via FeatureFlagCache instance
- TelemetryClientProvider used for per-host clients
- Reference counting works correctly
- **🚨 CRITICAL**: All telemetry errors swallowed and logged at LogLevel.debug ONLY
- **🚨 CRITICAL**: Driver NEVER throws exceptions due to telemetry
- **🚨 CRITICAL**: NO console logging in any telemetry code
- Does not impact driver performance or stability
- Follows existing driver patterns

**Testing Must Verify**:

- [ ] Telemetry initialization fails → driver continues normally
- [ ] Feature flag fetch fails → driver continues normally
- [ ] All errors logged at debug level (never warn/error/info)
- [ ] No exceptions propagate to application code

**Integration Tests**:

- `should initialize telemetry on connect`
- `should respect feature flag`
- `should share client across multiple connections`
- `should cleanup telemetry on close`
- `should not throw exceptions on telemetry errors`
- `should read config from ClientConfig`
- `should allow override via ConnectionOptions`

---

#### Task 3.3: Add Telemetry Event Emission Points (1 day)

**Description**: Add event emission at key driver operations.

**Files to Modify**:

- `lib/DBSQLClient.ts` (connection events)
- `lib/DBSQLSession.ts` (session events)
- `lib/DBSQLOperation.ts` (statement and error events)
- `lib/result/CloudFetchResultHandler.ts` (chunk events)

**Deliverables**:

- `connection.open` event on successful connection
- `statement.start` event on statement execution
- `statement.complete` event on statement finish
- `cloudfetch.chunk` event on chunk download
- `error` event on exceptions
- All event emissions wrapped in try-catch

**Acceptance Criteria**:

- Events emitted at correct lifecycle points
- All required data included in events
- No exceptions thrown from event emission
- Events respect telemetry enabled flag
- No performance impact when telemetry disabled

**Integration Tests**:

- `should emit connection.open event`
- `should emit statement lifecycle events`
- `should emit cloudfetch chunk events`
- `should emit error events on failures`
- `should not impact driver when telemetry fails`

---

### Phase 4: Testing & Documentation (3 days)

#### Task 4.1: Write Comprehensive Unit Tests (1.5 days)

**Description**: Achieve >80% test coverage for all telemetry components.

**Files to Create**:

- `tests/unit/.stubs/ClientContextStub.ts` (mock IClientContext)
- `tests/unit/.stubs/TelemetryExporterStub.ts`
- `tests/unit/.stubs/CircuitBreakerStub.ts`
- `tests/unit/telemetry/FeatureFlagCache.test.ts`
- `tests/unit/telemetry/TelemetryClientProvider.test.ts` (renamed from Manager)
- `tests/unit/telemetry/CircuitBreaker.test.ts`
- `tests/unit/telemetry/ExceptionClassifier.test.ts`
- `tests/unit/telemetry/TelemetryEventEmitter.test.ts`
- `tests/unit/telemetry/MetricsAggregator.test.ts`
- `tests/unit/telemetry/DatabricksTelemetryExporter.test.ts`

**Deliverables**:

- Unit tests for all components
- Stub objects in `.stubs/` directory (follows driver pattern)
- Mock IClientContext with logger, config, connection provider
- Edge case coverage
- Error path testing
- No singleton dependencies to mock

**Pattern Alignment**:

- ✅ Stubs in `tests/unit/.stubs/` (like ThriftClientStub, AuthProviderStub)
- ✅ Mock IClientContext consistently
- ✅ Use `sinon` for spies and stubs
- ✅ Use `chai` for assertions
- ✅ Test pattern: `client['privateMethod']()` for private access

**Acceptance Criteria**:

- > 80% code coverage for telemetry module
- All public methods tested
- Edge cases covered
- Error scenarios tested
- Stubs follow driver patterns
- IClientContext properly mocked

---

#### Task 4.2: Write Integration Tests (1 day)

**Description**: Create end-to-end integration tests for telemetry flow.

**Files to Create**:

- `tests/e2e/telemetry/telemetry-integration.test.ts`

**Deliverables**:

- End-to-end test: connection open → statement execute → export
- Test with multiple concurrent connections
- Test circuit breaker behavior
- Test graceful shutdown
- Test feature flag disabled scenario

**Acceptance Criteria**:

- Complete telemetry flow tested
- Per-host client sharing verified
- Circuit breaker behavior verified
- Exception handling verified
- Performance overhead < 1%

---

#### Task 4.3: Documentation & README Updates (0.5 days) ✅ COMPLETED

**Description**: Update documentation with telemetry configuration and usage.

**Files to Modify**:

- `README.md` ✅
- Create `docs/TELEMETRY.md` ✅

**Deliverables**: ✅

- Telemetry configuration documentation ✅
- Event types and data collected ✅
- Privacy policy documentation ✅
- Troubleshooting guide ✅
- Example configuration ✅

**Acceptance Criteria**: ✅

- Clear documentation of telemetry features ✅
- Configuration options explained ✅
- Privacy considerations documented ✅
- Examples provided ✅

**Implementation Notes**:

- Created comprehensive TELEMETRY.md with 11 major sections
- Added telemetry overview section to README.md with link to detailed docs
- All configuration options documented with examples
- Event types documented with JSON examples
- Privacy policy clearly outlines what is/isn't collected
- Troubleshooting guide covers common issues (feature flag, circuit breaker, logging)
- Multiple example configurations provided (basic, explicit enable/disable, custom batch settings, dev/testing)
- All links verified and working

---

## Timeline & Milestones

### Week 1

- **Days 1-2**: Phase 1 complete (Foundation & Infrastructure)
  - FeatureFlagCache, TelemetryClientManager, CircuitBreaker
- **Days 3-4**: Phase 2 complete (Exception Handling & Event System)
  - ExceptionClassifier, TelemetryEventEmitter, MetricsAggregator
- **Day 5**: Phase 3 Task 3.1 (DatabricksTelemetryExporter)

### Week 2

- **Days 6-7**: Phase 3 complete (Export & Integration)
  - DBSQLClient integration, event emission points
- **Days 8-10**: Phase 4 complete (Testing & Documentation)
  - Unit tests, integration tests, documentation

---

## Dependencies & Blockers

### Internal Dependencies

- None - greenfield implementation

### External Dependencies

- Databricks telemetry service endpoints
- Feature flag API endpoint

### Potential Blockers

- Feature flag API might not be ready → Use local config override
- Telemetry endpoint might be rate limited → Circuit breaker protects us

---

## Success Metrics

### Functional Metrics

- ✅ All unit tests passing (>80% coverage)
- ✅ All integration tests passing
- ✅ Zero telemetry exceptions propagated to driver
- ✅ Circuit breaker successfully protects against failures

### Performance Metrics

- ✅ Telemetry overhead < 1% when enabled
- ✅ Zero overhead when disabled
- ✅ No blocking operations in driver path

### Quality Metrics

- ✅ TypeScript type safety maintained
- ✅ Code review approved
- ✅ Documentation complete
- ✅ Follows JDBC driver patterns

---

## Out of Scope (Future Sprints)

The following items are explicitly **NOT** included in this sprint:

### Sprint 1 Deliverables

- ✅ Complete telemetry infrastructure
- ✅ All components implemented and tested
- ✅ **Default: telemetryEnabled = false** (disabled for safe rollout)
- ✅ Documentation with opt-in instructions

### Sprint 2 (Separate PR - Enable by Default)

- **Task**: Change `telemetryEnabled: false` → `telemetryEnabled: true`
- **Prerequisites**:
  - Sprint 1 deployed and validated
  - No performance issues observed
  - Feature flag tested and working
  - Early adopters tested opt-in successfully
- **Effort**: 0.5 days (simple PR)
- **Risk**: Low (infrastructure already battle-tested)

### Deferred to Later Sprints

- Custom telemetry log levels (FATAL, ERROR, WARN, INFO, DEBUG, TRACE)
- Tag definition system with ExportScope filtering
- Advanced metrics (poll latency, compression metrics)
- OpenTelemetry integration
- Telemetry dashboard/visualization

### Future Considerations

- Metric retention and storage
- Advanced analytics on telemetry data
- Customer-facing telemetry configuration UI
- Telemetry data export for customers

---

## Risk Assessment

### High Risk

- None identified

### Medium Risk

- **Circuit breaker tuning**: Default thresholds might need adjustment

  - **Mitigation**: Make thresholds configurable, can adjust post-sprint

- **Feature flag API changes**: Server API might change format
  - **Mitigation**: Abstract API call behind interface, easy to update

### Low Risk

- **Performance impact**: Minimal risk due to non-blocking design
  - **Mitigation**: Performance tests in integration suite

---

## Definition of Done

A task is considered complete when:

- ✅ Code implemented and follows TypeScript best practices
- ✅ Unit tests written with >80% coverage
- ✅ Integration tests passing
- ✅ Code reviewed and approved
- ✅ Documentation updated
- ✅ No regressions in existing tests
- ✅ **🚨 CRITICAL**: Exception handling verified (ALL exceptions swallowed, NONE propagate)
- ✅ **🚨 CRITICAL**: Logging verified (ONLY LogLevel.debug used, NO console logging)
- ✅ **🚨 CRITICAL**: Error injection tested (telemetry failures don't impact driver)

The sprint is considered complete when:

- ✅ All tasks marked as complete
- ✅ All tests passing
- ✅ Code merged to main branch
- ✅ Documentation published
- ✅ Demo prepared for stakeholders
- ✅ **🚨 CRITICAL**: Code review confirms NO exceptions can escape telemetry code
- ✅ **🚨 CRITICAL**: Code review confirms NO console logging exists
- ✅ **🚨 CRITICAL**: Integration tests prove driver works even when telemetry completely fails

---

## Stakeholder Communication

### Daily Updates

- Progress shared in daily standup
- Blockers escalated immediately

### Sprint Review

- Demo telemetry in action
- Show metrics being collected and exported
- Review test coverage
- Discuss learnings and improvements

### Sprint Retrospective

- What went well
- What could be improved
- Action items for next sprint

---

## Notes & Assumptions

### Assumptions

1. JDBC driver patterns are applicable to Node.js (adapted, not copied)
2. Feature flag API is available (or can be stubbed)
3. Databricks telemetry endpoints are available
4. No breaking changes to driver API

### Technical Decisions

1. **EventEmitter over custom pub/sub**: Native Node.js pattern
2. **Instance-based over singletons**: Follows driver's existing patterns (HttpConnection, DBSQLLogger)
3. **IClientContext dependency injection**: Consistent with HttpConnection, PlainHttpAuthentication
4. **Config in ClientConfig**: Follows pattern of useCloudFetch, useLZ4Compression
5. **Per-host clients**: Prevents rate limiting for large customers
6. **Circuit breaker**: Production-proven pattern from JDBC
7. **Exception swallowing with IDBSQLLogger**: Customer anxiety avoidance, uses driver's logger
8. **TypeScript**: Maintain type safety throughout

### Pattern Alignment Changes

From original JDBC-inspired design:

- ❌ Removed: `getInstance()` singleton pattern
- ✅ Added: IClientContext parameter to all constructors
- ❌ Removed: console.debug logging
- ✅ Added: logger.log(LogLevel.debug, ...) from context
- ❌ Removed: Config in ClientOptions
- ✅ Added: Config in ClientConfig (existing pattern)
- ❌ Renamed: "Manager" → "Provider"/"Registry"
- ✅ Added: Test stubs in `.stubs/` directory

### Open Questions

1. Should telemetry be enabled by default? **Decision needed before merge**
2. What workspace_id should be used in unauthenticated mode? **TBD**
3. Should we expose telemetry events to customers? **Future sprint**

---

## Appendix

### Reference Documents

- **Design Document**: `spec/telemetry-design.md`
- **JDBC Driver**: `/Users/samikshya.chand/Desktop/databricks-jdbc/`
  - `TelemetryClient.java`
  - `TelemetryClientFactory.java`
  - `CircuitBreakerTelemetryPushClient.java`
  - `TelemetryHelper.java`

### Key Files Created (Summary)

```
lib/telemetry/
├── types.ts                         # Type definitions
├── FeatureFlagCache.ts              # Per-host feature flag cache (instance)
├── TelemetryClientProvider.ts       # Per-host client provider (instance)
├── TelemetryClient.ts               # Client wrapper
├── CircuitBreaker.ts                # Circuit breaker + registry
├── ExceptionClassifier.ts           # Terminal vs retryable
├── TelemetryEventEmitter.ts         # Event emission
├── MetricsAggregator.ts             # Event aggregation
└── DatabricksTelemetryExporter.ts   # Export to Databricks

lib/contracts/IClientContext.ts      # Add telemetry config to ClientConfig

tests/unit/.stubs/
├── ClientContextStub.ts             # Mock IClientContext
├── TelemetryExporterStub.ts         # Mock exporter
└── CircuitBreakerStub.ts            # Mock circuit breaker

tests/unit/telemetry/
├── FeatureFlagCache.test.ts
├── TelemetryClientProvider.test.ts  # Renamed from Manager
├── CircuitBreaker.test.ts
├── ExceptionClassifier.test.ts
├── TelemetryEventEmitter.test.ts
├── MetricsAggregator.test.ts
└── DatabricksTelemetryExporter.test.ts

tests/e2e/telemetry/
└── telemetry-integration.test.ts
```

---

**Sprint Plan Version**: 1.0
**Last Updated**: 2026-01-28
**Status**: Ready for Review
