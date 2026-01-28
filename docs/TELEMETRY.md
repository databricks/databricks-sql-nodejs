# Databricks SQL Driver for Node.js - Telemetry

## Table of Contents

- [Overview](#overview)
- [Privacy-First Design](#privacy-first-design)
- [Configuration](#configuration)
  - [Client Configuration](#client-configuration)
  - [Configuration Options](#configuration-options)
  - [Example Configurations](#example-configurations)
- [Event Types and Data Collection](#event-types-and-data-collection)
  - [Connection Events](#connection-events)
  - [Statement Events](#statement-events)
  - [CloudFetch Events](#cloudfetch-events)
  - [Error Events](#error-events)
- [Feature Control](#feature-control)
  - [Server-Side Feature Flag](#server-side-feature-flag)
  - [Client-Side Override](#client-side-override)
- [Architecture](#architecture)
  - [Per-Host Management](#per-host-management)
  - [Circuit Breaker Protection](#circuit-breaker-protection)
  - [Exception Handling](#exception-handling)
- [Troubleshooting](#troubleshooting)
  - [Telemetry Not Working](#telemetry-not-working)
  - [Circuit Breaker Issues](#circuit-breaker-issues)
  - [Debug Logging](#debug-logging)
- [Privacy & Compliance](#privacy--compliance)
  - [Data Never Collected](#data-never-collected)
  - [Data Always Collected](#data-always-collected)
  - [Compliance Standards](#compliance-standards)
- [Performance Impact](#performance-impact)
- [FAQ](#faq)

---

## Overview

The Databricks SQL Driver for Node.js includes an event-based telemetry system that collects driver usage metrics and performance data. This telemetry helps Databricks:

- Track driver adoption and feature usage (e.g., CloudFetch, Arrow format)
- Monitor driver performance and identify bottlenecks
- Improve product quality through data-driven insights
- Provide better customer support

**Key Features:**
- **Privacy-first**: No PII, query text, or sensitive data is collected
- **Opt-in by default**: Telemetry is disabled by default (controlled via server-side feature flag)
- **Non-blocking**: All telemetry operations are asynchronous and never block your application
- **Resilient**: Circuit breaker protection prevents telemetry failures from affecting your application
- **Transparent**: This documentation describes exactly what data is collected

---

## Privacy-First Design

The telemetry system follows a **privacy-first design** that ensures no sensitive information is ever collected:

### Data Never Collected

- ❌ SQL query text
- ❌ Query results or data values
- ❌ Table names, column names, or schema information
- ❌ User identities (usernames, email addresses)
- ❌ Credentials, passwords, or authentication tokens
- ❌ IP addresses or network information
- ❌ Environment variables or system configurations

### Data Always Collected

- ✅ Driver version and configuration settings
- ✅ Operation latency and performance metrics
- ✅ Error types and status codes (not full stack traces with PII)
- ✅ Feature flag states (boolean settings)
- ✅ Statement/session IDs (randomly generated UUIDs)
- ✅ Aggregated metrics (counts, bytes, chunk sizes)
- ✅ Workspace ID (for correlation only)

See [Privacy & Compliance](#privacy--compliance) for more details.

---

## Configuration

Telemetry is **disabled by default** and controlled by a server-side feature flag. You can override this setting in your application if needed.

### Client Configuration

Telemetry settings are configured through the `DBSQLClient` constructor and can be overridden per connection:

```javascript
const { DBSQLClient } = require('@databricks/sql');

const client = new DBSQLClient({
  // Telemetry configuration (all optional)
  telemetryEnabled: true,              // Enable/disable telemetry (default: false)
  telemetryBatchSize: 100,             // Number of events to batch before sending (default: 100)
  telemetryFlushIntervalMs: 5000,      // Time interval to flush metrics in ms (default: 5000)
  telemetryMaxRetries: 3,              // Maximum retry attempts for export (default: 3)
  telemetryAuthenticatedExport: true,  // Use authenticated endpoint (default: true)
  telemetryCircuitBreakerThreshold: 5, // Circuit breaker failure threshold (default: 5)
  telemetryCircuitBreakerTimeout: 60000, // Circuit breaker timeout in ms (default: 60000)
});
```

You can also override telemetry settings per connection:

```javascript
await client.connect({
  host: '********.databricks.com',
  path: '/sql/2.0/warehouses/****************',
  token: 'dapi********************************',
  telemetryEnabled: true,  // Override default setting for this connection
});
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `telemetryEnabled` | `boolean` | `false` | Enable or disable telemetry collection. Even when enabled, the server-side feature flag must also be enabled. |
| `telemetryBatchSize` | `number` | `100` | Maximum number of events to accumulate before sending to the telemetry service. Larger values reduce network overhead but increase memory usage. |
| `telemetryFlushIntervalMs` | `number` | `5000` (5 sec) | Time interval in milliseconds to automatically flush pending metrics. Ensures metrics are sent even if batch size isn't reached. |
| `telemetryMaxRetries` | `number` | `3` | Maximum number of retry attempts when the telemetry export fails with retryable errors (e.g., network timeouts, 500 errors). |
| `telemetryAuthenticatedExport` | `boolean` | `true` | Whether to use the authenticated telemetry endpoint (`/api/2.0/sql/telemetry-ext`). If false, uses the unauthenticated endpoint (`/api/2.0/sql/telemetry-unauth`). |
| `telemetryCircuitBreakerThreshold` | `number` | `5` | Number of consecutive failures before the circuit breaker opens. When open, telemetry events are dropped to prevent wasting resources on a failing endpoint. |
| `telemetryCircuitBreakerTimeout` | `number` | `60000` (60 sec) | Time in milliseconds the circuit breaker stays open before attempting to recover. After this timeout, the circuit breaker enters a half-open state to test if the endpoint has recovered. |

### Example Configurations

#### Basic Usage (Default Settings)

The simplest approach is to let the server-side feature flag control telemetry:

```javascript
const { DBSQLClient } = require('@databricks/sql');

const client = new DBSQLClient();

await client.connect({
  host: 'my-workspace.databricks.com',
  path: '/sql/2.0/warehouses/abc123',
  token: 'dapi...',
});
// Telemetry will be enabled/disabled based on server feature flag
```

#### Explicitly Enable Telemetry

To force telemetry to be enabled (if permitted by server):

```javascript
const client = new DBSQLClient({
  telemetryEnabled: true,
});

await client.connect({
  host: 'my-workspace.databricks.com',
  path: '/sql/2.0/warehouses/abc123',
  token: 'dapi...',
});
```

#### Disable Telemetry

To completely disable telemetry collection:

```javascript
const client = new DBSQLClient({
  telemetryEnabled: false,
});

await client.connect({
  host: 'my-workspace.databricks.com',
  path: '/sql/2.0/warehouses/abc123',
  token: 'dapi...',
});
```

#### Custom Batch and Flush Settings

For high-throughput applications, you may want to adjust batching:

```javascript
const client = new DBSQLClient({
  telemetryEnabled: true,
  telemetryBatchSize: 200,        // Send larger batches
  telemetryFlushIntervalMs: 10000, // Flush every 10 seconds
});
```

#### Development/Testing Configuration

For development, you might want more aggressive flushing:

```javascript
const client = new DBSQLClient({
  telemetryEnabled: true,
  telemetryBatchSize: 10,         // Smaller batches
  telemetryFlushIntervalMs: 1000, // Flush every second
});
```

---

## Event Types and Data Collection

The driver emits telemetry events at key operations throughout the query lifecycle. Events are aggregated by statement and exported in batches.

### Connection Events

**Event Type**: `connection.open`

**When Emitted**: Once per connection, when the session is successfully opened.

**Data Collected**:
- `sessionId`: Unique identifier for the session (UUID)
- `workspaceId`: Workspace identifier (extracted from hostname)
- `driverConfig`: Driver configuration metadata:
  - `driverVersion`: Version of the Node.js SQL driver
  - `driverName`: Always "databricks-sql-nodejs"
  - `nodeVersion`: Node.js runtime version
  - `platform`: Operating system platform (linux, darwin, win32)
  - `osVersion`: Operating system version
  - `cloudFetchEnabled`: Whether CloudFetch is enabled
  - `lz4Enabled`: Whether LZ4 compression is enabled
  - `arrowEnabled`: Whether Arrow format is enabled
  - `directResultsEnabled`: Whether direct results are enabled
  - `socketTimeout`: Configured socket timeout in milliseconds
  - `retryMaxAttempts`: Maximum retry attempts configured
  - `cloudFetchConcurrentDownloads`: Number of concurrent CloudFetch downloads

**Example**:
```json
{
  "eventType": "connection.open",
  "timestamp": 1706453213456,
  "sessionId": "01234567-89ab-cdef-0123-456789abcdef",
  "workspaceId": "1234567890123456",
  "driverConfig": {
    "driverVersion": "3.5.0",
    "driverName": "databricks-sql-nodejs",
    "nodeVersion": "20.10.0",
    "platform": "linux",
    "osVersion": "5.4.0-1153-aws-fips",
    "cloudFetchEnabled": true,
    "lz4Enabled": true,
    "arrowEnabled": false,
    "directResultsEnabled": false,
    "socketTimeout": 900000,
    "retryMaxAttempts": 30,
    "cloudFetchConcurrentDownloads": 10
  }
}
```

### Statement Events

**Event Type**: `statement.start` and `statement.complete`

**When Emitted**:
- `statement.start`: When a SQL statement begins execution
- `statement.complete`: When statement execution finishes (success or failure)

**Data Collected**:
- `statementId`: Unique identifier for the statement (UUID)
- `sessionId`: Session ID for correlation
- `operationType`: Type of SQL operation (SELECT, INSERT, etc.) - *only for start event*
- `latencyMs`: Total execution latency in milliseconds - *only for complete event*
- `resultFormat`: Format of results (inline, cloudfetch, arrow) - *only for complete event*
- `pollCount`: Number of status poll operations performed - *only for complete event*
- `chunkCount`: Number of result chunks downloaded - *only for complete event*
- `bytesDownloaded`: Total bytes downloaded - *only for complete event*

**Example (statement.complete)**:
```json
{
  "eventType": "statement.complete",
  "timestamp": 1706453214567,
  "statementId": "fedcba98-7654-3210-fedc-ba9876543210",
  "sessionId": "01234567-89ab-cdef-0123-456789abcdef",
  "latencyMs": 1234,
  "resultFormat": "cloudfetch",
  "pollCount": 5,
  "chunkCount": 12,
  "bytesDownloaded": 104857600
}
```

### CloudFetch Events

**Event Type**: `cloudfetch.chunk`

**When Emitted**: Each time a CloudFetch chunk is downloaded from cloud storage.

**Data Collected**:
- `statementId`: Statement ID for correlation
- `chunkIndex`: Index of the chunk in the result set (0-based)
- `latencyMs`: Download latency for this chunk in milliseconds
- `bytes`: Size of the chunk in bytes
- `compressed`: Whether the chunk was compressed

**Example**:
```json
{
  "eventType": "cloudfetch.chunk",
  "timestamp": 1706453214123,
  "statementId": "fedcba98-7654-3210-fedc-ba9876543210",
  "chunkIndex": 3,
  "latencyMs": 45,
  "bytes": 8388608,
  "compressed": true
}
```

### Error Events

**Event Type**: `error`

**When Emitted**: When an error occurs during query execution. Terminal errors (authentication failures, invalid syntax) are flushed immediately. Retryable errors (network timeouts, server errors) are buffered and sent when the statement completes.

**Data Collected**:
- `statementId`: Statement ID for correlation (if available)
- `sessionId`: Session ID for correlation (if available)
- `errorName`: Error type/name (e.g., "AuthenticationError", "TimeoutError")
- `errorMessage`: Error message (sanitized, no PII)
- `isTerminal`: Whether the error is terminal (non-retryable)

**Example**:
```json
{
  "eventType": "error",
  "timestamp": 1706453214890,
  "statementId": "fedcba98-7654-3210-fedc-ba9876543210",
  "sessionId": "01234567-89ab-cdef-0123-456789abcdef",
  "errorName": "TimeoutError",
  "errorMessage": "Operation timed out after 30000ms",
  "isTerminal": false
}
```

---

## Feature Control

Telemetry is controlled by **both** a server-side feature flag and a client-side configuration setting.

### Server-Side Feature Flag

The Databricks server controls whether telemetry is enabled for a given workspace via a feature flag:

**Feature Flag Name**: `databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForNodeJs`

**Behavior**:
- The driver queries this feature flag when opening a connection
- If the flag is **disabled**, telemetry is **not collected**, regardless of client configuration
- If the flag is **enabled**, telemetry collection follows the client configuration
- The feature flag is cached for **15 minutes** per host to avoid rate limiting
- Multiple connections to the same host share the same cached feature flag value

**Why Server-Side Control?**
- Allows Databricks to control telemetry rollout across workspaces
- Enables quick disable in case of issues
- Provides per-workspace granularity

### Client-Side Override

The client-side `telemetryEnabled` setting provides an additional control:

**Decision Matrix**:

| Server Feature Flag | Client `telemetryEnabled` | Result |
|---------------------|---------------------------|--------|
| Disabled | `true` | Telemetry **disabled** (server wins) |
| Disabled | `false` | Telemetry **disabled** |
| Enabled | `true` | Telemetry **enabled** |
| Enabled | `false` | Telemetry **disabled** (client can opt-out) |

**In summary**: Both must be enabled for telemetry to be collected.

---

## Architecture

### Per-Host Management

The telemetry system uses **per-host** management to prevent rate limiting and optimize resource usage:

**Key Concepts**:
- **One telemetry client per host**: Multiple connections to the same Databricks host share a single telemetry client
- **Reference counting**: The shared client is only closed when the last connection to that host closes
- **Feature flag caching**: Feature flags are cached per host for 15 minutes to avoid repeated API calls

**Why Per-Host?**
- Large applications may open many parallel connections to the same warehouse
- A single shared client batches events from all connections, reducing network overhead
- Prevents rate limiting on the telemetry endpoint

### Circuit Breaker Protection

The circuit breaker protects your application from telemetry endpoint failures:

**States**:
1. **CLOSED** (normal): Telemetry requests are sent normally
2. **OPEN** (failing): After 5 consecutive failures, requests are rejected immediately (events dropped)
3. **HALF_OPEN** (testing): After 60 seconds, a test request is allowed to check if the endpoint recovered

**State Transitions**:
- **CLOSED → OPEN**: After `telemetryCircuitBreakerThreshold` consecutive failures (default: 5)
- **OPEN → HALF_OPEN**: After `telemetryCircuitBreakerTimeout` milliseconds (default: 60000 = 1 minute)
- **HALF_OPEN → CLOSED**: After 2 consecutive successes
- **HALF_OPEN → OPEN**: On any failure

**Why Circuit Breaker?**
- Prevents wasting resources on a failing telemetry endpoint
- Automatically recovers when the endpoint becomes healthy
- Isolates failures per host (one host's circuit breaker doesn't affect others)

### Exception Handling

The telemetry system follows a **strict exception swallowing policy**:

**Principle**: **No telemetry exception should ever impact your application.**

**Implementation**:
- All telemetry operations are wrapped in try-catch blocks
- All exceptions are caught and logged at `debug` level only (never `warn` or `error`)
- No exceptions propagate to application code
- The driver continues normally even if telemetry completely fails

**What This Means for You**:
- Telemetry failures won't cause your queries to fail
- You won't see error logs from telemetry in production (only debug logs)
- Your application performance is unaffected by telemetry issues

---

## Troubleshooting

### Telemetry Not Working

**Symptom**: Telemetry data is not being sent or logged.

**Possible Causes and Solutions**:

1. **Telemetry disabled by default**
   - **Solution**: Explicitly enable in client configuration:
     ```javascript
     const client = new DBSQLClient({
       telemetryEnabled: true,
     });
     ```

2. **Server feature flag disabled**
   - **Check**: Look for debug log: `"Telemetry disabled via feature flag"`
   - **Solution**: This is controlled by Databricks. If you believe it should be enabled, contact Databricks support.

3. **Circuit breaker is OPEN**
   - **Check**: Look for debug log: `"Circuit breaker OPEN - dropping telemetry"`
   - **Solution**: The circuit breaker opens after repeated failures. It will automatically attempt recovery after 60 seconds. Check network connectivity and Databricks service status.

4. **Debug logging not visible**
   - **Solution**: Enable debug logging in your logger:
     ```javascript
     const client = new DBSQLClient({
       // Use a logger that shows debug messages
     });
     ```

### Circuit Breaker Issues

**Symptom**: Circuit breaker frequently opens, telemetry events are dropped.

**Possible Causes**:
- Network connectivity issues
- Databricks telemetry service unavailable
- Rate limiting (if using multiple connections)
- Authentication failures

**Debugging Steps**:

1. **Check debug logs** for circuit breaker state transitions:
   ```
   [DEBUG] Circuit breaker transitioned to OPEN (will retry after 60000ms)
   [DEBUG] Circuit breaker failure (5/5)
   ```

2. **Verify network connectivity** to Databricks host

3. **Check authentication** - ensure your token is valid and has necessary permissions

4. **Adjust circuit breaker settings** if needed:
   ```javascript
   const client = new DBSQLClient({
     telemetryCircuitBreakerThreshold: 10,  // More tolerant
     telemetryCircuitBreakerTimeout: 30000, // Retry sooner
   });
   ```

### Debug Logging

To see detailed telemetry debug logs, use a logger that captures debug level messages:

```javascript
const { DBSQLClient, LogLevel } = require('@databricks/sql');

const client = new DBSQLClient();

// All telemetry logs will be at LogLevel.debug
// Configure your logger to show debug messages
```

**Useful Debug Log Messages**:
- `"Telemetry initialized"` - Telemetry system started successfully
- `"Telemetry disabled via feature flag"` - Server feature flag disabled
- `"Circuit breaker transitioned to OPEN"` - Circuit breaker opened due to failures
- `"Circuit breaker transitioned to CLOSED"` - Circuit breaker recovered
- `"Telemetry export error: ..."` - Export failed (with reason)

---

## Privacy & Compliance

### Data Never Collected

The telemetry system is designed to **never collect** sensitive information:

- **SQL Query Text**: The actual SQL statements you execute are never collected
- **Query Results**: Data returned from queries is never collected
- **Schema Information**: Table names, column names, database names are never collected
- **User Identities**: Usernames, email addresses, or user IDs are never collected (only workspace ID for correlation)
- **Credentials**: Passwords, tokens, API keys, or any authentication information is never collected
- **Network Information**: IP addresses, hostnames, or network topology is never collected
- **Environment Variables**: System environment variables or configuration files are never collected

### Data Always Collected

The following **non-sensitive** data is collected:

**Driver Metadata** (collected once per connection):
- Driver version (e.g., "3.5.0")
- Driver name ("databricks-sql-nodejs")
- Node.js version (e.g., "20.10.0")
- Platform (linux, darwin, win32)
- OS version
- Feature flags (boolean values: CloudFetch enabled, LZ4 enabled, etc.)
- Configuration values (timeouts, retry counts, etc.)

**Performance Metrics** (collected per statement):
- Execution latency in milliseconds
- Number of poll operations
- Number of result chunks
- Total bytes downloaded
- Result format (inline, cloudfetch, arrow)

**Correlation IDs** (for data aggregation):
- Session ID (randomly generated UUID, not tied to user identity)
- Statement ID (randomly generated UUID)
- Workspace ID (for grouping metrics by workspace)

**Error Information** (when errors occur):
- Error type/name (e.g., "TimeoutError", "AuthenticationError")
- HTTP status codes (e.g., 401, 500)
- Error messages (sanitized, no PII or sensitive data)

### Compliance Standards

The telemetry system is designed to comply with major privacy regulations:

**GDPR (General Data Protection Regulation)**:
- No personal data is collected
- UUIDs are randomly generated and not tied to individuals
- Workspace ID is used only for technical correlation

**CCPA (California Consumer Privacy Act)**:
- No personal information is collected
- No sale or sharing of personal data

**SOC 2 (Service Organization Control 2)**:
- All telemetry data is encrypted in transit using HTTPS
- Data is sent to Databricks-controlled endpoints
- Uses existing authentication mechanisms (no separate credentials)

**Data Residency**:
- Telemetry data is sent to the same regional Databricks control plane as your workloads
- No cross-region data transfer

---

## Performance Impact

The telemetry system is designed to have **minimal performance impact** on your application:

### When Telemetry is Disabled

- **Overhead**: ~0% (telemetry code paths are skipped entirely)
- **Memory**: No additional memory usage
- **Network**: No additional network traffic

### When Telemetry is Enabled

- **Overhead**: < 1% of query execution time
- **Event Emission**: < 1 microsecond per event (non-blocking)
- **Memory**: Minimal (~100 events buffered = ~100KB)
- **Network**: Batched exports every 5 seconds (configurable)

**Design Principles for Low Overhead**:
1. **Non-blocking**: All telemetry operations use asynchronous Promises
2. **Fire-and-forget**: Event emission doesn't wait for export completion
3. **Batching**: Events are aggregated and sent in batches to minimize network calls
4. **Circuit breaker**: Stops attempting exports if the endpoint is failing
5. **Exception swallowing**: No overhead from exception propagation

---

## FAQ

### Q: Is telemetry enabled by default?

**A**: No. Telemetry is **disabled by default** (`telemetryEnabled: false`). Even if you set `telemetryEnabled: true`, the server-side feature flag must also be enabled for telemetry to be collected.

### Q: Can I disable telemetry completely?

**A**: Yes. Set `telemetryEnabled: false` in your client configuration:

```javascript
const client = new DBSQLClient({
  telemetryEnabled: false,
});
```

This ensures telemetry is never collected, regardless of the server feature flag.

### Q: What if telemetry collection fails?

**A**: Telemetry failures **never impact your application**. All exceptions are caught, logged at debug level, and swallowed. Your queries will execute normally even if telemetry completely fails.

### Q: How much network bandwidth does telemetry use?

**A**: Very little. Events are batched (default: 100 events per request) and sent every 5 seconds. A typical batch is a few kilobytes. High-throughput applications can adjust batch size to reduce network overhead.

### Q: Can I see what telemetry data is being sent?

**A**: Yes. Enable debug logging in your logger to see all telemetry events being collected and exported. See [Debug Logging](#debug-logging).

### Q: Does telemetry collect my SQL queries?

**A**: **No**. SQL query text is **never collected**. Only performance metrics (latency, chunk counts, bytes downloaded) and error types are collected. See [Privacy-First Design](#privacy-first-design).

### Q: What happens when the circuit breaker opens?

**A**: When the circuit breaker opens (after 5 consecutive export failures), telemetry events are **dropped** to prevent wasting resources. The circuit breaker automatically attempts recovery after 60 seconds. Your application continues normally.

### Q: Can I control telemetry per query?

**A**: No. Telemetry is controlled at the client and connection level. Once enabled, telemetry is collected for all queries on that connection. To disable telemetry for specific queries, use a separate connection with `telemetryEnabled: false`.

### Q: How is telemetry data secured?

**A**: Telemetry data is sent over **HTTPS** using the same authentication as your queries. It uses your existing Databricks token or credentials. All data is encrypted in transit.

### Q: Where is telemetry data sent?

**A**: Telemetry data is sent to Databricks-controlled telemetry endpoints:
- **Authenticated**: `https://<your-host>/api/2.0/sql/telemetry-ext`
- **Unauthenticated**: `https://<your-host>/api/2.0/sql/telemetry-unauth`

The data stays within the same Databricks region as your workloads.

### Q: Can I export telemetry to my own monitoring system?

**A**: Not currently. Telemetry is designed to send data to Databricks for product improvement. If you need custom monitoring, consider implementing your own instrumentation using the driver's existing logging and error handling.

---

## Additional Resources

- [Design Document](../spec/telemetry-design.md) - Detailed technical design
- [Sprint Plan](../spec/telemetry-sprint-plan.md) - Implementation roadmap
- [README](../README.md) - Driver overview and setup
- [Contributing Guide](../CONTRIBUTING.md) - How to contribute

For questions or issues with telemetry, please open an issue on [GitHub](https://github.com/databricks/databricks-sql-nodejs/issues).
