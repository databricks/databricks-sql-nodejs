import IDBSQLLogger from './IDBSQLLogger';
import IDBSQLSession from './IDBSQLSession';
import IAuthentication from '../connection/contracts/IAuthentication';
import { ProxyOptions } from '../connection/contracts/IConnectionOptions';
import OAuthPersistence from '../connection/auth/DatabricksOAuth/OAuthPersistence';
import ITokenProvider from '../connection/auth/tokenProvider/ITokenProvider';
import { TokenCallback } from '../connection/auth/tokenProvider/ExternalTokenProvider';

export interface ClientOptions {
  logger?: IDBSQLLogger;
}

type AuthOptions =
  | {
      authType?: 'access-token';
      token: string;
    }
  | {
      authType: 'databricks-oauth';
      persistence?: OAuthPersistence;
      azureTenantId?: string;
      oauthClientId?: string;
      oauthClientSecret?: string;
      useDatabricksOAuthInAzure?: boolean;
      // OAuth scopes to request. When omitted, the kernel backend defaults the
      // U2M flow to `['sql', 'offline_access']` (parity with the Thrift driver's
      // `defaultOAuthScopes`), overriding the kernel's bare `all-apis offline_access`.
      oauthScopes?: Array<string>;
    }
  | {
      authType: 'custom';
      provider: IAuthentication;
    }
  | {
      authType: 'token-provider';
      tokenProvider: ITokenProvider;
      enableTokenFederation?: boolean;
      federationClientId?: string;
    }
  | {
      authType: 'external-token';
      getToken: TokenCallback;
      enableTokenFederation?: boolean;
      federationClientId?: string;
    }
  | {
      authType: 'static-token';
      staticToken: string;
      enableTokenFederation?: boolean;
      federationClientId?: string;
    };

export type ConnectionOptions = {
  host: string;
  port?: number;
  path: string;
  userAgentEntry?: string;
  socketTimeout?: number;
  proxy?: ProxyOptions;
  enableMetricViewMetadata?: boolean;

  /**
   * Preserve full numeric precision in results. When `true`, DECIMAL columns
   * are returned as exact strings and 64-bit integers (BIGINT) as JS `bigint`,
   * instead of the default lossy coercion to a JS `number` (which silently
   * rounds DECIMALs and integers beyond 2^53). Applies to both the Thrift and
   * kernel backends. Defaults to `false` to preserve the existing representation.
   */
  preserveBigNumericPrecision?: boolean;

  /**
   * Extra HTTP headers attached to driver-owned out-of-band requests
   * (telemetry POSTs and feature-flag GETs). Not applied to the primary
   * Thrift transport or to OAuth/OIDC token requests.
   *
   * When `path` contains `?o=<workspaceId>` (SPOG account-level routing),
   * the driver automatically injects an `x-databricks-org-id` header unless
   * one is already present in this map.
   */
  customHeaders?: Record<string, string>;

  /**
   * Whether the driver emits telemetry events (connection / statement /
   * cloud-fetch / error). Defaults to `true`.
   *
   * Activation is gated by **two** conditions:
   *   1. This flag is `true` **and**
   *   2. The remote feature flag for the workspace allows telemetry.
   *
   * Setting this to `false` is a hard, unconditional opt-out. Setting to
   * `true` only requests telemetry; the workspace must also allow it.
   *
   * The environment variable `DATABRICKS_TELEMETRY_DISABLED` set to one of
   * `1`, `true`, `yes`, or `on` (case-insensitive) overrides this flag and
   * disables telemetry entirely.
   */
  telemetryEnabled?: boolean;

  /**
   * Maximum number of metrics to batch before flushing to the telemetry
   * endpoint. Default 100.
   */
  telemetryBatchSize?: number;

  /**
   * How often to flush buffered telemetry metrics, in milliseconds.
   * The flush timer is `unref()`'d so it cannot keep the Node.js process
   * alive on its own. Default 5000ms.
   */
  telemetryFlushIntervalMs?: number;

  /**
   * Maximum retry attempts for a telemetry export *after* the initial call.
   * Default 3.
   */
  telemetryMaxRetries?: number;

  /**
   * When `true`, telemetry is sent to the authenticated `/telemetry-ext`
   * endpoint with workspace + session + statement IDs and a system
   * configuration block. When `false`, only error names are emitted via the
   * unauthenticated endpoint. Default `true`.
   *
   * Privacy-relevant: setting `false` minimizes the data surface at the
   * cost of losing most observability.
   */
  telemetryAuthenticatedExport?: boolean;

  /**
   * Number of consecutive telemetry export failures before the per-host
   * circuit breaker trips and pauses exports. Default 5.
   */
  telemetryCircuitBreakerThreshold?: number;

  /**
   * How long the circuit breaker stays open before re-probing the
   * telemetry endpoint, in milliseconds. Default 60000ms (1 minute).
   */
  telemetryCircuitBreakerTimeout?: number;

  /**
   * Maximum wall-clock time `client.close()` will wait for the final
   * telemetry flush HTTP POST. Bounds shutdown latency so callers
   * doing `await client.close(); process.exit(0)` are not held up by a
   * misbehaving telemetry endpoint. Default 2000ms.
   */
  telemetryCloseTimeoutMs?: number;

  /**
   * Hard cap on the per-statement aggregation map size. When the cap is
   * reached, the oldest entry is evicted (its buffered errors are emitted
   * as standalone metrics first so the first-failure signal survives).
   * Default 5000.
   */
  telemetryMaxStatementMetrics?: number;

  /**
   * Maximum number of telemetry metrics buffered in memory before the
   * oldest non-error entry is dropped. Raise this when
   * `getTelemetryStats().droppedMetrics` increases between observations,
   * which indicates the buffer is filling faster than the flush interval
   * can drain it. Default 500.
   */
  telemetryMaxPendingMetrics?: number;
} & AuthOptions;

export interface OpenSessionRequest {
  initialCatalog?: string;
  initialSchema?: string;
  configuration?: { [key: string]: string };
  /**
   * Session-level query tags as key-value pairs. Serialized and passed via session configuration
   * as "QUERY_TAGS". Values may be null/undefined to include a key without a value.
   * If both queryTags and configuration.QUERY_TAGS are specified, queryTags takes precedence.
   */
  queryTags?: Record<string, string | null | undefined>;
}

export default interface IDBSQLClient {
  connect(options: ConnectionOptions): Promise<IDBSQLClient>;

  openSession(request?: OpenSessionRequest): Promise<IDBSQLSession>;

  close(): Promise<void>;
}
