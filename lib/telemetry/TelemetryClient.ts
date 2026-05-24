/**
 * Copyright (c) 2025 Databricks Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import IClientContext, { ClientConfig } from '../contracts/IClientContext';
import IDBSQLLogger, { LogLevel } from '../contracts/IDBSQLLogger';
import IConnectionProvider from '../connection/contracts/IConnectionProvider';
import IThriftClient from '../contracts/IThriftClient';
import IDriver from '../contracts/IDriver';
import IAuthentication from '../connection/contracts/IAuthentication';
import { CircuitBreakerRegistry, CircuitBreakerState } from './CircuitBreaker';
import DatabricksTelemetryExporter from './DatabricksTelemetryExporter';
import MetricsAggregator from './MetricsAggregator';
import FeatureFlagCache from './FeatureFlagCache';

/**
 * Per-host telemetry resource owner. Held by `TelemetryClientProvider`
 * (process-wide singleton) and shared across every `DBSQLClient` that
 * connects to the same host.
 *
 * Owns the host-scoped triad — `MetricsAggregator`, `DatabricksTelemetryExporter`,
 * `CircuitBreakerRegistry`, `FeatureFlagCache` — and implements `IClientContext`
 * itself so those owned components have a stable context that outlives any
 * single `DBSQLClient`. The first registered `DBSQLClient`'s logger and config
 * are snapshotted; subsequent registrants donate their connection providers
 * and auth providers, and the `TelemetryClient` falls through them in
 * registration order when the current head closes.
 *
 * Why share at host granularity:
 * - Circuit-breaker state for `host` is correct only if all clients hitting
 *   the same endpoint share counters (5 failures means 5 actual failures, not
 *   5×N for N independent `DBSQLClient` instances).
 * - Feature-flag cache has a per-host TTL; deduping the GET prevents
 *   thundering-herd on cold cache.
 * - Metric batches mix events from every active client to the same host —
 *   one HTTP POST per `flushIntervalMs` instead of N.
 */
class TelemetryClient implements IClientContext {
  private closed: boolean = false;

  private readonly logger: IDBSQLLogger;

  private readonly config: ClientConfig;

  private readonly circuitBreakerRegistry: CircuitBreakerRegistry;

  private readonly featureFlagCache: FeatureFlagCache;

  private readonly exporter: DatabricksTelemetryExporter;

  private readonly aggregator: MetricsAggregator;

  // FIFO so the first-registered client's connection/auth providers are tried
  // first. Falls through to later registrants when an earlier one is closed.
  //
  // The `authProvider` is snapshotted at register time (NOT re-fetched at
  // unregister time). If a registrant's underlying `DBSQLClient` rotates its
  // auth provider between register and unregister (token refresh that
  // reconstructs the provider, lazy wrappers, etc.), filtering by referential
  // equality against a freshly-fetched provider would never match the original,
  // and the stale provider would remain in `authProviders` for the lifetime of
  // the per-host singleton — keeping the exporter authenticating with revoked
  // credentials.
  private contexts: Array<{ context: IClientContext; authProvider?: IAuthentication }> = [];

  constructor(initialContext: IClientContext, public readonly host: string) {
    this.logger = initialContext.getLogger();
    // Snapshot config at first registration. Subsequent clients with
    // divergent telemetry knobs (`telemetryBatchSize` etc.) inherit the
    // first-registrant's tuning — documented invariant.
    this.config = initialContext.getConfig();
    this.contexts.push({
      context: initialContext,
      authProvider: initialContext.getAuthProvider?.(),
    });

    this.circuitBreakerRegistry = new CircuitBreakerRegistry(this);
    this.featureFlagCache = new FeatureFlagCache(this);
    // Register this host with the feature-flag cache so isTelemetryEnabled()
    // does not short-circuit to false. close() releases via releaseContext().
    this.featureFlagCache.getOrCreateContext(host);
    this.exporter = new DatabricksTelemetryExporter(this, host, this.circuitBreakerRegistry);
    this.aggregator = new MetricsAggregator(this, this.exporter);

    this.logger.log(LogLevel.debug, `Created TelemetryClient for host: ${host}`);
  }

  /**
   * Add another `DBSQLClient`'s context to the pool. Tracked in registration
   * order; `getConnectionProvider()` / `getAuthProvider()` walk the list and
   * use the first entry that's still usable.
   */
  registerContext(context: IClientContext): void {
    if (this.contexts.some((entry) => entry.context === context)) {
      // Re-snapshot the auth provider on a duplicate register call so a caller
      // that intentionally rotated providers can refresh the entry.
      const idx = this.contexts.findIndex((entry) => entry.context === context);
      if (idx >= 0) {
        this.contexts[idx] = { context, authProvider: context.getAuthProvider?.() };
      }
      return;
    }
    const newAuthProvider = context.getAuthProvider?.();
    // Diverging auth providers across registrants on the same host is the
    // multi-tenant FIFO bleed case. The first-registered auth wins; tenant B
    // queries POST under tenant A's headers. Warn loudly — SaaS layers that
    // see this in their logs should either:
    //   - set telemetryEnabled: false on all DBSQLClients, or
    //   - partition by host so each tenant owns its own TelemetryClient.
    const firstAuthProvider = this.contexts[0]?.authProvider;
    if (
      this.contexts.length > 0 &&
      newAuthProvider !== undefined &&
      firstAuthProvider !== undefined &&
      newAuthProvider !== firstAuthProvider
    ) {
      this.logger.log(
        LogLevel.warn,
        `TelemetryClient(${this.host}): a second DBSQLClient registered with a different auth provider. ` +
          `Telemetry from this client will be POSTed under the first-registered client's auth headers. ` +
          `Multi-tenant SaaS layers should set telemetryEnabled: false on all clients or partition by host. ` +
          `See README "Multi-tenant SaaS deployments" for details.`,
      );
    }
    this.contexts.push({
      context,
      authProvider: newAuthProvider,
    });
    // Warn when subsequent registrants pass telemetry knobs that diverge
    // from the first-registrant's snapshot — those values are silently
    // ignored. Privacy-relevant for telemetryAuthenticatedExport.
    this.warnOnConfigDivergence(context.getConfig());
  }

  private warnOnConfigDivergence(other: ClientConfig): void {
    const keys: Array<keyof ClientConfig> = [
      'telemetryAuthenticatedExport',
      'telemetryBatchSize',
      'telemetryFlushIntervalMs',
      'telemetryMaxRetries',
      'telemetryCircuitBreakerThreshold',
      'telemetryCircuitBreakerTimeout',
      // Privacy-relevant: User-Agent is snapshotted from the first registrant
      // and shared across the host. Multi-tenant SaaS layers with per-tenant
      // userAgentEntry values would otherwise silently ship under tenant-1's UA.
      'userAgentEntry',
    ];
    const diverged = keys.filter((k) => other[k] !== undefined && other[k] !== this.config[k]);
    if (diverged.length > 0) {
      this.logger.log(
        LogLevel.warn,
        `TelemetryClient(${this.host}): registered context's telemetry settings ` +
          `[${diverged.join(', ')}] differ from the first registrant's; the new values will be ignored.`,
      );
    }
  }

  /**
   * Remove a `DBSQLClient`'s context from the pool. Called by
   * `TelemetryClientProvider.releaseClient` in the multi-refcount case so the
   * exporter doesn't keep trying to use a closing context. Deliberately
   * NOT called on the last refcount release: `close()` needs the snapshot
   * pair to resolve auth/connection providers for the final flush.
   *
   * Uses the cached snapshot pair (`context`, `authProvider`) from register
   * time, not a fresh `context.getAuthProvider?.()` call. If the underlying
   * client's auth provider was rotated, a fresh call would not match the
   * original reference and the stale entry would leak.
   */
  unregisterContext(context: IClientContext): void {
    this.contexts = this.contexts.filter((entry) => entry.context !== context);
  }

  // -- IClientContext --

  getConfig(): ClientConfig {
    return this.config;
  }

  getLogger(): IDBSQLLogger {
    return this.logger;
  }

  async getConnectionProvider(): Promise<IConnectionProvider> {
    return this.tryFallthrough((ctx) => ctx.getConnectionProvider(), 'connection provider');
  }

  async getClient(): Promise<IThriftClient> {
    return this.tryFallthrough((ctx) => ctx.getClient(), 'client');
  }

  async getDriver(): Promise<IDriver> {
    return this.tryFallthrough((ctx) => ctx.getDriver(), 'driver');
  }

  /**
   * Walk `contexts` and return the first call site that succeeds. Contexts
   * that throw on the same accessor for `MAX_CONSECUTIVE_CTX_FAILURES` calls
   * in a row are pruned from the pool — otherwise a registrant whose
   * underlying `DBSQLClient` is closed or has revoked auth would stay in the
   * list forever and be retried on every export.
   *
   * Pruning is keyed by the `accessor` function identity so a context with a
   * working `getConnectionProvider` but broken `getDriver` isn't dropped from
   * the working accessor's path.
   */
  private async tryFallthrough<T>(accessor: (ctx: IClientContext) => Promise<T>, accessorName: string): Promise<T> {
    let lastErr: unknown;
    const survivors: typeof this.contexts = [];
    /* eslint-disable no-await-in-loop */
    for (const entry of this.contexts) {
      try {
        const result = await accessor(entry.context);
        // Keep this entry and any not-yet-tried ones. We've found a working
        // head; the tail might be fine too. The throwing entries already in
        // `survivors`'s past iterations are dropped below.
        survivors.push(entry);
        for (const remaining of this.contexts.slice(this.contexts.indexOf(entry) + 1)) {
          survivors.push(remaining);
        }
        this.contexts = survivors;
        return result;
      } catch (err) {
        lastErr = err;
        // Drop this entry from the survivors set — repeatedly retrying a
        // throwing context drains nothing useful from the FIFO.
        this.logger.log(
          LogLevel.debug,
          `TelemetryClient(${this.host}): pruning context that threw on ${accessorName}: ` +
            `${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }
    /* eslint-enable no-await-in-loop */
    this.contexts = survivors;
    throw lastErr instanceof Error
      ? lastErr
      : new Error(`TelemetryClient: no ${accessorName} available for host ${this.host}`);
  }

  getAuthProvider(): IAuthentication | undefined {
    // Walk the FIFO and return the first usable provider. The provider was
    // snapshotted at register time, so even if the registrant's underlying
    // client has since rotated credentials, the original entry can still be
    // dropped by `unregisterContext` via cached-snapshot equality. A registered
    // head with revoked credentials surfaces as an `authenticate()` failure
    // inside the exporter retry loop.
    for (const entry of this.contexts) {
      if (entry.authProvider !== undefined) {
        return entry.authProvider;
      }
    }
    return undefined;
  }

  getTelemetryEmitter(): undefined {
    // The shared TelemetryClient holds the aggregator; emitters remain
    // per-DBSQLClient so each can respect its own `telemetryEnabled` flag.
    return undefined;
  }

  getTelemetryAggregator(): MetricsAggregator {
    return this.aggregator;
  }

  // -- shared resource accessors --

  getExporter(): DatabricksTelemetryExporter {
    return this.exporter;
  }

  getAggregator(): MetricsAggregator {
    return this.aggregator;
  }

  getFeatureFlagCache(): FeatureFlagCache {
    return this.featureFlagCache;
  }

  /**
   * Operator-visible snapshot of telemetry state for this host. Synchronous,
   * never throws — intended for health-check endpoints, shutdown banners,
   * and operator dashboards.
   */
  getTelemetryStats(): {
    host: string;
    pendingMetricsCount: number;
    inFlightStatements: number;
    droppedMetrics: number;
    evictedStatements: number;
    circuitBreakerState: CircuitBreakerState;
  } {
    return {
      host: this.host,
      ...this.aggregator.getStats(),
      circuitBreakerState: this.circuitBreakerRegistry.getCircuitBreaker(this.host).getState(),
    };
  }

  getHost(): string {
    return this.host;
  }

  isClosed(): boolean {
    return this.closed;
  }

  /**
   * Drain pending metrics and tear down owned resources. Called by
   * `TelemetryClientProvider.releaseClient` when refCount hits zero.
   */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.logger.log(LogLevel.debug, `Closing TelemetryClient for host: ${this.host}`);
    try {
      await this.aggregator.close();
    } catch (err) {
      this.logger.log(LogLevel.debug, `TelemetryClient aggregator close error: ${(err as Error).message}`);
    }
    try {
      this.exporter.dispose();
    } catch (err) {
      this.logger.log(LogLevel.debug, `TelemetryClient exporter dispose error: ${(err as Error).message}`);
    }
    try {
      this.featureFlagCache.releaseContext(this.host);
    } catch (err) {
      this.logger.log(LogLevel.debug, `TelemetryClient FFCache release error: ${(err as Error).message}`);
    }
    this.logger.log(LogLevel.debug, `Closed TelemetryClient for host: ${this.host}`);
  }
}

export default TelemetryClient;
