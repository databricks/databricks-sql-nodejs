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
import { CircuitBreakerRegistry } from './CircuitBreaker';
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
  private contexts: IClientContext[] = [];

  private authProviders: IAuthentication[] = [];

  constructor(initialContext: IClientContext, public readonly host: string) {
    this.logger = initialContext.getLogger();
    // Snapshot config at first registration. Subsequent clients with
    // divergent telemetry knobs (`telemetryBatchSize` etc.) inherit the
    // first-registrant's tuning — documented invariant.
    this.config = initialContext.getConfig();
    this.contexts.push(initialContext);
    const auth = initialContext.getAuthProvider?.();
    if (auth) {
      this.authProviders.push(auth);
    }

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
    if (!this.contexts.includes(context)) {
      this.contexts.push(context);
      // Warn when subsequent registrants pass telemetry knobs that diverge
      // from the first-registrant's snapshot — those values are silently
      // ignored. Privacy-relevant for telemetryAuthenticatedExport.
      this.warnOnConfigDivergence(context.getConfig());
    }
    const auth = context.getAuthProvider?.();
    if (auth && !this.authProviders.includes(auth)) {
      this.authProviders.push(auth);
    }
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
   * `TelemetryClientProvider.releaseClient` before refcount decrement so the
   * exporter doesn't keep trying to use a closed context.
   */
  unregisterContext(context: IClientContext): void {
    this.contexts = this.contexts.filter((c) => c !== context);
    const auth = context.getAuthProvider?.();
    if (auth) {
      this.authProviders = this.authProviders.filter((a) => a !== auth);
    }
  }

  // -- IClientContext --

  getConfig(): ClientConfig {
    return this.config;
  }

  getLogger(): IDBSQLLogger {
    return this.logger;
  }

  async getConnectionProvider(): Promise<IConnectionProvider> {
    let lastErr: unknown;
    for (const ctx of this.contexts) {
      try {
        // Sequential fall-through is intentional — each context returns the
        // same shared connection provider; we try the next registrant only
        // when the current head is unusable.
        // eslint-disable-next-line no-await-in-loop
        return await ctx.getConnectionProvider();
      } catch (err) {
        lastErr = err;
      }
    }
    throw lastErr instanceof Error
      ? lastErr
      : new Error(`TelemetryClient: no connection provider available for host ${this.host}`);
  }

  async getClient(): Promise<IThriftClient> {
    if (this.contexts.length === 0) {
      throw new Error(`TelemetryClient: no client available for host ${this.host}`);
    }
    return this.contexts[0].getClient();
  }

  async getDriver(): Promise<IDriver> {
    if (this.contexts.length === 0) {
      throw new Error(`TelemetryClient: no driver available for host ${this.host}`);
    }
    return this.contexts[0].getDriver();
  }

  getAuthProvider(): IAuthentication | undefined {
    return this.authProviders[0];
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
