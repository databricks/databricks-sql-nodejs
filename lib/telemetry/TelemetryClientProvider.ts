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

import IClientContext from '../contracts/IClientContext';
import { LogLevel } from '../contracts/IDBSQLLogger';
import TelemetryClient from './TelemetryClient';

interface TelemetryClientHolder {
  client: TelemetryClient;
  refCount: number;
}

// Soft cap on distinct host entries. Above this the provider warns once so a
// misconfigured caller (per-request hosts, unnormalized aliases) is visible
// in logs rather than silently growing the map.
const MAX_CLIENTS_SOFT_LIMIT = 128;

/**
 * Process-wide registry of `TelemetryClient`s, one per host. Multiple
 * `DBSQLClient` instances connecting to the same host share the same
 * `TelemetryClient`, which owns the host-scoped circuit breaker, feature
 * flag cache, exporter, and aggregator.
 *
 * Singleton because the resources we're sharing — circuit-breaker counters,
 * batched HTTP exports — are correct only at process scope. Per-`DBSQLClient`
 * provider scope (the previous design) deduplicates nothing.
 *
 * `getOrCreateClient` is sync: caller increments refcount, registers its
 * context+auth, and walks away. `releaseClient` is `async` because the
 * underlying `TelemetryClient.close()` awaits the final flush.
 */
class TelemetryClientProvider {
  private static instance: TelemetryClientProvider | undefined;

  private clients = new Map<string, TelemetryClientHolder>();

  private softLimitWarned = false;

  // Production code should use `TelemetryClientProvider.getInstance()` for
  // the process-wide singleton. The constructor remains public so unit tests
  // can build an isolated provider with its own map. Deliberate no-op body —
  // initial state is set inline on the field declarations above.

  static getInstance(): TelemetryClientProvider {
    if (!TelemetryClientProvider.instance) {
      TelemetryClientProvider.instance = new TelemetryClientProvider();
    }
    return TelemetryClientProvider.instance;
  }

  /**
   * Reset the process-wide singleton. Test-only — name-prefixed so
   * production callsites can't reach for it accidentally via autocomplete.
   * Resetting in production drops every host's circuit-breaker counters,
   * feature-flag cache, exporter, and pending-metric buffer at once.
   *
   * @internal Test-only. Production code MUST NOT call this.
   */
  static __resetInstanceForTests(): void {
    TelemetryClientProvider.instance = undefined;
  }

  private static normalizeHostKey(host: string): string {
    return host
      .trim()
      .toLowerCase()
      .replace(/^https?:\/\//, '')
      .replace(/\/+$/, '')
      .replace(/\.$/, '')
      .replace(/:443$/, '');
  }

  /**
   * Get or create the `TelemetryClient` for `host`, registering `context` as
   * a participant. Increments refcount.
   */
  getOrCreateClient(context: IClientContext, host: string): TelemetryClient {
    const logger = context.getLogger();
    const key = TelemetryClientProvider.normalizeHostKey(host);
    let holder = this.clients.get(key);

    if (!holder) {
      const client = new TelemetryClient(context, key);
      holder = { client, refCount: 0 };
      this.clients.set(key, holder);
      logger.log(LogLevel.debug, `Created new TelemetryClient for host: ${host}`);

      if (!this.softLimitWarned && this.clients.size > MAX_CLIENTS_SOFT_LIMIT) {
        this.softLimitWarned = true;
        logger.log(
          LogLevel.warn,
          `TelemetryClientProvider has ${this.clients.size} distinct hosts — possible alias or leak`,
        );
      }
    } else {
      holder.client.registerContext(context);
    }

    holder.refCount += 1;
    logger.log(LogLevel.debug, `TelemetryClient reference count for ${host}: ${holder.refCount}`);
    return holder.client;
  }

  /**
   * Release `context`'s registration. When the last `DBSQLClient` releases,
   * the underlying `TelemetryClient.close()` runs and the entry is removed.
   */
  async releaseClient(context: IClientContext, host: string): Promise<void> {
    const logger = context.getLogger();
    const key = TelemetryClientProvider.normalizeHostKey(host);
    const holder = this.clients.get(key);

    if (!holder) {
      logger.log(LogLevel.debug, `No TelemetryClient found for host: ${host}`);
      return;
    }

    if (holder.refCount <= 0) {
      logger.log(LogLevel.warn, `Unbalanced release for TelemetryClient host: ${host}`);
      return;
    }

    // Skip unregister on the last release so close()'s final flush can still
    // resolve auth/connection providers from the FIFO snapshot.
    if (holder.refCount > 1) {
      holder.client.unregisterContext(context);
    }
    holder.refCount -= 1;
    logger.log(LogLevel.debug, `TelemetryClient reference count for ${host}: ${holder.refCount}`);

    if (holder.refCount <= 0) {
      // Remove from map BEFORE awaiting close so a concurrent
      // getOrCreateClient creates a fresh instance rather than receiving
      // this closing one.
      this.clients.delete(key);
      try {
        await holder.client.close();
        logger.log(LogLevel.debug, `Closed and removed TelemetryClient for host: ${host}`);
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        logger.log(LogLevel.debug, `Error releasing TelemetryClient: ${msg}`);
      }
    }
  }

  /** @internal Exposed for testing only. */
  getRefCount(host: string): number {
    const holder = this.clients.get(TelemetryClientProvider.normalizeHostKey(host));
    return holder ? holder.refCount : 0;
  }

  /** @internal Exposed for testing only. */
  getActiveClients(): Map<string, TelemetryClient> {
    const result = new Map<string, TelemetryClient>();
    for (const [host, holder] of this.clients.entries()) {
      result.set(host, holder.client);
    }
    return result;
  }
}

export default TelemetryClientProvider;
