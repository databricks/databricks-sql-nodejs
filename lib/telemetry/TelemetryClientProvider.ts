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

/**
 * Holds a telemetry client and its reference count.
 * The reference count tracks how many connections are using this client.
 */
interface TelemetryClientHolder {
  client: TelemetryClient;
  refCount: number;
}

// Soft cap on distinct host entries. Above this the provider warns once so a
// misconfigured caller (per-request hosts, unnormalized aliases) is visible in
// logs rather than silently growing the map.
const MAX_CLIENTS_SOFT_LIMIT = 128;

/**
 * Manages one telemetry client per host.
 * Prevents rate limiting by sharing clients across connections to the same host.
 * Instance-based (not singleton), stored in DBSQLClient.
 *
 * Reference counts are incremented and decremented synchronously, and
 * `close()` is sync today, so there is no await between map mutation and
 * client teardown. The map entry is removed before `close()` runs so a
 * concurrent `getOrCreateClient` call for the same host gets a fresh
 * instance rather than receiving this closing one. When `close()` becomes
 * async (e.g. HTTP flush in [5/7]) the flow will need to `await` after the
 * delete to preserve the same invariant.
 */
class TelemetryClientProvider {
  private clients: Map<string, TelemetryClientHolder>;

  private softLimitWarned = false;

  constructor(private context: IClientContext) {
    this.clients = new Map();
    const logger = context.getLogger();
    logger.log(LogLevel.debug, 'Created TelemetryClientProvider');
  }

  /**
   * Canonicalize host so aliases (scheme, default port, trailing slash, case,
   * trailing dot, surrounding whitespace) map to the same entry. Kept to a
   * lightweight lexical normalization — `buildTelemetryUrl` still performs
   * the strict security validation when a request is actually built.
   */
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
   * Gets or creates a telemetry client for the specified host.
   * Increments the reference count for the client.
   *
   * @param host The host identifier (e.g., "workspace.cloud.databricks.com")
   * @returns The telemetry client for the host
   */
  getOrCreateClient(host: string): TelemetryClient {
    const logger = this.context.getLogger();
    const key = TelemetryClientProvider.normalizeHostKey(host);
    let holder = this.clients.get(key);

    if (!holder) {
      const client = new TelemetryClient(this.context, key);
      holder = {
        client,
        refCount: 0,
      };
      this.clients.set(key, holder);
      logger.log(LogLevel.debug, `Created new TelemetryClient for host: ${host}`);

      if (!this.softLimitWarned && this.clients.size > MAX_CLIENTS_SOFT_LIMIT) {
        this.softLimitWarned = true;
        logger.log(
          LogLevel.warn,
          `TelemetryClientProvider has ${this.clients.size} distinct hosts — possible alias or leak`,
        );
      }
    }

    holder.refCount += 1;
    logger.log(LogLevel.debug, `TelemetryClient reference count for ${host}: ${holder.refCount}`);

    return holder.client;
  }

  /**
   * Releases a telemetry client for the specified host.
   * Decrements the reference count and closes the client when it reaches zero.
   *
   * @param host The host identifier
   */
  releaseClient(host: string): void {
    const logger = this.context.getLogger();
    const key = TelemetryClientProvider.normalizeHostKey(host);
    const holder = this.clients.get(key);

    if (!holder) {
      logger.log(LogLevel.debug, `No TelemetryClient found for host: ${host}`);
      return;
    }

    // Guard against double-release: a caller releasing more times than it got
    // would otherwise drive refCount negative and close a client another
    // caller is still holding. Warn loudly and refuse to decrement further.
    if (holder.refCount <= 0) {
      logger.log(LogLevel.warn, `Unbalanced release for TelemetryClient host: ${host}`);
      return;
    }

    holder.refCount -= 1;
    logger.log(LogLevel.debug, `TelemetryClient reference count for ${host}: ${holder.refCount}`);

    // Close and remove client when reference count reaches zero.
    // Remove from map before calling close so a concurrent getOrCreateClient
    // creates a fresh client rather than receiving this closing one.
    if (holder.refCount <= 0) {
      this.clients.delete(key);
      try {
        holder.client.close();
        logger.log(LogLevel.debug, `Closed and removed TelemetryClient for host: ${host}`);
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        logger.log(LogLevel.debug, `Error releasing TelemetryClient: ${msg}`);
      }
    }
  }

  /**
   * @internal Exposed for testing only.
   */
  getRefCount(host: string): number {
    const holder = this.clients.get(TelemetryClientProvider.normalizeHostKey(host));
    return holder ? holder.refCount : 0;
  }

  /**
   * @internal Exposed for testing only.
   */
  getActiveClients(): Map<string, TelemetryClient> {
    const result = new Map<string, TelemetryClient>();
    for (const [host, holder] of this.clients.entries()) {
      result.set(host, holder.client);
    }
    return result;
  }
}

export default TelemetryClientProvider;
