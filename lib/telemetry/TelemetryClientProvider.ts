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

/**
 * Manages one telemetry client per host.
 * Prevents rate limiting by sharing clients across connections to the same host.
 * Instance-based (not singleton), stored in DBSQLClient.
 *
 * Pattern from JDBC TelemetryClientFactory.java:27 with
 * ConcurrentHashMap<String, TelemetryClientHolder>.
 */
class TelemetryClientProvider {
  private clients: Map<string, TelemetryClientHolder>;

  constructor(private context: IClientContext) {
    this.clients = new Map();
    const logger = context.getLogger();
    logger.log(LogLevel.debug, 'Created TelemetryClientProvider');
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
    let holder = this.clients.get(host);

    if (!holder) {
      // Create new client for this host
      const client = new TelemetryClient(this.context, host);
      holder = {
        client,
        refCount: 0,
      };
      this.clients.set(host, holder);
      logger.log(LogLevel.debug, `Created new TelemetryClient for host: ${host}`);
    }

    // Increment reference count
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
  async releaseClient(host: string): Promise<void> {
    const logger = this.context.getLogger();
    const holder = this.clients.get(host);

    if (!holder) {
      logger.log(LogLevel.debug, `No TelemetryClient found for host: ${host}`);
      return;
    }

    // Decrement reference count
    holder.refCount -= 1;
    logger.log(LogLevel.debug, `TelemetryClient reference count for ${host}: ${holder.refCount}`);

    // Close and remove client when reference count reaches zero
    if (holder.refCount <= 0) {
      try {
        await holder.client.close();
        this.clients.delete(host);
        logger.log(LogLevel.debug, `Closed and removed TelemetryClient for host: ${host}`);
      } catch (error: any) {
        // Swallow all exceptions per requirement
        logger.log(LogLevel.debug, `Error releasing TelemetryClient: ${error.message}`);
      }
    }
  }

  /**
   * Gets the current reference count for a host's client.
   * Useful for testing and diagnostics.
   *
   * @param host The host identifier
   * @returns The reference count, or 0 if no client exists
   */
  getRefCount(host: string): number {
    const holder = this.clients.get(host);
    return holder ? holder.refCount : 0;
  }

  /**
   * Gets all active clients.
   * Useful for testing and diagnostics.
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
