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

import fetch from 'node-fetch';
import IClientContext from '../contracts/IClientContext';
import { LogLevel } from '../contracts/IDBSQLLogger';
import driverVersion from '../version';
import { buildUrl } from './urlUtils';
import { CircuitBreaker, CircuitBreakerRegistry } from './CircuitBreaker';

/**
 * Context holding feature flag state for a specific host.
 * Stores all feature flags from the server for extensibility.
 */
export interface FeatureFlagContext {
  flags: Map<string, string>; // All feature flags from server (extensible for future flags)
  lastFetched?: Date;
  refCount: number;
  cacheDuration: number; // 15 minutes in ms
}

/**
 * Manages feature flag cache per host.
 * Prevents rate limiting by caching feature flag responses.
 * Instance-based, stored in DBSQLClient.
 */
export default class FeatureFlagCache {
  private contexts: Map<string, FeatureFlagContext>;

  private readonly CACHE_DURATION_MS = 15 * 60 * 1000; // 15 minutes

  private readonly FEATURE_FLAG_NAME = 'databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForNodeJs';

  private circuitBreakerRegistry: CircuitBreakerRegistry;

  constructor(
    private context: IClientContext,
    circuitBreakerRegistry?: CircuitBreakerRegistry,
  ) {
    this.contexts = new Map();
    this.circuitBreakerRegistry = circuitBreakerRegistry || new CircuitBreakerRegistry(context);
  }

  /**
   * Gets or creates a feature flag context for the host.
   * Increments reference count.
   */
  getOrCreateContext(host: string): FeatureFlagContext {
    let ctx = this.contexts.get(host);
    if (!ctx) {
      ctx = {
        flags: new Map<string, string>(),
        refCount: 0,
        cacheDuration: this.CACHE_DURATION_MS,
      };
      this.contexts.set(host, ctx);
    }
    ctx.refCount += 1;
    return ctx;
  }

  /**
   * Decrements reference count for the host.
   * Removes context when ref count reaches zero.
   */
  releaseContext(host: string): void {
    const ctx = this.contexts.get(host);
    if (ctx) {
      ctx.refCount -= 1;
      if (ctx.refCount <= 0) {
        this.contexts.delete(host);
      }
    }
  }

  /**
   * Generic method to check if a feature flag is enabled.
   * Uses cached value if available and not expired.
   *
   * @param host The host to check
   * @param flagName The feature flag name to query
   * @returns true if flag is enabled (value is "true"), false otherwise
   */
  async isFeatureEnabled(host: string, flagName: string): Promise<boolean> {
    const logger = this.context.getLogger();
    const ctx = this.contexts.get(host);

    if (!ctx) {
      return false;
    }

    const isExpired = !ctx.lastFetched || Date.now() - ctx.lastFetched.getTime() > ctx.cacheDuration;

    if (isExpired) {
      try {
        // Fetch all feature flags from server with circuit breaker protection
        const circuitBreaker = this.circuitBreakerRegistry.getCircuitBreaker(host);
        await circuitBreaker.execute(async () => {
          await this.fetchFeatureFlags(host);
        });
        ctx.lastFetched = new Date();
      } catch (error: any) {
        // Log at debug level only, never propagate exceptions
        // Circuit breaker OPEN or fetch failed - use cached values
        if (error.message === 'Circuit breaker OPEN') {
          logger.log(LogLevel.debug, 'Feature flags: Circuit breaker OPEN - using cached values');
        } else {
          logger.log(LogLevel.debug, `Error fetching feature flags: ${error.message}`);
        }
      }
    }

    // Get flag value and parse as boolean
    const value = ctx.flags.get(flagName);
    return value?.toLowerCase() === 'true';
  }

  /**
   * Convenience method to check if telemetry is enabled for the host.
   * Uses cached value if available and not expired.
   */
  async isTelemetryEnabled(host: string): Promise<boolean> {
    return this.isFeatureEnabled(host, this.FEATURE_FLAG_NAME);
  }

  /**
   * Fetches all feature flags from server using connector-service API.
   * Calls GET /api/2.0/connector-service/feature-flags/NODEJS/{version}
   * Stores all flags in the context for extensibility.
   *
   * @param host The host to fetch feature flags for
   */
  private async fetchFeatureFlags(host: string): Promise<void> {
    const logger = this.context.getLogger();

    try {
      // Get driver version for endpoint
      const version = this.getDriverVersion();

      // Build feature flags endpoint for Node.js driver
      const endpoint = buildUrl(host, `/api/2.0/connector-service/feature-flags/NODEJS/${version}`);

      // Get authentication headers
      const authHeaders = await this.context.getAuthHeaders();

      logger.log(LogLevel.debug, `Fetching feature flags from ${endpoint}`);

      // Get agent with proxy settings (same pattern as CloudFetchResultHandler and DBSQLSession)
      const connectionProvider = await this.context.getConnectionProvider();
      const agent = await connectionProvider.getAgent();

      // Make HTTP GET request with authentication and proxy support
      const response = await fetch(endpoint, {
        method: 'GET',
        headers: {
          ...authHeaders,
          'Content-Type': 'application/json',
          'User-Agent': `databricks-sql-nodejs/${driverVersion}`,
        },
        agent, // Include agent for proxy support
      });

      if (!response.ok) {
        logger.log(LogLevel.debug, `Feature flags fetch failed: ${response.status} ${response.statusText}`);
        return;
      }

      // Parse response JSON
      const data: any = await response.json();

      // Response format: { flags: [{ name: string, value: string }], ttl_seconds?: number }
      if (data && data.flags && Array.isArray(data.flags)) {
        const ctx = this.contexts.get(host);
        if (!ctx) {
          return;
        }

        // Clear existing flags and store all flags from response
        ctx.flags.clear();
        for (const flag of data.flags) {
          if (flag.name && flag.value !== undefined) {
            ctx.flags.set(flag.name, String(flag.value));
          }
        }

        logger.log(LogLevel.debug, `Stored ${ctx.flags.size} feature flags from server`);

        // Update cache duration if TTL provided
        if (data.ttl_seconds) {
          ctx.cacheDuration = data.ttl_seconds * 1000; // Convert to milliseconds
          logger.log(LogLevel.debug, `Updated cache duration to ${data.ttl_seconds} seconds`);
        }
      }
    } catch (error: any) {
      // Log at debug level only, never propagate exceptions
      logger.log(LogLevel.debug, `Error fetching feature flags from ${host}: ${error.message}`);
    }
  }

  /**
   * Gets the driver version without -oss suffix for API calls.
   * Format: "1.12.0" from "1.12.0-oss"
   */
  private getDriverVersion(): string {
    // Remove -oss suffix if present
    return driverVersion.replace(/-oss$/, '');
  }
}
