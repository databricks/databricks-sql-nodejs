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
import buildTelemetryUrl from './telemetryUtils';
import driverVersion from '../version';

/**
 * Context holding feature flag state for a specific host.
 */
export interface FeatureFlagContext {
  telemetryEnabled?: boolean;
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

  /** In-flight fetch promises for deduplication (prevents thundering herd) */
  private fetchPromises: Map<string, Promise<boolean>> = new Map();

  private readonly CACHE_DURATION_MS = 15 * 60 * 1000; // 15 minutes

  private readonly MIN_CACHE_DURATION_S = 60; // 1 minute minimum TTL

  private readonly MAX_CACHE_DURATION_S = 3600; // 1 hour maximum TTL

  private readonly FEATURE_FLAG_NAME = 'databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForNodeJs';

  constructor(private context: IClientContext) {
    this.contexts = new Map();
  }

  /**
   * Gets or creates a feature flag context for the host.
   * Increments reference count.
   */
  getOrCreateContext(host: string): FeatureFlagContext {
    let ctx = this.contexts.get(host);
    if (!ctx) {
      ctx = {
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
        this.fetchPromises.delete(host); // Invalidate stale in-flight fetch
      }
    }
  }

  /**
   * Checks if telemetry is enabled for the host.
   * Uses cached value if available and not expired.
   */
  async isTelemetryEnabled(host: string): Promise<boolean> {
    const logger = this.context.getLogger();
    const ctx = this.contexts.get(host);

    if (!ctx) {
      return false;
    }

    const isExpired = !ctx.lastFetched || Date.now() - ctx.lastFetched.getTime() > ctx.cacheDuration;

    if (isExpired) {
      // Deduplicate concurrent fetches for the same host (prevents thundering herd)
      if (!this.fetchPromises.has(host)) {
        const fetchPromise = this.fetchFeatureFlag(host)
          .then((enabled) => {
            ctx.telemetryEnabled = enabled;
            ctx.lastFetched = new Date();
            return enabled;
          })
          .catch((error: any) => {
            logger.log(LogLevel.debug, `Error fetching feature flag: ${error.message}`);
            return ctx.telemetryEnabled ?? false;
          })
          .finally(() => {
            this.fetchPromises.delete(host);
          });
        this.fetchPromises.set(host, fetchPromise);
      }

      // Promise is guaranteed to resolve (never rejects) due to .catch() in the chain above
      await this.fetchPromises.get(host);
    }

    return ctx.telemetryEnabled ?? false;
  }

  /**
   * Fetches feature flag from server using connector-service API.
   * Calls GET /api/2.0/connector-service/feature-flags/NODEJS/{version}
   *
   * @param host The host to fetch feature flag for
   * @returns true if feature flag is enabled, false otherwise
   */
  private async fetchFeatureFlag(host: string): Promise<boolean> {
    const logger = this.context.getLogger();

    try {
      // Get driver version for endpoint
      const version = this.getDriverVersion();

      // Build feature flags endpoint for Node.js driver
      const endpoint = buildTelemetryUrl(host, `/api/2.0/connector-service/feature-flags/NODEJS/${version}`);

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
        timeout: 10000, // 10 second timeout to prevent indefinite hangs
      });

      if (!response.ok) {
        // Consume response body to release socket back to connection pool
        await response.text().catch(() => {});
        logger.log(LogLevel.debug, `Feature flag fetch failed: ${response.status} ${response.statusText}`);
        return false;
      }

      // Parse response JSON
      const data: any = await response.json();

      // Response format: { flags: [{ name: string, value: string }], ttl_seconds?: number }
      if (data && data.flags && Array.isArray(data.flags)) {
        // Update cache duration if TTL provided, clamped to safe bounds
        const ctx = this.contexts.get(host);
        if (ctx && typeof data.ttl_seconds === 'number' && data.ttl_seconds > 0) {
          const clampedTtl = Math.max(this.MIN_CACHE_DURATION_S, Math.min(this.MAX_CACHE_DURATION_S, data.ttl_seconds));
          ctx.cacheDuration = clampedTtl * 1000; // Convert to milliseconds
          logger.log(LogLevel.debug, `Updated cache duration to ${clampedTtl} seconds`);
        }

        // Look for our specific feature flag
        const flag = data.flags.find((f: any) => f.name === this.FEATURE_FLAG_NAME);

        if (flag) {
          // Parse boolean value (can be string "true"/"false")
          const value = String(flag.value).toLowerCase();
          const enabled = value === 'true';
          logger.log(LogLevel.debug, `Feature flag ${this.FEATURE_FLAG_NAME}: ${enabled}`);
          return enabled;
        }
      }

      // Feature flag not found in response, default to false
      logger.log(LogLevel.debug, `Feature flag ${this.FEATURE_FLAG_NAME} not found in response`);
      return false;
    } catch (error: any) {
      // Log at debug level only, never propagate exceptions
      logger.log(LogLevel.debug, `Error fetching feature flag from ${host}: ${error.message}`);
      return false;
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
