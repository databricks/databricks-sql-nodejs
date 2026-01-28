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
  private readonly CACHE_DURATION_MS = 15 * 60 * 1000; // 15 minutes
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
    ctx.refCount++;
    return ctx;
  }

  /**
   * Decrements reference count for the host.
   * Removes context when ref count reaches zero.
   */
  releaseContext(host: string): void {
    const ctx = this.contexts.get(host);
    if (ctx) {
      ctx.refCount--;
      if (ctx.refCount <= 0) {
        this.contexts.delete(host);
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

    const isExpired = !ctx.lastFetched ||
      (Date.now() - ctx.lastFetched.getTime() > ctx.cacheDuration);

    if (isExpired) {
      try {
        // Fetch feature flag from server
        ctx.telemetryEnabled = await this.fetchFeatureFlag(host);
        ctx.lastFetched = new Date();
      } catch (error: any) {
        // Log at debug level only, never propagate exceptions
        logger.log(LogLevel.debug, `Error fetching feature flag: ${error.message}`);
      }
    }

    return ctx.telemetryEnabled ?? false;
  }

  /**
   * Fetches feature flag from server.
   * This is a placeholder implementation that returns false.
   * Real implementation would fetch from server using connection provider.
   */
  private async fetchFeatureFlag(host: string): Promise<boolean> {
    // Placeholder implementation
    // Real implementation would use:
    // const connectionProvider = await this.context.getConnectionProvider();
    // and make an API call to fetch the feature flag
    return false;
  }
}
