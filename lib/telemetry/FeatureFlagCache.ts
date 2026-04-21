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

import fetch, { RequestInit, Response } from 'node-fetch';
import IClientContext from '../contracts/IClientContext';
import { LogLevel } from '../contracts/IDBSQLLogger';
import IAuthentication from '../connection/contracts/IAuthentication';
import { buildTelemetryUrl, normalizeHeaders } from './telemetryUtils';
import ExceptionClassifier from './ExceptionClassifier';
import buildUserAgentString from '../utils/buildUserAgentString';
import driverVersion from '../version';

export interface FeatureFlagContext {
  telemetryEnabled?: boolean;
  lastFetched?: Date;
  refCount: number;
  cacheDuration: number;
}

/**
 * Per-host feature-flag cache used to gate telemetry emission. Responsibilities:
 *   - dedupe in-flight fetches (thundering-herd protection);
 *   - ref-count so context goes away when the last consumer closes;
 *   - clamp server-provided TTL into a safe band.
 *
 * Shares HTTP plumbing (agent, user agent) with DatabricksTelemetryExporter.
 * Consumer wiring lands in a later PR in this stack (see PR description).
 */
export default class FeatureFlagCache {
  private contexts: Map<string, FeatureFlagContext>;

  private fetchPromises: Map<string, Promise<boolean>> = new Map();

  private readonly userAgent: string;

  private readonly CACHE_DURATION_MS = 15 * 60 * 1000;

  private readonly MIN_CACHE_DURATION_S = 60;

  private readonly MAX_CACHE_DURATION_S = 3600;

  private readonly FEATURE_FLAG_NAME = 'databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForNodeJs';

  constructor(private context: IClientContext, private authProvider?: IAuthentication) {
    this.contexts = new Map();
    this.userAgent = buildUserAgentString(this.context.getConfig().userAgentEntry);
  }

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

  releaseContext(host: string): void {
    const ctx = this.contexts.get(host);
    if (ctx) {
      ctx.refCount -= 1;
      if (ctx.refCount <= 0) {
        this.contexts.delete(host);
        this.fetchPromises.delete(host);
      }
    }
  }

  async isTelemetryEnabled(host: string): Promise<boolean> {
    const logger = this.context.getLogger();
    const ctx = this.contexts.get(host);

    if (!ctx) {
      return false;
    }

    const isExpired = !ctx.lastFetched || Date.now() - ctx.lastFetched.getTime() > ctx.cacheDuration;

    if (isExpired) {
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

      await this.fetchPromises.get(host);
    }

    return ctx.telemetryEnabled ?? false;
  }

  /**
   * Strips the `-oss` suffix the feature-flag API does not accept. The server
   * keys off the SemVer triplet only, so anything appended would 404.
   */
  private getDriverVersion(): string {
    return driverVersion.replace(/-oss$/, '');
  }

  private async fetchFeatureFlag(host: string): Promise<boolean> {
    const logger = this.context.getLogger();

    try {
      const endpoint = buildTelemetryUrl(
        host,
        `/api/2.0/connector-service/feature-flags/NODEJS/${this.getDriverVersion()}`,
      );
      if (!endpoint) {
        logger.log(LogLevel.debug, `Feature flag fetch skipped: invalid host ${host}`);
        return false;
      }

      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        'User-Agent': this.userAgent,
        ...(await this.getAuthHeaders()),
      };

      logger.log(LogLevel.debug, `Fetching feature flags from ${endpoint}`);

      const response = await this.fetchWithRetry(endpoint, {
        method: 'GET',
        headers,
        timeout: 10000,
      });

      if (!response.ok) {
        await response.text().catch(() => {});
        logger.log(LogLevel.debug, `Feature flag fetch failed: ${response.status} ${response.statusText}`);
        return false;
      }

      const data: any = await response.json();

      if (data && data.flags && Array.isArray(data.flags)) {
        const ctx = this.contexts.get(host);
        if (ctx && typeof data.ttl_seconds === 'number' && data.ttl_seconds > 0) {
          const clampedTtl = Math.max(this.MIN_CACHE_DURATION_S, Math.min(this.MAX_CACHE_DURATION_S, data.ttl_seconds));
          ctx.cacheDuration = clampedTtl * 1000;
          logger.log(LogLevel.debug, `Updated cache duration to ${clampedTtl} seconds`);
        }

        const flag = data.flags.find((f: any) => f.name === this.FEATURE_FLAG_NAME);
        if (flag) {
          const enabled = String(flag.value).toLowerCase() === 'true';
          logger.log(LogLevel.debug, `Feature flag ${this.FEATURE_FLAG_NAME}: ${enabled}`);
          return enabled;
        }
      }

      logger.log(LogLevel.debug, `Feature flag ${this.FEATURE_FLAG_NAME} not found in response`);
      return false;
    } catch (error: any) {
      logger.log(LogLevel.debug, `Error fetching feature flag from ${host}: ${error.message}`);
      return false;
    }
  }

  /**
   * Retries transient network errors once before giving up. Without a retry
   * a single hiccup would leave telemetry disabled for the full cache TTL
   * (15 min). One retry gives an ephemeral DNS / connection-reset failure
   * a second chance without pushing sustained load at a broken endpoint.
   */
  private async fetchWithRetry(url: string, init: RequestInit): Promise<Response> {
    const connectionProvider = await this.context.getConnectionProvider();
    const agent = await connectionProvider.getAgent();
    const logger = this.context.getLogger();

    try {
      return await fetch(url, { ...init, agent });
    } catch (err: any) {
      if (!ExceptionClassifier.isRetryable(err)) {
        throw err;
      }
      logger.log(LogLevel.debug, `Feature flag fetch retry after transient: ${err?.code ?? err?.message ?? err}`);
      await new Promise((resolve) => {
        setTimeout(resolve, 100 + Math.random() * 100);
      });
      return fetch(url, { ...init, agent });
    }
  }

  private async getAuthHeaders(): Promise<Record<string, string>> {
    if (!this.authProvider) {
      return {};
    }
    try {
      return normalizeHeaders(await this.authProvider.authenticate());
    } catch (error: any) {
      this.context.getLogger().log(LogLevel.debug, `Feature flag auth failed: ${error?.message ?? error}`);
      return {};
    }
  }
}
