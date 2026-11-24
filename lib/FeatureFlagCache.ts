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

import fetch, { RequestInit, Response, Request } from 'node-fetch';
import IClientContext from './contracts/IClientContext';
import { LogLevel } from './contracts/IDBSQLLogger';
import IAuthentication from './connection/contracts/IAuthentication';
import { buildTelemetryUrl, normalizeHeaders } from './telemetry/telemetryUtils';
import buildUserAgentString from './utils/buildUserAgentString';
import driverVersion from './version';

export interface FeatureFlagContext {
  flags?: Map<string, string>;
  fetchPromise?: Promise<void>;
  lastFetched?: number;
  cacheable?: boolean;
  refCount: number;
  cacheDuration: number;
}

/**
 * Shared feature-flag values per workspace.
 * Acquire/release a context per consumer; each reader uses its caller's auth.
 * Responsibilities:
 *   - dedupe in-flight fetches (thundering-herd protection);
 *   - retain bounded successful responses until their original expiry;
 *   - clamp server-provided TTL into a safe band.
 *
 * Workspace ID partitions SPOG traffic; normalized host is the fallback.
 */
export default class FeatureFlagCache {
  private static sharedContexts = new Map<string, FeatureFlagContext>();

  private static idleContexts = new Map<string, FeatureFlagContext>();

  private static readonly MAX_IDLE_CONTEXTS = 100;

  private contexts = FeatureFlagCache.sharedContexts;

  private readonly userAgent: string;

  private readonly CACHE_DURATION_MS = 15 * 60 * 1000;

  private readonly MIN_CACHE_DURATION_S = 60;

  private readonly MAX_CACHE_DURATION_S = 3600;

  constructor(private context: IClientContext, private authProvider?: IAuthentication) {
    this.userAgent = buildUserAgentString(this.context.getConfig().userAgentEntry);
  }

  private cacheKey(host: string): string {
    const headers = this.context.getConfig().customHeaders ?? {};
    const workspace = Object.entries(headers).find(([name]) => name.toLowerCase() === 'x-databricks-org-id')?.[1];
    return workspace ? `workspace:${workspace}` : `host:${buildTelemetryUrl(host, '') ?? host}`;
  }

  private isExpired(ctx: FeatureFlagContext): boolean {
    return ctx.lastFetched === undefined || performance.now() - ctx.lastFetched > ctx.cacheDuration;
  }

  getOrCreateContext(host: string): FeatureFlagContext {
    const key = this.cacheKey(host);
    let ctx = this.contexts.get(key);
    if (!ctx) {
      const idle = FeatureFlagCache.idleContexts.get(key);
      FeatureFlagCache.idleContexts.delete(key);
      ctx =
        idle && !this.isExpired(idle)
          ? idle
          : {
              refCount: 0,
              cacheDuration: this.CACHE_DURATION_MS,
            };
      this.contexts.set(key, ctx);
    }
    ctx.refCount += 1;
    return ctx;
  }

  releaseContext(host: string): void {
    const key = this.cacheKey(host);
    const ctx = this.contexts.get(key);
    if (ctx) {
      ctx.refCount -= 1;
      if (ctx.refCount <= 0) {
        this.contexts.delete(key);
        if (ctx.cacheable && !ctx.fetchPromise && !this.isExpired(ctx)) {
          const idle = FeatureFlagCache.idleContexts;
          for (const [idleKey, value] of idle) {
            if (this.isExpired(value)) idle.delete(idleKey);
          }
          idle.delete(key);
          idle.set(key, ctx);
          while (idle.size > FeatureFlagCache.MAX_IDLE_CONTEXTS) {
            idle.delete(idle.keys().next().value!);
          }
        }
      }
    }
  }

  private async getRawValue(host: string, name: string): Promise<string | undefined> {
    const logger = this.context.getLogger();
    const ctx = this.contexts.get(this.cacheKey(host));

    if (!ctx) {
      return undefined;
    }

    const isExpired = this.isExpired(ctx);

    if (ctx.fetchPromise || isExpired) {
      if (!ctx.fetchPromise) {
        ctx.fetchPromise = this.fetchFeatureFlags(host)
          .then((flags) => {
            ctx.flags = flags;
            ctx.lastFetched = performance.now();
          })
          .catch((error: any) => {
            logger.log(LogLevel.debug, `Error fetching feature flag: ${error.message}`);
          })
          .finally(() => {
            ctx.fetchPromise = undefined;
          });
      }

      await ctx.fetchPromise;
    }

    return ctx.flags?.get(name);
  }

  private async getValue(host: string, name: string): Promise<unknown> {
    const raw = await this.getRawValue(host, name);
    try {
      return raw === undefined ? undefined : JSON.parse(raw);
    } catch {
      return undefined;
    }
  }

  async getBoolean(host: string, name: string, defaultValue = false): Promise<boolean> {
    // Accept legacy mixed-case boolean literals as well as canonical JSON.
    const value = (await this.getRawValue(host, name))?.trim().toLowerCase();
    if (value === 'true') return true;
    if (value === 'false') return false;
    return defaultValue;
  }

  async getInt32(host: string, name: string, defaultValue?: number): Promise<number | undefined> {
    const value = await this.getInt64(host, name);
    return value !== undefined && BigInt.asIntN(32, value) === value ? Number(value) : defaultValue;
  }

  async getInt64(host: string, name: string, defaultValue?: bigint): Promise<bigint | undefined> {
    const raw = (await this.getRawValue(host, name))?.trim();
    // Parse integer text directly: JSON.parse would round values beyond 2^53.
    if (!raw || !/^-?(0|[1-9]\d*)$/.test(raw)) return defaultValue;
    const value = BigInt(raw);
    return BigInt.asIntN(64, value) === value ? value : defaultValue;
  }

  async getDouble(host: string, name: string, defaultValue?: number): Promise<number | undefined> {
    const value = await this.getValue(host, name);
    return typeof value === 'number' && Number.isFinite(value) ? value : defaultValue;
  }

  async getString(host: string, name: string, defaultValue?: string): Promise<string | undefined> {
    const value = await this.getValue(host, name);
    return typeof value === 'string' ? value : defaultValue;
  }

  async getStringList(host: string, name: string, defaultValue?: string[]): Promise<string[] | undefined> {
    const value = await this.getValue(host, name);
    return Array.isArray(value) && value.every((item) => typeof item === 'string') ? value : defaultValue;
  }

  /**
   * Strips the `-oss` suffix the feature-flag API does not accept. The server
   * keys off the SemVer triplet only, so anything appended would 404.
   */
  private getDriverVersion(): string {
    return driverVersion.replace(/-oss$/, '');
  }

  private async fetchFeatureFlags(host: string): Promise<Map<string, string>> {
    const logger = this.context.getLogger();
    const ctx = this.contexts.get(this.cacheKey(host));
    const previousCacheDuration = ctx?.cacheDuration ?? this.CACHE_DURATION_MS;
    if (ctx) {
      ctx.cacheable = false;
      ctx.cacheDuration = this.CACHE_DURATION_MS;
    }

    try {
      const endpoint = buildTelemetryUrl(
        host,
        `/api/2.0/connector-service/feature-flags/NODEJS/${this.getDriverVersion()}`,
      );
      if (!endpoint) {
        logger.log(LogLevel.debug, `Feature flag fetch skipped: invalid host ${host}`);
        return new Map();
      }

      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        'User-Agent': this.userAgent,
        ...(this.context.getConfig().customHeaders ?? {}),
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
        throw new Error(`Feature flag fetch failed: ${response.status} ${response.statusText}`);
      }

      const data: any = await response.json();

      if (data && data.flags && Array.isArray(data.flags)) {
        if (ctx) {
          ctx.cacheable = data.flags.every(
            (flag: any) => typeof flag?.name === 'string' && typeof flag.value === 'string',
          );
        }
        if (ctx && Number.isFinite(data.ttl_seconds) && data.ttl_seconds > 0) {
          const clampedTtl = Math.max(this.MIN_CACHE_DURATION_S, Math.min(this.MAX_CACHE_DURATION_S, data.ttl_seconds));
          ctx.cacheDuration = clampedTtl * 1000;
          logger.log(LogLevel.debug, `Updated cache duration to ${clampedTtl} seconds`);
        }

        return new Map(
          data.flags
            .filter((flag: any) => typeof flag?.name === 'string' && typeof flag.value === 'string')
            .map((flag: any) => [flag.name, flag.value]),
        );
      }

      return new Map();
    } catch (error: any) {
      logger.log(LogLevel.debug, `Error fetching feature flag from ${host}: ${error.message}`);
      // Preserve the existing policy: use consumer defaults until the next TTL.
      // Retrying on every read would add request latency during an outage.
      return new Map();
    } finally {
      if (ctx && !ctx.cacheable) {
        ctx.cacheDuration = Math.min(previousCacheDuration, this.CACHE_DURATION_MS);
      }
    }
  }

  /** Retries transient failures using the connection's retry policy. */
  private async fetchWithRetry(url: string, init: RequestInit): Promise<Response> {
    const connectionProvider = await this.context.getConnectionProvider();
    const agent = await connectionProvider.getAgent(url);
    const retryPolicy = await connectionProvider.getRetryPolicy();
    const requestConfig: RequestInit = { agent, ...init };
    const result = await retryPolicy.invokeWithRetry(() => {
      const request = new Request(url, requestConfig);
      return fetch(request).then((response) => ({ request, response }));
    });
    return result.response;
  }

  private async getAuthHeaders(): Promise<Record<string, string>> {
    // Resolve auth from this caller, never from the shared cache state.
    const authProvider = this.authProvider ?? this.context.getAuthProvider?.();
    if (!authProvider) {
      return {};
    }
    try {
      return normalizeHeaders(await authProvider.authenticate());
    } catch (error: any) {
      this.context.getLogger().log(LogLevel.debug, `Feature flag auth failed: ${error?.message ?? error}`);
      return {};
    }
  }
}
