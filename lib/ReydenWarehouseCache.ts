/**
 * Process-wide cache for tracking Reyden (Real-Time SQL) warehouses.
 *
 * When a Thrift OpenSession fails with SQLSTATE KP001, the driver falls back
 * to the SEA (Statement Execution API) backend. This cache avoids retrying
 * the same failed Thrift path on subsequent connections by recording which
 * warehouses are known to require SEA.
 *
 * The cache is keyed by (host_lowercased, warehouse_id) to handle multi-tenant
 * safety — the same warehouse ID on different hosts may have different support.
 *
 * TTL is ~6 hours to allow the server side to update warehouse routing without
 * requiring a process restart. Expired entries are opportunistically evicted on
 * access (no background GC thread — Node is single-threaded).
 */

const TTL_MS = 6 * 60 * 60 * 1000; // 6 hours

interface CacheEntry {
  timestamp: number;
}

class ReydenWarehouseCache {
  private static instance?: ReydenWarehouseCache;

  private cache: Map<string, CacheEntry> = new Map();

  // Singleton: constructor is private to enforce getInstance() usage
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  private constructor() {}

  public static getInstance(): ReydenWarehouseCache {
    if (!ReydenWarehouseCache.instance) {
      ReydenWarehouseCache.instance = new ReydenWarehouseCache();
    }
    return ReydenWarehouseCache.instance;
  }

  /**
   * Constructs a cache key from host and warehouse ID.
   * Host is lowercased for case-insensitive comparison.
   */
  private getKey(host: string, warehouseId: string): string {
    return `${host.toLowerCase()}:${warehouseId}`;
  }

  /**
   * Check if an entry is expired based on TTL.
   */
  private isExpired(entry: CacheEntry): boolean {
    return Date.now() - entry.timestamp > TTL_MS;
  }

  /**
   * Checks if a warehouse is known to be Reyden (requiring SEA fallback).
   *
   * Membership is presence-based: the cache only ever records known-Reyden
   * warehouses (via markReyden), so an unexpired entry means Reyden and the
   * absence of one means "not known" — there is no negative-cache state.
   * Returns false when the warehouse is not in the cache or the entry expired.
   */
  public isKnownReyden(host: string, warehouseId: string): boolean {
    const key = this.getKey(host, warehouseId);
    const entry = this.cache.get(key);

    if (!entry) {
      return false;
    }

    // Opportunistically evict expired entries on access
    if (this.isExpired(entry)) {
      this.cache.delete(key);
      return false;
    }

    return true;
  }

  /**
   * Mark a warehouse as being Reyden (KP001 rejection detected).
   */
  public markReyden(host: string, warehouseId: string): void {
    const now = Date.now();

    // Opportunistic sweep: markReyden runs only on an actual Thrift rejection
    // (rare), so purging every expired entry here is near-free and bounds the
    // cache to warehouses seen within the TTL window. The per-key lazy eviction
    // in isKnownReyden only reclaims entries that are looked up again, so an
    // entry that is never queried after marking would otherwise persist for the
    // life of the process.
    for (const [existingKey, entry] of this.cache) {
      if (now - entry.timestamp > TTL_MS) {
        this.cache.delete(existingKey);
      }
    }

    this.cache.set(this.getKey(host, warehouseId), { timestamp: now });
  }

  /**
   * Clears the cache. Intended for testing only.
   *
   * @internal
   */
  public clear(): void {
    this.cache.clear();
  }

  /**
   * Returns the current cache size. Intended for testing/observability.
   *
   * @internal
   */
  public size(): number {
    return this.cache.size;
  }
}

export default ReydenWarehouseCache.getInstance();
