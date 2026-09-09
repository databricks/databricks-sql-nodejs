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
  isReyden: boolean;
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
   * Returns undefined if the warehouse is not in the cache or the entry has expired.
   */
  public isKnownReyden(host: string, warehouseId: string): boolean | undefined {
    const key = this.getKey(host, warehouseId);
    const entry = this.cache.get(key);

    if (!entry) {
      return undefined;
    }

    // Opportunistically evict expired entries on access
    if (this.isExpired(entry)) {
      this.cache.delete(key);
      return undefined;
    }

    return entry.isReyden;
  }

  /**
   * Mark a warehouse as being Reyden (KP001 rejection detected).
   */
  public markReyden(host: string, warehouseId: string): void {
    const key = this.getKey(host, warehouseId);
    this.cache.set(key, {
      timestamp: Date.now(),
      isReyden: true,
    });
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
