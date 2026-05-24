/**
 * Internal, non-exported extension of `ConnectionOptions`. Carries M0-only
 * flags that should not appear in the published `.d.ts`.
 *
 * Matches the Python connector pattern: there, `use_sea` is consumed via
 * `kwargs.get("use_sea", False)` and is intentionally absent from the typed
 * signature (see `databricks-sql-python/src/databricks/sql/session.py`).
 *
 * Callers cast `ConnectionOptions` to this type *only* at the read site
 * inside the driver; user code that wants to set `useSEA` may still do so
 * via an untyped object literal — the option is not part of the public
 * contract and may be removed without notice.
 */
export interface InternalConnectionOptions {
  /**
   * Opt-in flag to dispatch through the Statement Execution API (SEA)
   * backend instead of the default Thrift backend. Defaults to `false`.
   * @internal Not stable; M0 stub only.
   */
  useSEA?: boolean;
}
