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

  /**
   * SEA-only: kernel connection-pool size (`ConnectionOptions.max_connections`).
   * Validated as a positive integer within the napi `u32` range.
   * @internal SEA path only.
   */
  maxConnections?: number;

  /**
   * SEA-only: verify the server's TLS certificate. Secure-by-default — omit
   * to keep full chain + hostname verification; set `false` only to opt into
   * the insecure accept-anything mode.
   * @internal SEA path only.
   */
  checkServerCertificate?: boolean;

  /**
   * SEA-only: PEM-encoded CA certificate (string or `Buffer`) added to the
   * trust store on top of the system roots — for TLS-inspecting proxies or
   * on-prem internal CAs. Honoured regardless of `checkServerCertificate`.
   * @internal SEA path only.
   */
  customCaCert?: Buffer | string;
}
