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
   * the insecure accept-anything mode. This is the master verify toggle:
   * `false` also subsumes the hostname check (see
   * `checkServerCertificateHostname`). Mirrors the Python connector's
   * `_tls_no_verify` (inverted).
   * @internal SEA path only.
   */
  checkServerCertificate?: boolean;

  /**
   * SEA-only: verify that the server certificate matches the host
   * (hostname-vs-SNI check), independently of full chain validation. Omit
   * to keep the secure default (on); set `false` to skip only the hostname
   * check while still validating the chain — e.g. connecting via an IP
   * literal or a host the cert wasn't issued for. No-op when
   * `checkServerCertificate` is `false` (that disables everything). Mirrors
   * the Python connector's `_tls_verify_hostname`.
   * @internal SEA path only.
   */
  checkServerCertificateHostname?: boolean;

  /**
   * SEA-only: PEM-encoded CA certificate (string or `Buffer`) added to the
   * trust store on top of the system roots — for TLS-inspecting proxies or
   * on-prem internal CAs. Honoured regardless of `checkServerCertificate`.
   * @internal SEA path only.
   */
  customCaCert?: Buffer | string;

  /**
   * SEA-only: PEM-encoded client certificate (string or `Buffer`) for
   * mutual TLS (mTLS). Must be supplied together with `clientKeyPem`; a
   * leaf cert optionally followed by its intermediate chain is accepted.
   * Mirrors the Python connector's `_tls_client_cert_file`.
   * @internal SEA path only.
   */
  clientCertPem?: Buffer | string;

  /**
   * SEA-only: PEM-encoded private key (string or `Buffer`) for the mTLS
   * client certificate. Must be supplied together with `clientCertPem`.
   * For portability supply a PKCS#8 key (`BEGIN PRIVATE KEY`). Mirrors the
   * Python connector's `_tls_client_cert_key_file`.
   * @internal SEA path only.
   */
  clientKeyPem?: Buffer | string;
}
