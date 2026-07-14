/**
 * Internal, non-exported extension of `ConnectionOptions`. Carries M0-only
 * flags that should not appear in the published `.d.ts`.
 *
 * Matches the Python connector pattern: there, `use_kernel` is consumed via
 * `kwargs.get("use_kernel", False)` and is intentionally absent from the typed
 * signature (see `databricks-sql-python/src/databricks/sql/session.py`).
 *
 * Callers cast `ConnectionOptions` to this type *only* at the read site
 * inside the driver; user code that wants to set `useKernel` may still do so
 * via an untyped object literal — the option is not part of the public
 * contract and may be removed without notice.
 */
export interface InternalConnectionOptions {
  /**
   * Opt-in flag to dispatch through the Statement Execution API (SEA)
   * backend instead of the default Thrift backend. Defaults to `false`.
   * @internal Not stable; M0 stub only.
   */
  useKernel?: boolean;

  /**
   * kernel-only: kernel connection-pool size (`ConnectionOptions.max_connections`).
   * Validated as a positive integer within the napi `u32` range.
   * @internal kernel path only.
   */
  maxConnections?: number;

  /**
   * kernel-only: verify the server's TLS certificate. Secure-by-default — omit
   * to keep full chain + hostname verification; set `false` only to opt into
   * the insecure accept-anything mode. This is the master verify toggle:
   * `false` also subsumes the hostname check (see
   * `checkServerCertificateHostname`). Mirrors the Python connector's
   * `_tls_no_verify` (inverted).
   * @internal kernel path only.
   */
  checkServerCertificate?: boolean;

  /**
   * kernel-only: verify that the server certificate matches the host
   * (hostname-vs-SNI check), independently of full chain validation. Omit
   * to keep the secure default (on); set `false` to skip only the hostname
   * check while still validating the chain — e.g. connecting via an IP
   * literal or a host the cert wasn't issued for. No-op when
   * `checkServerCertificate` is `false` (that disables everything). Mirrors
   * the Python connector's `_tls_verify_hostname`.
   * @internal kernel path only.
   */
  checkServerCertificateHostname?: boolean;

  /**
   * kernel-only: PEM-encoded CA certificate (string or `Buffer`) added to the
   * trust store on top of the system roots — for TLS-inspecting proxies or
   * on-prem internal CAs. Honoured regardless of `checkServerCertificate`.
   * @internal kernel path only.
   */
  customCaCert?: Buffer | string;

  /**
   * kernel-only: PEM-encoded client certificate (string or `Buffer`) for
   * mutual TLS (mTLS). Must be supplied together with `clientKeyPem`; a
   * leaf cert optionally followed by its intermediate chain is accepted.
   * Mirrors the Python connector's `_tls_client_cert_file`.
   * @internal kernel path only.
   */
  clientCertPem?: Buffer | string;

  /**
   * kernel-only: PEM-encoded private key (string or `Buffer`) for the mTLS
   * client certificate. Must be supplied together with `clientCertPem`.
   * For portability supply a PKCS#8 key (`BEGIN PRIVATE KEY`). Mirrors the
   * Python connector's `_tls_client_cert_key_file`.
   * @internal kernel path only.
   */
  clientKeyPem?: Buffer | string;
}
