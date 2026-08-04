import HiveDriverError from '../errors/HiveDriverError';

/**
 * Normalise a PEM input (`string` or `Buffer`) accepted on the public surface
 * into a `Buffer`. Does a light, ordered BEGIN…END sanity check so a
 * truncated/headerless/DER blob (or a stray page that merely contains the
 * literals out of order, e.g. a proxy-intercept page) is rejected here rather
 * than surfacing as an opaque TLS handshake error further down. The bytes are
 * NOT fully parsed in JS — that is deferred to the TLS stack, which returns a
 * meaningful error on a malformed PEM/key.
 *
 * `kind` selects the expected block: `'certificate'` matches a `CERTIFICATE`
 * block; `'private key'` matches any `… PRIVATE KEY` block (PKCS#8 `PRIVATE
 * KEY`, PKCS#1 `RSA PRIVATE KEY`, SEC1 `EC PRIVATE KEY`).
 *
 * `backendLabel` prefixes the error message so the caller (e.g. `DBSQLClient`
 * on the Thrift path, `kernel backend` on the kernel path) is named accurately.
 *
 * Throws `HiveDriverError` when the value is empty or (for strings) lacks the
 * expected PEM header.
 */
export default function normalizePemBytes(
  value: Buffer | string,
  optionName: string,
  kind: 'certificate' | 'private key',
  backendLabel: string,
): Buffer {
  if (typeof value === 'string') {
    const re =
      kind === 'certificate'
        ? /-----BEGIN CERTIFICATE-----[\s\S]+?-----END CERTIFICATE-----/
        : /-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]+?-----END [A-Z0-9 ]*PRIVATE KEY-----/;
    if (!re.test(value)) {
      const expected =
        kind === 'certificate'
          ? "a '-----BEGIN CERTIFICATE-----' … '-----END CERTIFICATE-----' block"
          : "a 'BEGIN … PRIVATE KEY' / 'END … PRIVATE KEY' PEM block (PKCS#8, PKCS#1, or SEC1)";
      throw new HiveDriverError(
        `${backendLabel}: \`${optionName}\` string does not look like a PEM ${kind} (expected ${expected}). ` +
          'Pass PEM text or a Buffer of PEM bytes.',
      );
    }
    return Buffer.from(value, 'utf8');
  }
  if (Buffer.isBuffer(value)) {
    if (value.length === 0) {
      throw new HiveDriverError(`${backendLabel}: \`${optionName}\` Buffer is empty.`);
    }
    return value;
  }
  throw new HiveDriverError(`${backendLabel}: \`${optionName}\` must be a PEM string or a Buffer.`);
}
