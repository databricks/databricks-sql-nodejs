/**
 * Decodes a JWT token without verifying the signature.
 * This is safe because the server will validate the token anyway.
 *
 * @param token - The JWT token string
 * @returns The decoded payload as a record, or null if decoding fails
 */
export function decodeJWT(token: string): Record<string, unknown> | null {
  try {
    const parts = token.split('.');
    if (parts.length < 2) {
      return null;
    }
    const payload = Buffer.from(parts[1], 'base64').toString('utf8');
    return JSON.parse(payload);
  } catch {
    return null;
  }
}

/**
 * Extracts the issuer from a JWT token.
 *
 * @param token - The JWT token string
 * @returns The issuer string, or null if not found
 */
export function getJWTIssuer(token: string): string | null {
  const payload = decodeJWT(token);
  if (!payload || typeof payload.iss !== 'string') {
    return null;
  }
  return payload.iss;
}

/**
 * Compares two host URLs, ignoring ports.
 * Treats "example.com" and "example.com:443" as equivalent.
 *
 * @param url1 - First URL or hostname
 * @param url2 - Second URL or hostname
 * @returns true if the hosts are the same
 */
export function isSameHost(url1: string, url2: string): boolean {
  try {
    const host1 = extractHostname(url1);
    const host2 = extractHostname(url2);
    return host1.toLowerCase() === host2.toLowerCase();
  } catch {
    return false;
  }
}

/**
 * Extracts the hostname from a URL or hostname string.
 * Handles both full URLs and bare hostnames.
 *
 * @param urlOrHostname - A URL or hostname string
 * @returns The extracted hostname
 */
function extractHostname(urlOrHostname: string): string {
  // If it looks like a URL, parse it
  if (urlOrHostname.includes('://')) {
    const url = new URL(urlOrHostname);
    return url.hostname;
  }

  // Handle hostname with port (e.g., "example.com:443")
  const colonIndex = urlOrHostname.indexOf(':');
  if (colonIndex !== -1) {
    return urlOrHostname.substring(0, colonIndex);
  }

  // Bare hostname
  return urlOrHostname;
}
