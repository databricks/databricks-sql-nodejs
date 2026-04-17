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

/**
 * Hosts we always refuse to send authenticated telemetry to. Targeted at the
 * `/api/2.0/sql/telemetry-ext` exfil vector: an attacker-influenced `host`
 * (env var, tampered config, etc.) must not be able to redirect the Bearer
 * token to a loopback/IMDS/RFC1918 endpoint.
 */
const BLOCKED_HOST_PATTERNS: RegExp[] = [
  /^(?:127\.|0\.|10\.|169\.254\.|172\.(?:1[6-9]|2[0-9]|3[01])\.|192\.168\.)/,
  /^(?:localhost|metadata\.google\.internal|metadata\.azure\.com)$/i,
  /^\[?::1\]?$/,
  /^\[?(?:fc|fd)[0-9a-f]{2}:/i,
  /^\[?::ffff:(?:127|10|0|169\.254)\./i,
];

/**
 * Build an HTTPS telemetry URL from a host and a path.
 *
 * Refuses anything beyond a bare `host[:port]` so a compromised or mistyped
 * host cannot redirect the authenticated request to an attacker-controlled
 * endpoint. Defeated historical bypasses include:
 *   - protocol-relative prefix: `//attacker.com`
 *   - zero-width / ASCII whitespace in the host
 *   - userinfo (`user:pass@host`)
 *   - path/query/fragment
 *   - CRLF (header injection on some fetch backends)
 *   - loopback / link-local / RFC1918 / cloud-metadata addresses
 *
 * Returns `null` when the host fails any check; callers drop the batch.
 */
export function buildTelemetryUrl(host: string, path: string): string | null {
  if (typeof host !== 'string' || host.length === 0) {
    return null;
  }

  // Reject ASCII whitespace + common zero-width/BOM codepoints that JS `\s`
  // does not cover but `new URL` silently strips.
  if (/[\s\u200b-\u200f\u2060\ufeff]/.test(host)) {
    return null;
  }

  const cleanHost = host.replace(/^https?:\/\//, '').replace(/\/+$/, '');
  if (cleanHost.length === 0) {
    return null;
  }

  // Reject anything that looks like userinfo / path / protocol-relative
  // prefix before URL parsing. `new URL('https://' + '//x')` would otherwise
  // normalise the doubled slash and accept `x` as the host.
  if (/[/\\@]/.test(cleanHost)) {
    return null;
  }

  let parsed: URL;
  try {
    parsed = new URL(`https://${cleanHost}`);
  } catch {
    return null;
  }

  if (
    parsed.pathname !== '/' ||
    parsed.search !== '' ||
    parsed.hash !== '' ||
    parsed.username !== '' ||
    parsed.password !== ''
  ) {
    return null;
  }

  // Defence in depth: ensure `new URL` did not silently rewrite the host we
  // validated (e.g. by stripping a codepoint we missed above). `new URL`
  // normalises away the default :443 for https, so compare using the
  // port-stripped hostname instead of .host.
  const expectedHost = cleanHost.toLowerCase().replace(/:443$/, '');
  const actualHost = parsed.host.toLowerCase().replace(/:443$/, '');
  if (actualHost !== expectedHost) {
    return null;
  }

  if (BLOCKED_HOST_PATTERNS.some((r) => r.test(parsed.hostname))) {
    return null;
  }

  return `https://${parsed.host}${path}`;
}

/**
 * Prefixes the Databricks driver uses for internal token formats. Kept in
 * sync with `lib/utils/buildUserAgentString.ts`'s `redactInternalToken`.
 * Extending one list should extend the other.
 */
const DATABRICKS_TOKEN_PREFIXES = ['dkea', 'dskea', 'dapi', 'dsapi', 'dose'];

const SECRET_PATTERNS: Array<[RegExp, string]> = [
  // `Authorization: Bearer <token>` / `Bearer <token>` anywhere in a stack.
  [/Bearer\s+[A-Za-z0-9._\-+/=]+/gi, 'Bearer <REDACTED>'],
  // `Authorization: Basic <base64>`.
  [/Basic\s+[A-Za-z0-9+/=]+/gi, 'Basic <REDACTED>'],
  // URL userinfo: `https://user:pass@host/…`.
  [/([a-z][a-z0-9+.-]*:\/\/)[^/\s:@]+:[^/\s@]+@/gi, '$1<REDACTED>@'],
  // Databricks PATs / service-token prefixes without `Bearer`, e.g.
  // `token is dapi0123…` — appears in error stacks that echo the raw value.
  [new RegExp(`\\b(?:${DATABRICKS_TOKEN_PREFIXES.join('|')})[A-Za-z0-9]{8,}`, 'g'), '<REDACTED>'],
  // JWTs (three base64url segments separated by dots).
  [/\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g, '<REDACTED_JWT>'],
  // JSON-encoded secrets: `"client_secret":"..."`, `"access_token":"..."` etc.
  [
    /"(password|token|client_secret|refresh_token|access_token|id_token|secret|api[_-]?key|apikey)"\s*:\s*"[^"]*"/gi,
    '"$1":"<REDACTED>"',
  ],
  // Form-URL-encoded / key=value secrets.
  [
    /\b(token|password|client_secret|refresh_token|access_token|id_token|secret|api[_-]?key|apikey)=[^\s&"']+/gi,
    '$1=<REDACTED>',
  ],
];

/**
 * Strips common secret shapes from a free-form error string and caps length.
 * Applied before anything is shipped off-box. Redaction happens before
 * truncation so a long stack cannot bury a secret past the cap; truncation
 * then runs a second pass to catch anything that appeared only in the tail.
 */
export function redactSensitive(value: string | undefined, maxLen = 2048): string {
  if (!value) {
    return '';
  }
  let redacted = value;
  for (const [pattern, replacement] of SECRET_PATTERNS) {
    redacted = redacted.replace(pattern, replacement);
  }
  if (redacted.length > maxLen) {
    redacted = `${redacted.slice(0, maxLen)}…[truncated]`;
    for (const [pattern, replacement] of SECRET_PATTERNS) {
      redacted = redacted.replace(pattern, replacement);
    }
  }
  return redacted;
}

/**
 * Normalises any `HeadersInit` shape (`Headers`, `[string,string][]`, or
 * `Record<string, string>`) into a plain string dictionary. Non-string
 * values are dropped. Shared by the exporter and FeatureFlagCache so there
 * is one source of truth for auth-header handling.
 */
export function normalizeHeaders(raw: unknown): Record<string, string> {
  if (!raw || typeof raw !== 'object') {
    return {};
  }
  // Avoid importing node-fetch here; use structural check.
  if (typeof (raw as any).forEach === 'function' && !Array.isArray(raw)) {
    const out: Record<string, string> = {};
    (raw as any).forEach((value: unknown, key: unknown) => {
      if (typeof key === 'string' && typeof value === 'string') {
        out[key] = value;
      }
    });
    return out;
  }
  if (Array.isArray(raw)) {
    const out: Record<string, string> = {};
    for (const entry of raw as Array<[unknown, unknown]>) {
      if (Array.isArray(entry) && entry.length === 2 && typeof entry[0] === 'string' && typeof entry[1] === 'string') {
        const [key, value] = entry;
        out[key] = value;
      }
    }
    return out;
  }
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
    if (typeof v === 'string') {
      out[k] = v;
    }
  }
  return out;
}

/**
 * Case-insensitive check for a non-empty `Authorization` header.
 */
export function hasAuthorization(headers: Record<string, string>): boolean {
  for (const key of Object.keys(headers)) {
    if (key.toLowerCase() === 'authorization' && headers[key]) {
      return true;
    }
  }
  return false;
}

/**
 * Returns a safe `process_name` value: the basename of the first whitespace-
 * delimited token, with trailing whitespace trimmed. This defeats both the
 * absolute-path PII leak (`/home/<user>/app.js`) and the argv-leak shape
 * (`node --db-password=X app.js`) that some producers pass in.
 */
export function sanitizeProcessName(name: string | undefined): string {
  if (!name) {
    return '';
  }
  const trimmed = name.trim();
  if (trimmed.length === 0) {
    return '';
  }
  // Drop argv tail: anything after the first whitespace — argv[0] shouldn't
  // contain spaces, but producers sometimes pass `argv.join(' ')`.
  const firstToken = trimmed.split(/\s/, 1)[0];
  if (!firstToken) {
    return '';
  }
  const lastSep = Math.max(firstToken.lastIndexOf('/'), firstToken.lastIndexOf('\\'));
  return lastSep < 0 ? firstToken : firstToken.slice(lastSep + 1);
}
