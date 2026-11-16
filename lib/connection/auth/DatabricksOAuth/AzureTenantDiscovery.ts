import http from 'http';
import fetch, { RequestInit, Response } from 'node-fetch';
import AuthenticationError from '../../../errors/AuthenticationError';

// Microsoft Entra login authorities (commercial, US Government, China). Matched as host suffixes,
// so a look-alike such as `login.microsoftonline.com.attacker.example` is rejected.
const MICROSOFT_LOGIN_HOST_SUFFIXES = ['.microsoftonline.com', '.microsoftonline.us', '.chinacloudapi.cn'];

// Until the response headers arrive.
const DISCOVERY_TIMEOUT_MS = 30000;

// For the redirect body after the headers (the tenant is already known by then).
const BODY_DRAIN_TIMEOUT_MS = 1000;

/**
 * Returns the tenant id from the Entra login URL that a workspace's `/aad/auth` redirects to
 * (`https://login.microsoftonline.com/{tenant}/oauth2/authorize?...`): its first path segment.
 */
export function tenantFromLoginUrl(location: string): string {
  let url: URL;
  try {
    url = new URL(location);
  } catch {
    throw new AuthenticationError(
      `Azure tenant discovery: /aad/auth redirected to an invalid URL; pass azureTenantId explicitly`,
    );
  }

  const host = url.hostname.toLowerCase();
  if (!MICROSOFT_LOGIN_HOST_SUFFIXES.some((suffix) => host.endsWith(suffix))) {
    throw new AuthenticationError(
      `Azure tenant discovery: /aad/auth did not redirect to a Microsoft login host (got ${host}); pass azureTenantId explicitly`,
    );
  }

  const tenant = url.pathname.split('/')[1];
  if (!tenant) {
    throw new AuthenticationError(
      'Azure tenant discovery: could not read a tenant id from the /aad/auth redirect; pass azureTenantId explicitly',
    );
  }
  return tenant;
}

/**
 * Discovers the workspace's Entra tenant from the first redirect of `{host}/aad/auth`, as the
 * Python connector and the SQL kernel do.
 */
export async function discoverAzureTenantId(
  host: string,
  agent?: http.Agent,
  timeoutsMs = { headers: DISCOVERY_TIMEOUT_MS, bodyDrain: BODY_DRAIN_TIMEOUT_MS },
): Promise<string> {
  const base = (host.includes('://') ? host : `https://${host}`).replace(/\/+$/, '');
  // node-fetch's `timeout` rejects a stalled body without closing the request; aborting destroys the socket.
  const controller = new AbortController();
  let timer = setTimeout(() => controller.abort(), timeoutsMs.headers);
  try {
    let response: Response;
    try {
      response = await fetch(`${base}/aad/auth`, {
        method: 'GET',
        redirect: 'manual',
        agent,
        signal: controller.signal as unknown as RequestInit['signal'],
      });
    } catch (error) {
      // Report only the code/type: a network error's message can carry request details.
      const { code, type } = error as { code?: string; type?: string };
      const reason = type === 'aborted' ? 'timed out' : code ?? type ?? 'network error';
      throw new AuthenticationError(
        `Azure tenant discovery: ${base}/aad/auth failed (${reason}); pass azureTenantId explicitly`,
      );
    }

    // The tenant comes from the headers. Drain the body only so the keep-alive agent can reuse the
    // socket, and abort if that fails or doesn't finish promptly.
    clearTimeout(timer);
    timer = setTimeout(() => controller.abort(), timeoutsMs.bodyDrain);
    await response.text().catch(() => controller.abort());

    const location = response.headers.get('location');
    if (response.status < 300 || response.status >= 400 || !location) {
      throw new AuthenticationError(
        `Azure tenant discovery: ${base}/aad/auth returned HTTP ${response.status} without a redirect; pass azureTenantId explicitly`,
      );
    }
    return tenantFromLoginUrl(location);
  } finally {
    clearTimeout(timer);
  }
}
