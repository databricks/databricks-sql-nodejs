import http from 'http';
import fetch, { RequestInit } from 'node-fetch';
import AuthenticationError from '../../../errors/AuthenticationError';

// Microsoft Entra login authorities (commercial, US Government, China). Matched as host suffixes,
// so a look-alike such as `login.microsoftonline.com.attacker.example` is rejected.
const MICROSOFT_LOGIN_HOST_SUFFIXES = ['.microsoftonline.com', '.microsoftonline.us', '.chinacloudapi.cn'];

const DISCOVERY_TIMEOUT_MS = 30000;

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
export async function discoverAzureTenantId(host: string, agent?: http.Agent): Promise<string> {
  const url = `${(host.includes('://') ? host : `https://${host}`).replace(/\/+$/, '')}/aad/auth`;
  const controller = new AbortController();
  const response = await fetch(url, {
    redirect: 'manual',
    agent,
    timeout: DISCOVERY_TIMEOUT_MS,
    signal: controller.signal as unknown as RequestInit['signal'],
  });
  // Only the headers are needed; aborting releases the socket instead of leaving the body unread.
  controller.abort();

  const location = response.headers.get('location');
  if (response.status < 300 || response.status >= 400 || !location) {
    throw new AuthenticationError(
      `Azure tenant discovery: ${url} returned HTTP ${response.status} without a redirect; pass azureTenantId explicitly`,
    );
  }
  return tenantFromLoginUrl(location);
}
