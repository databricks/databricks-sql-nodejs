export enum OAuthScope {
  offlineAccess = 'offline_access',
  SQL = 'sql',
  allAPIs = 'all-apis',
}

export type OAuthScopes = Array<string>;

export const defaultOAuthScopes: OAuthScopes = [OAuthScope.SQL, OAuthScope.offlineAccess];

export const scopeDelimiter = ' ';

/**
 * Splits each entry on whitespace, dropping blanks and duplicates, so `['sql offline_access']`
 * and `['sql', 'offline_access']` are equivalent. Returns `[]` when nothing usable was supplied,
 * which callers treat as "use the flow's default scopes".
 */
export function normalizeOAuthScopes(scopes?: ReadonlyArray<string>): OAuthScopes {
  if (!Array.isArray(scopes)) {
    return [];
  }
  const result = new Set<string>();
  for (const entry of scopes) {
    if (typeof entry === 'string') {
      for (const scope of entry.split(/\s+/)) {
        if (scope.length > 0) {
          result.add(scope);
        }
      }
    }
  }
  return [...result];
}
