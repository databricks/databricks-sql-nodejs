export enum OAuthScope {
  offlineAccess = 'offline_access',
  SQL = 'sql',
  allAPIs = 'all-apis',
}

export type OAuthScopes = Array<string>;

export const defaultOAuthScopes: OAuthScopes = [OAuthScope.SQL, OAuthScope.offlineAccess];

export const scopeDelimiter = ' ';
