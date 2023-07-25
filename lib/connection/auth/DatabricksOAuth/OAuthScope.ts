export enum OAuthScope {
  offlineAccess = 'offline_access',
  SQL = 'sql',
}

export type OAuthScopes = Array<string>;

export const defaultOAuthScopes: OAuthScopes = [OAuthScope.SQL, OAuthScope.offlineAccess];

export const scopeDelimiter = ' ';
