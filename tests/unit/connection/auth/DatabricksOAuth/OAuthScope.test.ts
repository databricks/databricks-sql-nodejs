import { expect } from 'chai';
import { normalizeOAuthScopes } from '../../../../../lib/connection/auth/DatabricksOAuth/OAuthScope';

describe('normalizeOAuthScopes', () => {
  it('splits entries on runs of whitespace and trims them', () => {
    expect(normalizeOAuthScopes([' sql   offline_access '])).to.deep.equal(['sql', 'offline_access']);
    expect(normalizeOAuthScopes(['sql\toffline_access\n', 'all-apis'])).to.deep.equal([
      'sql',
      'offline_access',
      'all-apis',
    ]);
  });

  it('drops duplicates, keeping first-seen order', () => {
    expect(normalizeOAuthScopes(['offline_access sql', 'sql'])).to.deep.equal(['offline_access', 'sql']);
  });

  it('returns an empty list for unset, blank, or non-array input', () => {
    expect(normalizeOAuthScopes(undefined)).to.deep.equal([]);
    expect(normalizeOAuthScopes([])).to.deep.equal([]);
    expect(normalizeOAuthScopes(['', '   '])).to.deep.equal([]);
    expect(normalizeOAuthScopes('sql' as unknown as string[])).to.deep.equal([]);
  });

  it('ignores non-string entries', () => {
    expect(normalizeOAuthScopes(['sql', 42, null] as unknown as string[])).to.deep.equal(['sql']);
  });
});
