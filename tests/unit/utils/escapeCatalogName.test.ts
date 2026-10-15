import { expect } from 'chai';
import escapeCatalogName from '../../../lib/utils/escapeCatalogName';

describe('escapeCatalogName JDBC compatibility', () => {
  it('preserves two backslashes before an underscore, matching JDBC', () => {
    const catalogName = String.raw`my\\_cat`;
    expect(escapeCatalogName(catalogName)).to.equal(catalogName);
  });
});
