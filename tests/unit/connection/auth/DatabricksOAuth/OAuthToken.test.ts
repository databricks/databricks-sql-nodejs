import { expect } from 'chai';
import OAuthToken from '../../../../../lib/connection/auth/DatabricksOAuth/OAuthToken';

import { createAccessToken } from '../../../.stubs/OAuth';

describe('OAuthToken', () => {
  it('should be properly initialized', () => {
    const accessToken = 'access';
    const refreshToken = 'refresh';
    const scopes = ['test'];

    const token1 = new OAuthToken(accessToken);
    expect(token1.accessToken).to.be.equal(accessToken);

    const token2 = new OAuthToken(accessToken, refreshToken);
    expect(token2.accessToken).to.be.equal(accessToken);
    expect(token2.refreshToken).to.be.equal(refreshToken);

    const token3 = new OAuthToken(accessToken, refreshToken, scopes);
    expect(token3.accessToken).to.be.equal(accessToken);
    expect(token3.refreshToken).to.be.equal(refreshToken);
    expect(token3.scopes).to.deep.equal(scopes);
  });

  it('should return valid expiration time', () => {
    const expirationTime = Math.trunc(Date.now() / 1000);
    const accessToken = createAccessToken(expirationTime);

    const token = new OAuthToken(accessToken);
    expect(token.expirationTime).to.be.equal(expirationTime);
    // second attempt - to make sure it returns the same value
    expect(token.expirationTime).to.be.equal(expirationTime);
  });

  it('should throw error if cannot get expiration time', () => {
    expect(() => {
      const token = new OAuthToken('without_payload');
      expect(token.expirationTime).to.be.equal(undefined);
    }).to.throw();

    expect(() => {
      const token = new OAuthToken('invalid.payload');
      expect(token.expirationTime).to.be.equal(undefined);
    }).to.throw();

    expect(() => {
      const payload = Buffer.from('qwerty', 'utf8').toString('base64');
      const token = new OAuthToken(`malformed.${payload}`);
      expect(token.expirationTime).to.be.equal(undefined);
    }).to.throw();
  });

  it('should test for expired token', () => {
    const expirationTime = Math.trunc(Date.now() / 1000) - 1;
    const accessToken = createAccessToken(expirationTime);

    const token = new OAuthToken(accessToken);
    expect(token.expirationTime).to.be.equal(expirationTime);
    expect(token.hasExpired).to.be.true;
  });

  it('should test for valid token', () => {
    const expirationTime = Math.trunc(Date.now() / 1000) + 1;
    const accessToken = createAccessToken(expirationTime);

    const token = new OAuthToken(accessToken);
    expect(token.expirationTime).to.be.equal(expirationTime);
    expect(token.hasExpired).to.be.false;
  });
});
