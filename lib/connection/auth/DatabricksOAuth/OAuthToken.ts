import { OAuthScopes } from './OAuthScope';

export default class OAuthToken {
  private readonly _accessToken: string;

  private readonly _refreshToken?: string;

  private readonly _scopes?: OAuthScopes;

  private _expirationTime?: number;

  constructor(accessToken: string, refreshToken?: string, scopes?: OAuthScopes) {
    this._accessToken = accessToken;
    this._refreshToken = refreshToken;
    this._scopes = scopes;
  }

  get accessToken(): string {
    return this._accessToken;
  }

  get refreshToken(): string | undefined {
    return this._refreshToken;
  }

  get scopes(): OAuthScopes | undefined {
    return this._scopes;
  }

  get expirationTime(): number {
    // This token has already been verified, and we are just parsing it.
    // If it has been tampered with, it will be rejected on the server side.
    // This avoids having to fetch the public key from the issuer and perform
    // an unnecessary signature verification.
    if (this._expirationTime === undefined) {
      const accessTokenPayload = Buffer.from(this._accessToken.split('.')[1], 'base64').toString('utf8');
      const decoded = JSON.parse(accessTokenPayload);
      this._expirationTime = Number(decoded.exp);
    }
    return this._expirationTime;
  }

  get hasExpired(): boolean {
    const now = Math.floor(Date.now() / 1000); // convert it to seconds
    return this.expirationTime <= now;
  }
}
