import ITokenProvider from './ITokenProvider';
import Token, { TokenOptions, TokenFromJWTOptions } from './Token';

/**
 * A token provider that returns a static token.
 * Useful for testing or when the token is obtained through external means.
 */
export default class StaticTokenProvider implements ITokenProvider {
  private readonly token: Token;

  /**
   * Creates a new StaticTokenProvider.
   * @param accessToken - The access token string
   * @param options - Optional token configuration (tokenType, expiresAt, refreshToken, scopes)
   */
  constructor(accessToken: string, options?: TokenOptions) {
    this.token = new Token(accessToken, options);
  }

  /**
   * Creates a StaticTokenProvider from a JWT string.
   * The expiration time will be extracted from the JWT payload.
   * @param jwt - The JWT token string
   * @param options - Optional token configuration
   */
  static fromJWT(jwt: string, options?: TokenFromJWTOptions): StaticTokenProvider {
    const token = Token.fromJWT(jwt, options);
    return new StaticTokenProvider(token.accessToken, {
      tokenType: token.tokenType,
      expiresAt: token.expiresAt,
      refreshToken: token.refreshToken,
      scopes: token.scopes,
    });
  }

  async getToken(): Promise<Token> {
    return this.token;
  }

  getName(): string {
    return 'StaticTokenProvider';
  }
}
