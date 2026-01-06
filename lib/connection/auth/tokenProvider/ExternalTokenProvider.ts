import ITokenProvider from './ITokenProvider';
import Token from './Token';

/**
 * Type for the callback function that retrieves tokens from external sources.
 */
export type TokenCallback = () => Promise<string>;

/**
 * A token provider that delegates token retrieval to an external callback function.
 * Useful for integrating with secret managers, vaults, or other token sources.
 */
export default class ExternalTokenProvider implements ITokenProvider {
  private readonly getTokenCallback: TokenCallback;

  private readonly parseJWT: boolean;

  private readonly providerName: string;

  /**
   * Creates a new ExternalTokenProvider.
   * @param getToken - Callback function that returns the access token string
   * @param options - Optional configuration
   * @param options.parseJWT - If true, attempt to extract expiration from JWT payload (default: true)
   * @param options.name - Custom name for this provider (default: "ExternalTokenProvider")
   */
  constructor(
    getToken: TokenCallback,
    options?: {
      parseJWT?: boolean;
      name?: string;
    },
  ) {
    this.getTokenCallback = getToken;
    this.parseJWT = options?.parseJWT ?? true;
    this.providerName = options?.name ?? 'ExternalTokenProvider';
  }

  async getToken(): Promise<Token> {
    const accessToken = await this.getTokenCallback();

    if (this.parseJWT) {
      return Token.fromJWT(accessToken);
    }

    return new Token(accessToken);
  }

  getName(): string {
    return this.providerName;
  }
}
