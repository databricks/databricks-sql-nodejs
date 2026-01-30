import Token from './Token';

/**
 * Interface for token providers that supply access tokens for authentication.
 * Token providers can be wrapped with caching and federation decorators.
 */
export default interface ITokenProvider {
  /**
   * Retrieves an access token for authentication.
   * @returns A Promise that resolves to a Token object containing the access token
   */
  getToken(): Promise<Token>;

  /**
   * Returns the name of this token provider for logging and debugging purposes.
   * @returns The provider name
   */
  getName(): string;
}
