import ITokenProvider from './ITokenProvider';
import Token from './Token';

/**
 * Default refresh threshold in milliseconds (5 minutes).
 * Tokens will be refreshed when they are within this threshold of expiring.
 */
const DEFAULT_REFRESH_THRESHOLD_MS = 5 * 60 * 1000;

/**
 * A token provider that wraps another provider with automatic caching.
 * Tokens are cached and reused until they are close to expiring.
 */
export default class CachedTokenProvider implements ITokenProvider {
  private readonly baseProvider: ITokenProvider;

  private readonly refreshThresholdMs: number;

  private cache: Token | null = null;

  private refreshPromise: Promise<Token> | null = null;

  /**
   * Creates a new CachedTokenProvider.
   * @param baseProvider - The underlying token provider to cache
   * @param options - Optional configuration
   * @param options.refreshThresholdMs - Refresh tokens this many ms before expiry (default: 5 minutes)
   */
  constructor(
    baseProvider: ITokenProvider,
    options?: {
      refreshThresholdMs?: number;
    },
  ) {
    this.baseProvider = baseProvider;
    this.refreshThresholdMs = options?.refreshThresholdMs ?? DEFAULT_REFRESH_THRESHOLD_MS;
  }

  async getToken(): Promise<Token> {
    // Return cached token if it's still valid
    if (this.cache && !this.shouldRefresh(this.cache)) {
      return this.cache;
    }

    // If already refreshing, wait for that to complete
    if (this.refreshPromise) {
      return this.refreshPromise;
    }

    // Start refresh
    this.refreshPromise = this.refreshToken();

    try {
      const token = await this.refreshPromise;
      return token;
    } finally {
      this.refreshPromise = null;
    }
  }

  getName(): string {
    return `cached[${this.baseProvider.getName()}]`;
  }

  /**
   * Clears the cached token, forcing a refresh on the next getToken() call.
   */
  clearCache(): void {
    this.cache = null;
  }

  /**
   * Determines if the token should be refreshed.
   * @param token - The token to check
   * @returns true if the token should be refreshed
   */
  private shouldRefresh(token: Token): boolean {
    // If no expiration is known, don't refresh proactively
    if (!token.expiresAt) {
      return false;
    }

    const now = Date.now();
    const expiresAtMs = token.expiresAt.getTime();
    const refreshAtMs = expiresAtMs - this.refreshThresholdMs;

    return now >= refreshAtMs;
  }

  /**
   * Fetches a new token from the base provider and caches it.
   */
  private async refreshToken(): Promise<Token> {
    const token = await this.baseProvider.getToken();
    this.cache = token;
    return token;
  }
}
