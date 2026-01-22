import { HeadersInit } from 'node-fetch';

/**
 * Safety buffer in seconds to consider a token expired before its actual expiration time.
 * This prevents using tokens that are about to expire during in-flight requests.
 */
const EXPIRATION_BUFFER_SECONDS = 30;

/**
 * Represents an access token with optional metadata and lifecycle management.
 */
export default class Token {
  private readonly _accessToken: string;

  private readonly _tokenType: string;

  private readonly _expiresAt?: Date;

  private readonly _refreshToken?: string;

  private readonly _scopes?: string[];

  constructor(
    accessToken: string,
    options?: {
      tokenType?: string;
      expiresAt?: Date;
      refreshToken?: string;
      scopes?: string[];
    },
  ) {
    this._accessToken = accessToken;
    this._tokenType = options?.tokenType ?? 'Bearer';
    this._expiresAt = options?.expiresAt;
    this._refreshToken = options?.refreshToken;
    this._scopes = options?.scopes;
  }

  /**
   * The access token string.
   */
  get accessToken(): string {
    return this._accessToken;
  }

  /**
   * The token type (e.g., "Bearer").
   */
  get tokenType(): string {
    return this._tokenType;
  }

  /**
   * The expiration time of the token, if known.
   */
  get expiresAt(): Date | undefined {
    return this._expiresAt;
  }

  /**
   * The refresh token, if available.
   */
  get refreshToken(): string | undefined {
    return this._refreshToken;
  }

  /**
   * The scopes associated with this token.
   */
  get scopes(): string[] | undefined {
    return this._scopes;
  }

  /**
   * Checks if the token has expired, including a safety buffer.
   * Returns false if expiration time is unknown.
   */
  isExpired(): boolean {
    if (!this._expiresAt) {
      return false;
    }
    const now = new Date();
    const bufferMs = EXPIRATION_BUFFER_SECONDS * 1000;
    return this._expiresAt.getTime() - bufferMs <= now.getTime();
  }

  /**
   * Sets the Authorization header on the provided headers object.
   * @param headers - The headers object to modify
   * @returns The modified headers object with Authorization set
   */
  setAuthHeader(headers: HeadersInit): HeadersInit {
    return {
      ...headers,
      Authorization: `${this._tokenType} ${this._accessToken}`,
    };
  }

  /**
   * Creates a Token from a JWT string, extracting the expiration time from the payload.
   * If the JWT cannot be decoded, the token is created without expiration info.
   * The server will validate the token anyway, so decoding failures are handled gracefully.
   * @param jwt - The JWT token string
   * @param options - Additional token options (tokenType, refreshToken, scopes)
   * @returns A new Token instance with expiration extracted from the JWT (if available)
   */
  static fromJWT(
    jwt: string,
    options?: {
      tokenType?: string;
      refreshToken?: string;
      scopes?: string[];
    },
  ): Token {
    let expiresAt: Date | undefined;

    try {
      const parts = jwt.split('.');
      if (parts.length >= 2) {
        const payload = Buffer.from(parts[1], 'base64').toString('utf8');
        const decoded = JSON.parse(payload);
        if (typeof decoded.exp === 'number') {
          expiresAt = new Date(decoded.exp * 1000);
        }
      }
    } catch {
      // If we can't decode the JWT, we'll proceed without expiration info
      // The server will validate the token anyway
    }

    return new Token(jwt, {
      tokenType: options?.tokenType,
      expiresAt,
      refreshToken: options?.refreshToken,
      scopes: options?.scopes,
    });
  }

  /**
   * Converts the token to a plain object for serialization.
   */
  toJSON(): Record<string, unknown> {
    return {
      accessToken: this._accessToken,
      tokenType: this._tokenType,
      expiresAt: this._expiresAt?.toISOString(),
      refreshToken: this._refreshToken,
      scopes: this._scopes,
    };
  }
}
