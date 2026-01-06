import fetch from 'node-fetch';
import ITokenProvider from './ITokenProvider';
import Token from './Token';
import { decodeJWT, getJWTIssuer, isSameHost } from './utils';

/**
 * Token exchange endpoint path for Databricks OIDC.
 */
const TOKEN_EXCHANGE_ENDPOINT = '/oidc/v1/token';

/**
 * Grant type for RFC 8693 token exchange.
 */
const TOKEN_EXCHANGE_GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:token-exchange';

/**
 * Subject token type for JWT tokens.
 */
const SUBJECT_TOKEN_TYPE = 'urn:ietf:params:oauth:token-type:jwt';

/**
 * Default scope for SQL operations.
 */
const DEFAULT_SCOPE = 'sql';

/**
 * Timeout for token exchange requests in milliseconds.
 */
const REQUEST_TIMEOUT_MS = 30000;

/**
 * A token provider that wraps another provider with automatic token federation.
 * When the base provider returns a token from a different issuer, this provider
 * exchanges it for a Databricks-compatible token using RFC 8693.
 */
export default class FederationProvider implements ITokenProvider {
  private readonly baseProvider: ITokenProvider;

  private readonly databricksHost: string;

  private readonly clientId?: string;

  private readonly returnOriginalTokenOnFailure: boolean;

  /**
   * Creates a new FederationProvider.
   * @param baseProvider - The underlying token provider
   * @param databricksHost - The Databricks workspace host URL
   * @param options - Optional configuration
   * @param options.clientId - Client ID for M2M/service principal federation
   * @param options.returnOriginalTokenOnFailure - Return original token if exchange fails (default: true)
   */
  constructor(
    baseProvider: ITokenProvider,
    databricksHost: string,
    options?: {
      clientId?: string;
      returnOriginalTokenOnFailure?: boolean;
    },
  ) {
    this.baseProvider = baseProvider;
    this.databricksHost = databricksHost;
    this.clientId = options?.clientId;
    this.returnOriginalTokenOnFailure = options?.returnOriginalTokenOnFailure ?? true;
  }

  async getToken(): Promise<Token> {
    const token = await this.baseProvider.getToken();

    // Check if token needs exchange
    if (!this.needsTokenExchange(token)) {
      return token;
    }

    // Attempt token exchange
    try {
      return await this.exchangeToken(token);
    } catch (error) {
      if (this.returnOriginalTokenOnFailure) {
        // Fall back to original token
        return token;
      }
      throw error;
    }
  }

  getName(): string {
    return `federated[${this.baseProvider.getName()}]`;
  }

  /**
   * Determines if the token needs to be exchanged.
   * @param token - The token to check
   * @returns true if the token should be exchanged
   */
  private needsTokenExchange(token: Token): boolean {
    const issuer = getJWTIssuer(token.accessToken);

    // If we can't extract the issuer, don't exchange (might not be a JWT)
    if (!issuer) {
      return false;
    }

    // If the issuer is the same as Databricks host, no exchange needed
    if (isSameHost(issuer, this.databricksHost)) {
      return false;
    }

    return true;
  }

  /**
   * Exchanges the token for a Databricks-compatible token using RFC 8693.
   * @param token - The token to exchange
   * @returns The exchanged token
   */
  private async exchangeToken(token: Token): Promise<Token> {
    const url = this.buildExchangeUrl();

    const params = new URLSearchParams({
      grant_type: TOKEN_EXCHANGE_GRANT_TYPE,
      subject_token_type: SUBJECT_TOKEN_TYPE,
      subject_token: token.accessToken,
      scope: DEFAULT_SCOPE,
    });

    if (this.clientId) {
      params.append('client_id', this.clientId);
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: params.toString(),
        signal: controller.signal,
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Token exchange failed: ${response.status} ${response.statusText} - ${errorText}`);
      }

      const data = (await response.json()) as {
        access_token?: string;
        token_type?: string;
        expires_in?: number;
      };

      if (!data.access_token) {
        throw new Error('Token exchange response missing access_token');
      }

      // Calculate expiration from expires_in
      let expiresAt: Date | undefined;
      if (typeof data.expires_in === 'number') {
        expiresAt = new Date(Date.now() + data.expires_in * 1000);
      }

      return new Token(data.access_token, {
        tokenType: data.token_type ?? 'Bearer',
        expiresAt,
      });
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Builds the token exchange URL.
   */
  private buildExchangeUrl(): string {
    let host = this.databricksHost;

    // Ensure host has a protocol
    if (!host.includes('://')) {
      host = `https://${host}`;
    }

    // Remove trailing slash
    if (host.endsWith('/')) {
      host = host.slice(0, -1);
    }

    return `${host}${TOKEN_EXCHANGE_ENDPOINT}`;
  }
}
