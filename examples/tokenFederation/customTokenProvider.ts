/**
 * Example: Custom Token Provider Implementation
 *
 * This example demonstrates how to create a custom token provider by
 * implementing the ITokenProvider interface. This gives you full control
 * over token management, including custom caching, refresh logic, and
 * error handling.
 */

import { DBSQLClient } from '@databricks/sql';
import {
  ITokenProvider,
  Token,
} from '../../lib/connection/auth/tokenProvider';

/**
 * Custom token provider that refreshes tokens from a custom OAuth server.
 */
class CustomOAuthTokenProvider implements ITokenProvider {
  private readonly oauthServerUrl: string;
  private readonly clientId: string;
  private readonly clientSecret: string;

  constructor(oauthServerUrl: string, clientId: string, clientSecret: string) {
    this.oauthServerUrl = oauthServerUrl;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }

  async getToken(): Promise<Token> {
    console.log('Fetching token from custom OAuth server...');

    // Example: Fetch token using client credentials grant
    const response = await fetch(`${this.oauthServerUrl}/oauth/token`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: this.clientId,
        client_secret: this.clientSecret,
        scope: 'sql',
      }).toString(),
    });

    if (!response.ok) {
      throw new Error(`OAuth token request failed: ${response.status}`);
    }

    const data = await response.json() as {
      access_token: string;
      token_type?: string;
      expires_in?: number;
    };

    // Calculate expiration
    let expiresAt: Date | undefined;
    if (typeof data.expires_in === 'number') {
      expiresAt = new Date(Date.now() + data.expires_in * 1000);
    }

    return new Token(data.access_token, {
      tokenType: data.token_type ?? 'Bearer',
      expiresAt,
    });
  }

  getName(): string {
    return 'CustomOAuthTokenProvider';
  }
}

/**
 * Simple token provider that reads from a file (for development/testing).
 */
class FileTokenProvider implements ITokenProvider {
  private readonly filePath: string;

  constructor(filePath: string) {
    this.filePath = filePath;
  }

  async getToken(): Promise<Token> {
    const fs = await import('fs/promises');
    const tokenData = await fs.readFile(this.filePath, 'utf-8');
    const parsed = JSON.parse(tokenData);

    return Token.fromJWT(parsed.access_token, {
      refreshToken: parsed.refresh_token,
    });
  }

  getName(): string {
    return 'FileTokenProvider';
  }
}

async function main() {
  const host = process.env.DATABRICKS_HOST!;
  const path = process.env.DATABRICKS_HTTP_PATH!;

  const client = new DBSQLClient();

  // Option 1: Use a custom OAuth token provider
  const oauthProvider = new CustomOAuthTokenProvider(
    process.env.OAUTH_SERVER_URL!,
    process.env.OAUTH_CLIENT_ID!,
    process.env.OAUTH_CLIENT_SECRET!,
  );

  await client.connect({
    host,
    path,
    authType: 'token-provider',
    tokenProvider: oauthProvider,
    // Optionally enable federation if your OAuth server issues non-Databricks tokens
    enableTokenFederation: true,
  });

  console.log('Connected successfully with custom token provider');

  // Open a session and run a query
  const session = await client.openSession();
  const operation = await session.executeStatement('SELECT 1 AS result');
  const result = await operation.fetchAll();

  console.log('Query result:', result);

  await operation.close();
  await session.close();
  await client.close();
}

main().catch(console.error);
