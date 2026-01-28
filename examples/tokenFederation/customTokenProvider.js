'use strict';
/**
 * Example: Custom Token Provider Implementation
 *
 * This example demonstrates how to create a custom token provider by
 * implementing the ITokenProvider interface. This gives you full control
 * over token management, including custom caching, refresh logic, and
 * error handling.
 */
var __createBinding =
  (this && this.__createBinding) ||
  (Object.create
    ? function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        var desc = Object.getOwnPropertyDescriptor(m, k);
        if (!desc || ('get' in desc ? !m.__esModule : desc.writable || desc.configurable)) {
          desc = {
            enumerable: true,
            get: function () {
              return m[k];
            },
          };
        }
        Object.defineProperty(o, k2, desc);
      }
    : function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        o[k2] = m[k];
      });
var __setModuleDefault =
  (this && this.__setModuleDefault) ||
  (Object.create
    ? function (o, v) {
        Object.defineProperty(o, 'default', { enumerable: true, value: v });
      }
    : function (o, v) {
        o['default'] = v;
      });
var __importStar =
  (this && this.__importStar) ||
  function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null)
      for (var k in mod)
        if (k !== 'default' && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
  };
Object.defineProperty(exports, '__esModule', { value: true });
const sql_1 = require('@databricks/sql');
const tokenProvider_1 = require('../../lib/connection/auth/tokenProvider');
/**
 * Custom token provider that refreshes tokens from a custom OAuth server.
 */
class CustomOAuthTokenProvider {
  constructor(oauthServerUrl, clientId, clientSecret) {
    this.oauthServerUrl = oauthServerUrl;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }
  async getToken() {
    var _a;
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
    const data = await response.json();
    // Calculate expiration
    let expiresAt;
    if (typeof data.expires_in === 'number') {
      expiresAt = new Date(Date.now() + data.expires_in * 1000);
    }
    return new tokenProvider_1.Token(data.access_token, {
      tokenType: (_a = data.token_type) !== null && _a !== void 0 ? _a : 'Bearer',
      expiresAt,
    });
  }
  getName() {
    return 'CustomOAuthTokenProvider';
  }
}
/**
 * Simple token provider that reads from a file (for development/testing).
 */
class FileTokenProvider {
  constructor(filePath) {
    this.filePath = filePath;
  }
  async getToken() {
    const fs = await Promise.resolve().then(() => __importStar(require('fs/promises')));
    const tokenData = await fs.readFile(this.filePath, 'utf-8');
    const parsed = JSON.parse(tokenData);
    return tokenProvider_1.Token.fromJWT(parsed.access_token, {
      refreshToken: parsed.refresh_token,
    });
  }
  getName() {
    return 'FileTokenProvider';
  }
}
async function main() {
  const host = process.env.DATABRICKS_HOST;
  const path = process.env.DATABRICKS_HTTP_PATH;
  const client = new sql_1.DBSQLClient();
  // Option 1: Use a custom OAuth token provider
  const oauthProvider = new CustomOAuthTokenProvider(
    process.env.OAUTH_SERVER_URL,
    process.env.OAUTH_CLIENT_ID,
    process.env.OAUTH_CLIENT_SECRET,
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
//# sourceMappingURL=customTokenProvider.js.map
