/**
 * Example: Token Federation with an External Identity Provider
 *
 * This example demonstrates how to use token federation to automatically
 * exchange tokens from external identity providers (Azure AD, Google, Okta,
 * Auth0, AWS Cognito, GitHub) for Databricks-compatible tokens.
 *
 * Token federation uses RFC 8693 (OAuth 2.0 Token Exchange) to exchange
 * the external JWT token for a Databricks access token.
 */

import { DBSQLClient } from '@databricks/sql';

// Example: Fetch a token from Azure AD
// In a real application, you would use the Azure SDK or similar
async function getAzureADToken(): Promise<string> {
  // Example using @azure/identity:
  //
  // import { DefaultAzureCredential } from '@azure/identity';
  // const credential = new DefaultAzureCredential();
  // const token = await credential.getToken('https://your-scope/.default');
  // return token.token;

  // For this example, we use an environment variable
  const token = process.env.AZURE_AD_TOKEN!;
  console.log('Fetched token from Azure AD');
  return token;
}

// Example: Fetch a token from Google
async function getGoogleToken(): Promise<string> {
  // Example using google-auth-library:
  //
  // import { GoogleAuth } from 'google-auth-library';
  // const auth = new GoogleAuth();
  // const client = await auth.getClient();
  // const token = await client.getAccessToken();
  // return token.token;

  const token = process.env.GOOGLE_TOKEN!;
  console.log('Fetched token from Google');
  return token;
}

async function main() {
  const host = process.env.DATABRICKS_HOST!;
  const path = process.env.DATABRICKS_HTTP_PATH!;

  const client = new DBSQLClient();

  // Connect using token federation
  // The driver will automatically:
  // 1. Get the token from the callback
  // 2. Check if the token's issuer matches the Databricks host
  // 3. If not, exchange the token for a Databricks token via RFC 8693
  // 4. Cache the result for subsequent requests
  await client.connect({
    host,
    path,
    authType: 'external-token',
    getToken: getAzureADToken, // or getGoogleToken, etc.
    enableTokenFederation: true,
  });

  console.log('Connected successfully with token federation');

  // Open a session and run a query
  const session = await client.openSession();
  const operation = await session.executeStatement('SELECT current_user() AS user');
  const result = await operation.fetchAll();

  console.log('Query result:', result);

  await operation.close();
  await session.close();
  await client.close();
}

main().catch(console.error);
