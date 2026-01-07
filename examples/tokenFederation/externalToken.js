'use strict';
/**
 * Example: Using an external token provider
 *
 * This example demonstrates how to use a callback function to provide
 * tokens dynamically. This is useful for integrating with secret managers,
 * vaults, or other token sources that may refresh tokens.
 */
Object.defineProperty(exports, '__esModule', { value: true });
const sql_1 = require('@databricks/sql');
// Simulate fetching a token from a secret manager or vault
async function fetchTokenFromVault() {
  // In a real application, this would fetch from AWS Secrets Manager,
  // Azure Key Vault, HashiCorp Vault, or another secret manager
  console.log('Fetching token from vault...');
  // Simulated token - replace with actual vault integration
  const token = process.env.DATABRICKS_TOKEN;
  return token;
}
async function main() {
  const host = process.env.DATABRICKS_HOST;
  const path = process.env.DATABRICKS_HTTP_PATH;
  const client = new sql_1.DBSQLClient();
  // Connect using an external token provider
  // The callback will be called each time a new token is needed
  // Note: The token is automatically cached, so the callback won't be
  // called on every request
  await client.connect({
    host,
    path,
    authType: 'external-token',
    getToken: fetchTokenFromVault,
  });
  console.log('Connected successfully with external token provider');
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
//# sourceMappingURL=externalToken.js.map
