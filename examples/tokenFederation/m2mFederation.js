'use strict';
/**
 * Example: Machine-to-Machine (M2M) Token Federation with Service Principal
 *
 * This example demonstrates how to use token federation with a service
 * principal or machine identity. This is useful for server-to-server
 * authentication where there is no interactive user.
 *
 * When using M2M federation, you typically need to provide a client_id
 * to identify the service principal to Databricks.
 */
Object.defineProperty(exports, '__esModule', { value: true });
const sql_1 = require('@databricks/sql');
// Example: Fetch a service account token from your identity provider
async function getServiceAccountToken() {
  // Example for Azure service principal:
  //
  // import { ClientSecretCredential } from '@azure/identity';
  // const credential = new ClientSecretCredential(
  //   process.env.AZURE_TENANT_ID!,
  //   process.env.AZURE_CLIENT_ID!,
  //   process.env.AZURE_CLIENT_SECRET!
  // );
  // const token = await credential.getToken('https://your-scope/.default');
  // return token.token;
  // For this example, we use an environment variable
  const token = process.env.SERVICE_ACCOUNT_TOKEN;
  console.log('Fetched service account token');
  return token;
}
async function main() {
  const host = process.env.DATABRICKS_HOST;
  const path = process.env.DATABRICKS_HTTP_PATH;
  const clientId = process.env.DATABRICKS_CLIENT_ID;
  const client = new sql_1.DBSQLClient();
  // Connect using M2M token federation
  // The federationClientId identifies your service principal to Databricks
  await client.connect({
    host,
    path,
    authType: 'external-token',
    getToken: getServiceAccountToken,
    enableTokenFederation: true,
    federationClientId: clientId, // Required for M2M/SP federation
  });
  console.log('Connected successfully with M2M token federation');
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
//# sourceMappingURL=m2mFederation.js.map
