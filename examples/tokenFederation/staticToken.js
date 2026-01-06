"use strict";
/**
 * Example: Using a static token with the token provider system
 *
 * This example demonstrates how to use a static access token with the
 * token provider infrastructure. This is useful when you have a token
 * that doesn't change during the lifetime of your application.
 */
Object.defineProperty(exports, "__esModule", { value: true });
const sql_1 = require("@databricks/sql");
async function main() {
    const host = process.env.DATABRICKS_HOST;
    const path = process.env.DATABRICKS_HTTP_PATH;
    const token = process.env.DATABRICKS_TOKEN;
    const client = new sql_1.DBSQLClient();
    // Connect using a static token
    await client.connect({
        host,
        path,
        authType: 'static-token',
        staticToken: token,
    });
    console.log('Connected successfully with static token');
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
//# sourceMappingURL=staticToken.js.map