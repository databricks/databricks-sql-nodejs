/**
 * LOCAL TELEMETRY TEST - NOT FOR COMMIT
 *
 * This test verifies telemetry requests are properly sent.
 * Run locally with valid credentials to check telemetry payload structure.
 *
 * Set environment variables:
 * - DATABRICKS_SERVER_HOSTNAME
 * - DATABRICKS_HTTP_PATH
 * - DATABRICKS_TOKEN
 */

import { DBSQLClient, LogLevel } from '../../lib';
import IDBSQLLogger from '../../lib/contracts/IDBSQLLogger';

// Custom logger to capture telemetry debug logs
class DebugLogger implements IDBSQLLogger {
  async log(level: LogLevel, message: string): Promise<void> {
    const timestamp = new Date().toISOString();
    const levelStr = LogLevel[level].padEnd(5);

    // Highlight telemetry-related logs
    if (message.includes('telemetry') || message.includes('Telemetry')) {
      console.log(`\x1b[36m[${timestamp}] [${levelStr}] ${message}\x1b[0m`);
    } else {
      console.log(`[${timestamp}] [${levelStr}] ${message}`);
    }
  }
}

describe('Telemetry E2E Test (Local Only)', () => {
  it('should send telemetry for SELECT 1 query', async function () {
    this.timeout(30000);

    // Check for required environment variables
    const host = process.env.DATABRICKS_SERVER_HOSTNAME;
    const path = process.env.DATABRICKS_HTTP_PATH;
    const token = process.env.DATABRICKS_TOKEN;

    if (!host || !path || !token) {
      console.log('\n❌ Skipping test: Missing environment variables');
      console.log('Set the following variables to run this test:');
      console.log('  - DATABRICKS_SERVER_HOSTNAME');
      console.log('  - DATABRICKS_HTTP_PATH');
      console.log('  - DATABRICKS_TOKEN\n');
      this.skip();
      return;
    }

    console.log('\n' + '='.repeat(60));
    console.log('TELEMETRY E2E TEST');
    console.log('='.repeat(60));

    const client = new DBSQLClient({
      logger: new DebugLogger(),
    });

    console.log('\n📡 Connecting with telemetry enabled...\n');

    const connection = await client.connect({
      host,
      path,
      token,
      telemetryEnabled: true,
      telemetryBatchSize: 1, // Flush immediately for testing
    });

    console.log('\n' + '='.repeat(60));
    console.log('EXECUTING SELECT 1');
    console.log('='.repeat(60) + '\n');

    const session = await connection.openSession();
    const queryOperation = await session.executeStatement('SELECT 1', {
      runAsync: false,
    });

    const result = await queryOperation.fetchAll();
    console.log('\n✅ Query Result:', JSON.stringify(result, null, 2));

    await queryOperation.close();
    console.log('\n📝 Statement closed - waiting for telemetry flush...\n');

    // Wait for telemetry to flush
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 3000);
    });

    console.log('\n' + '='.repeat(60));
    console.log('CLEANING UP');
    console.log('='.repeat(60) + '\n');

    await session.close();
    await connection.close();

    // Wait for final flush
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 2000);
    });

    console.log('\n' + '='.repeat(60));
    console.log('TEST COMPLETE');
    console.log('='.repeat(60));
    console.log('\nCheck the logs above for telemetry-related messages (shown in cyan)');
    console.log('Look for:');
    console.log('  - "Exporting N telemetry metrics"');
    console.log('  - "Successfully exported N telemetry metrics"');
    console.log('  - "Feature flag enabled: true"\n');
  });
});
