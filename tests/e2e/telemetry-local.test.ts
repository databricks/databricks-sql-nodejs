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
import sinon from 'sinon';
import * as nodeFetch from 'node-fetch';

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
  let fetchStub: sinon.SinonStub;

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

    // Stub fetch to capture telemetry payloads
    const originalFetch = nodeFetch.default;
    fetchStub = sinon.stub(nodeFetch, 'default').callsFake(async (url: any, options?: any) => {
      // Capture and log telemetry requests
      if (typeof url === 'string' && (url.includes('/telemetry-ext') || url.includes('/telemetry-unauth'))) {
        const body = options?.body ? JSON.parse(options.body) : null;

        console.log('\n' + '='.repeat(60));
        console.log('📊 TELEMETRY REQUEST CAPTURED');
        console.log('='.repeat(60));
        console.log('URL:', url);

        if (body && body.protoLogs) {
          console.log(`\nProtoLogs count: ${body.protoLogs.length}`);
          body.protoLogs.forEach((log: string, index: number) => {
            const parsed = JSON.parse(log);
            console.log(`\n--- ProtoLog ${index + 1} ---`);
            console.log(JSON.stringify(parsed, null, 2));
          });
        }
        console.log('='.repeat(60) + '\n');
      }

      // Call original fetch
      return originalFetch(url, options);
    });

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
    console.log('\nCheck the logs above for captured telemetry payloads');
    console.log('Should see 3 ProtoLogs:');
    console.log('  1. CONNECTION_OPEN (CREATE_SESSION)');
    console.log('  2. STATEMENT_COMPLETE (EXECUTE_STATEMENT)');
    console.log('  3. CONNECTION_CLOSE (DELETE_SESSION)\n');

    // Restore fetch stub
    if (fetchStub) {
      fetchStub.restore();
    }
  });
});
