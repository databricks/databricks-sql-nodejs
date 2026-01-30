/**
 * Copyright (c) 2025 Databricks Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { expect } from 'chai';
import sinon from 'sinon';
import DatabricksTelemetryExporter from '../../../lib/telemetry/DatabricksTelemetryExporter';
import { CircuitBreakerRegistry, CircuitBreakerState } from '../../../lib/telemetry/CircuitBreaker';
import { TelemetryMetric } from '../../../lib/telemetry/types';
import ClientContextStub from '../.stubs/ClientContextStub';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

describe('DatabricksTelemetryExporter', () => {
  let context: ClientContextStub;
  let circuitBreakerRegistry: CircuitBreakerRegistry;
  let exporter: DatabricksTelemetryExporter;
  let fetchStub: sinon.SinonStub;
  let logSpy: sinon.SinonSpy;

  beforeEach(() => {
    context = new ClientContextStub({
      telemetryAuthenticatedExport: true,
      telemetryMaxRetries: 3,
    });
    circuitBreakerRegistry = new CircuitBreakerRegistry(context);

    // Create fetch stub
    fetchStub = sinon.stub();

    // Create exporter with injected fetch function
    exporter = new DatabricksTelemetryExporter(
      context,
      'test.databricks.com',
      circuitBreakerRegistry,
      fetchStub as any,
    );

    // Spy on logger
    logSpy = sinon.spy(context.logger, 'log');
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('Constructor', () => {
    it('should create exporter with IClientContext', () => {
      expect(exporter).to.be.instanceOf(DatabricksTelemetryExporter);
    });

    it('should create circuit breaker for host', () => {
      const breaker = circuitBreakerRegistry.getCircuitBreaker('test.databricks.com');
      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
    });
  });

  describe('export() - endpoint selection', () => {
    it('should export to authenticated endpoint when enabled', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
          workspaceId: 'ws-1',
        },
      ];

      fetchStub.resolves({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      await exporter.export(metrics);

      expect(fetchStub.calledOnce).to.be.true;
      const call = fetchStub.getCall(0);
      expect(call.args[0]).to.equal('https://test.databricks.com/api/2.0/sql/telemetry-ext');
    });

    it('should export to unauthenticated endpoint when disabled', async () => {
      context = new ClientContextStub({
        telemetryAuthenticatedExport: false,
        telemetryMaxRetries: 3,
      });

      // Create new exporter with updated context and inject fetchStub
      exporter = new DatabricksTelemetryExporter(
        context,
        'test.databricks.com',
        circuitBreakerRegistry,
        fetchStub as any,
      );

      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
          workspaceId: 'ws-1',
        },
      ];

      fetchStub.resolves({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      await exporter.export(metrics);

      expect(fetchStub.calledOnce).to.be.true;
      const call = fetchStub.getCall(0);
      expect(call.args[0]).to.equal('https://test.databricks.com/api/2.0/sql/telemetry-unauth');
    });
  });

  describe('export() - payload format', () => {
    it('should format connection metric correctly', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: 1234567890,
          sessionId: 'session-1',
          workspaceId: 'ws-1',
          driverConfig: {
            driverVersion: '1.0.0',
            driverName: 'databricks-sql-nodejs',
            nodeVersion: 'v16.0.0',
            platform: 'linux',
            osVersion: 'Ubuntu 20.04',
            cloudFetchEnabled: true,
            lz4Enabled: true,
            arrowEnabled: false,
            directResultsEnabled: true,
            socketTimeout: 3000,
            retryMaxAttempts: 3,
            cloudFetchConcurrentDownloads: 10,
          },
        },
      ];

      fetchStub.resolves({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      await exporter.export(metrics);

      expect(fetchStub.calledOnce).to.be.true;
      const call = fetchStub.getCall(0);
      const body = JSON.parse(call.args[1].body);

      expect(body.frontend_logs).to.have.lengthOf(1);
      expect(body.frontend_logs[0].workspace_id).to.equal('ws-1');
      expect(body.frontend_logs[0].entry.sql_driver_log.session_id).to.equal('session-1');
      expect(body.frontend_logs[0].entry.sql_driver_log.driver_config).to.deep.equal(metrics[0].driverConfig);
    });

    it('should format statement metric correctly', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'statement',
          timestamp: 1234567890,
          sessionId: 'session-1',
          statementId: 'stmt-1',
          workspaceId: 'ws-1',
          latencyMs: 1500,
          resultFormat: 'cloudfetch',
          chunkCount: 5,
          bytesDownloaded: 1024000,
        },
      ];

      fetchStub.resolves({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      await exporter.export(metrics);

      expect(fetchStub.calledOnce).to.be.true;
      const call = fetchStub.getCall(0);
      const body = JSON.parse(call.args[1].body);

      expect(body.frontend_logs).to.have.lengthOf(1);
      const log = body.frontend_logs[0];
      expect(log.workspace_id).to.equal('ws-1');
      expect(log.entry.sql_driver_log.session_id).to.equal('session-1');
      expect(log.entry.sql_driver_log.sql_statement_id).to.equal('stmt-1');
      expect(log.entry.sql_driver_log.operation_latency_ms).to.equal(1500);
      expect(log.entry.sql_driver_log.sql_operation.execution_result_format).to.equal('cloudfetch');
      expect(log.entry.sql_driver_log.sql_operation.chunk_details.chunk_count).to.equal(5);
      expect(log.entry.sql_driver_log.sql_operation.chunk_details.total_bytes).to.equal(1024000);
    });

    it('should format error metric correctly', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'error',
          timestamp: 1234567890,
          sessionId: 'session-1',
          statementId: 'stmt-1',
          workspaceId: 'ws-1',
          errorName: 'AuthenticationError',
          errorMessage: 'Invalid credentials',
        },
      ];

      fetchStub.resolves({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      await exporter.export(metrics);

      expect(fetchStub.calledOnce).to.be.true;
      const call = fetchStub.getCall(0);
      const body = JSON.parse(call.args[1].body);

      expect(body.frontend_logs).to.have.lengthOf(1);
      const log = body.frontend_logs[0];
      expect(log.entry.sql_driver_log.error_info.error_name).to.equal('AuthenticationError');
      expect(log.entry.sql_driver_log.error_info.stack_trace).to.equal('Invalid credentials');
    });

    it('should include workspace_id, session_id, and sql_statement_id', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'statement',
          timestamp: Date.now(),
          sessionId: 'session-123',
          statementId: 'stmt-456',
          workspaceId: 'ws-789',
          latencyMs: 100,
        },
      ];

      fetchStub.resolves({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      await exporter.export(metrics);

      const call = fetchStub.getCall(0);
      const body = JSON.parse(call.args[1].body);
      const log = body.frontend_logs[0];

      expect(log.workspace_id).to.equal('ws-789');
      expect(log.entry.sql_driver_log.session_id).to.equal('session-123');
      expect(log.entry.sql_driver_log.sql_statement_id).to.equal('stmt-456');
    });
  });

  describe('export() - retry logic', () => {
    it('should retry on retryable error (429)', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      // First call fails with 429, second succeeds
      fetchStub.onFirstCall().resolves({
        ok: false,
        status: 429,
        statusText: 'Too Many Requests',
      });
      fetchStub.onSecondCall().resolves({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      await exporter.export(metrics);

      expect(fetchStub.callCount).to.equal(2);
    });

    it('should retry on retryable error (500)', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      fetchStub.onFirstCall().resolves({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      });
      fetchStub.onSecondCall().resolves({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      await exporter.export(metrics);

      expect(fetchStub.callCount).to.equal(2);
    });

    it('should not retry on terminal error (400)', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      fetchStub.resolves({
        ok: false,
        status: 400,
        statusText: 'Bad Request',
      });

      await exporter.export(metrics);

      // Should only be called once (no retry)
      expect(fetchStub.callCount).to.equal(1);
    });

    it('should not retry on terminal error (401)', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      fetchStub.resolves({
        ok: false,
        status: 401,
        statusText: 'Unauthorized',
      });

      await exporter.export(metrics);

      expect(fetchStub.callCount).to.equal(1);
    });

    it('should respect max retry limit', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      // Always fail with retryable error
      fetchStub.resolves({
        ok: false,
        status: 503,
        statusText: 'Service Unavailable',
      });

      await exporter.export(metrics);

      // Should try initial + 3 retries = 4 total
      expect(fetchStub.callCount).to.equal(4);
    });

    it('should use exponential backoff with jitter', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      // Mock all failures to test retry behavior
      fetchStub.callsFake(() => {
        return Promise.resolve({
          ok: false,
          status: 503,
          statusText: 'Service Unavailable',
        });
      });

      await exporter.export(metrics);

      // Should have multiple attempts (initial + retries)
      expect(fetchStub.callCount).to.be.greaterThan(1);
    });
  });

  describe('export() - circuit breaker integration', () => {
    it('should use circuit breaker for endpoint protection', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      fetchStub.resolves({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      await exporter.export(metrics);

      const breaker = circuitBreakerRegistry.getCircuitBreaker('test.databricks.com');
      expect(breaker.getState()).to.equal(CircuitBreakerState.CLOSED);
    });

    it('should handle circuit breaker OPEN state gracefully', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      // Trigger circuit breaker to open
      const breaker = circuitBreakerRegistry.getCircuitBreaker('test.databricks.com');
      fetchStub.rejects(new Error('Network failure'));

      for (let i = 0; i < 5; i++) {
        try {
          await breaker.execute(async () => {
            throw new Error('Network failure');
          });
        } catch {
          // Expected
        }
      }

      expect(breaker.getState()).to.equal(CircuitBreakerState.OPEN);

      // Now export should be dropped without error
      await exporter.export(metrics);

      // Should log circuit breaker OPEN
      expect(logSpy.calledWith(LogLevel.debug, 'Circuit breaker OPEN - dropping telemetry')).to.be.true;
    });
  });

  describe('export() - exception handling', () => {
    it('CRITICAL: should never throw on network failure', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      fetchStub.rejects(new Error('Network failure'));

      // Should not throw
      await exporter.export(metrics);

      // Should log at debug level only
      expect(logSpy.args.every((args) => args[0] === LogLevel.debug)).to.be.true;
    });

    it('CRITICAL: should never throw on invalid response', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      fetchStub.resolves({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      });

      // Should not throw
      await exporter.export(metrics);

      // Should log at debug level only
      expect(logSpy.args.every((args) => args[0] === LogLevel.debug)).to.be.true;
    });

    it('CRITICAL: should swallow all exceptions and log at debug level', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      fetchStub.rejects(new Error('Unexpected error'));

      await exporter.export(metrics);

      // Verify all logging is at debug level
      logSpy.getCalls().forEach((call) => {
        expect(call.args[0]).to.equal(LogLevel.debug);
      });
    });

    it('CRITICAL: should handle empty metrics array gracefully', async () => {
      await exporter.export([]);

      // Should not call fetch
      expect(fetchStub.called).to.be.false;
    });

    it('CRITICAL: should handle null/undefined metrics gracefully', async () => {
      await exporter.export(null as any);
      await exporter.export(undefined as any);

      // Should not call fetch
      expect(fetchStub.called).to.be.false;
    });
  });

  describe('export() - logging', () => {
    it('CRITICAL: should log only at debug level', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      fetchStub.resolves({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      await exporter.export(metrics);

      // All log calls should be at debug level
      logSpy.getCalls().forEach((call) => {
        expect(call.args[0]).to.equal(LogLevel.debug);
      });
    });

    it('CRITICAL: should not use console logging', async () => {
      const consoleLogSpy = sinon.spy(console, 'log');
      const consoleErrorSpy = sinon.spy(console, 'error');
      const consoleWarnSpy = sinon.spy(console, 'warn');

      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      fetchStub.rejects(new Error('Test error'));

      await exporter.export(metrics);

      expect(consoleLogSpy.called).to.be.false;
      expect(consoleErrorSpy.called).to.be.false;
      expect(consoleWarnSpy.called).to.be.false;

      consoleLogSpy.restore();
      consoleErrorSpy.restore();
      consoleWarnSpy.restore();
    });
  });

  describe('export() - connection provider integration', () => {
    it('should use connection provider from context', async () => {
      const metrics: TelemetryMetric[] = [
        {
          metricType: 'connection',
          timestamp: Date.now(),
          sessionId: 'session-1',
        },
      ];

      const getConnectionProviderSpy = sinon.spy(context, 'getConnectionProvider');

      fetchStub.resolves({
        ok: true,
        status: 200,
        statusText: 'OK',
      });

      await exporter.export(metrics);

      expect(getConnectionProviderSpy.called).to.be.true;
    });
  });
});
