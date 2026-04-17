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
import { CircuitBreakerRegistry } from '../../../lib/telemetry/CircuitBreaker';
import { TelemetryMetric } from '../../../lib/telemetry/types';
import ClientContextStub from '../.stubs/ClientContextStub';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import IAuthentication from '../../../lib/connection/contracts/IAuthentication';

const fakeAuthProvider: IAuthentication = {
  authenticate: async () => ({ Authorization: 'Bearer test-token' }),
};

function makeMetric(overrides: Partial<TelemetryMetric> = {}): TelemetryMetric {
  return {
    metricType: 'connection',
    timestamp: Date.now(),
    sessionId: 'session-1',
    ...overrides,
  };
}

function makeOkResponse() {
  return Promise.resolve({ ok: true, status: 200, statusText: 'OK', text: () => Promise.resolve('') });
}

function makeErrorResponse(status: number, statusText: string) {
  return Promise.resolve({ ok: false, status, statusText, text: () => Promise.resolve('') });
}

describe('DatabricksTelemetryExporter', () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
    clock = sinon.useFakeTimers();
  });

  afterEach(() => {
    clock.restore();
    sinon.restore();
  });

  describe('export() - basic', () => {
    it('should return immediately for empty metrics array', async () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([]);

      expect(sendRequestStub.called).to.be.false;
    });

    it('should call sendRequest with correct endpoint for authenticated export', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: true } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      expect(sendRequestStub.calledOnce).to.be.true;
      const url = sendRequestStub.firstCall.args[0] as string;
      expect(url).to.include('telemetry-ext');
      expect(url).to.include('https://');
    });

    it('should call sendRequest with unauthenticated endpoint when configured', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: false } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      const url = sendRequestStub.firstCall.args[0] as string;
      expect(url).to.include('telemetry-unauth');
    });

    it('should preserve host protocol if already set', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: true } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'https://host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      const url = sendRequestStub.firstCall.args[0] as string;
      expect(url).to.equal('https://host.example.com/telemetry-ext');
    });

    it('should never throw even when sendRequest fails', async () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      sinon.stub(exporter as any, 'sendRequest').rejects(new Error('network error'));

      let threw = false;
      try {
        await exporter.export([makeMetric()]);
      } catch {
        threw = true;
      }
      expect(threw).to.be.false;
    });
  });

  describe('export() - retry logic', () => {
    it('should retry on retryable HTTP errors (503)', async () => {
      const context = new ClientContextStub({ telemetryMaxRetries: 2 } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      // Fail twice with 503, then succeed
      const sendRequestStub = sinon
        .stub(exporter as any, 'sendRequest')
        .onFirstCall()
        .returns(makeErrorResponse(503, 'Service Unavailable'))
        .onSecondCall()
        .returns(makeErrorResponse(503, 'Service Unavailable'))
        .onThirdCall()
        .returns(makeOkResponse());

      // Advance fake timers automatically for sleep calls
      const exportPromise = exporter.export([makeMetric()]);
      await clock.runAllAsync();
      await exportPromise;

      expect(sendRequestStub.callCount).to.equal(3);
    });

    it('should not retry on terminal HTTP errors (400)', async () => {
      const context = new ClientContextStub({ telemetryMaxRetries: 3 } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeErrorResponse(400, 'Bad Request'));

      await exporter.export([makeMetric()]);

      // Only one call — no retry on terminal error
      expect(sendRequestStub.callCount).to.equal(1);
    });

    it('should not retry on terminal HTTP errors (401)', async () => {
      const context = new ClientContextStub({ telemetryMaxRetries: 3 } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon
        .stub(exporter as any, 'sendRequest')
        .returns(makeErrorResponse(401, 'Unauthorized'));

      await exporter.export([makeMetric()]);

      expect(sendRequestStub.callCount).to.equal(1);
    });

    it('should give up after maxRetries are exhausted', async () => {
      const context = new ClientContextStub({ telemetryMaxRetries: 2 } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon
        .stub(exporter as any, 'sendRequest')
        .returns(makeErrorResponse(503, 'Service Unavailable'));

      const exportPromise = exporter.export([makeMetric()]);
      await clock.runAllAsync();
      await exportPromise;

      // 1 initial + 2 retries = 3 total calls
      expect(sendRequestStub.callCount).to.equal(3);
    });
  });

  describe('export() - circuit breaker integration', () => {
    it('should drop telemetry when circuit breaker is OPEN', async () => {
      // maxRetries: 0 avoids sleep delays; failureThreshold: 1 trips the breaker on first failure
      const context = new ClientContextStub({ telemetryMaxRetries: 0 } as any);
      const registry = new CircuitBreakerRegistry(context);
      registry.getCircuitBreaker('host.example.com', { failureThreshold: 1 });
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon
        .stub(exporter as any, 'sendRequest')
        .returns(makeErrorResponse(503, 'Service Unavailable'));

      // Trip the circuit breaker (1 non-retryable-pathway failure is enough)
      await exporter.export([makeMetric()]);
      sendRequestStub.reset();

      // Now circuit is OPEN, export should be dropped without calling sendRequest
      await exporter.export([makeMetric()]);

      expect(sendRequestStub.called).to.be.false;
    });

    it('should log at debug level when circuit is OPEN', async () => {
      const context = new ClientContextStub({ telemetryMaxRetries: 0 } as any);
      const logSpy = sinon.spy((context as any).logger, 'log');
      const registry = new CircuitBreakerRegistry(context);
      registry.getCircuitBreaker('host.example.com', { failureThreshold: 1 });
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      sinon.stub(exporter as any, 'sendRequest').returns(makeErrorResponse(503, 'Service Unavailable'));

      await exporter.export([makeMetric()]);
      logSpy.resetHistory();

      await exporter.export([makeMetric()]);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/Circuit breaker OPEN/))).to.be.true;
    });
  });

  describe('export() - payload format', () => {
    it('should send POST request with JSON content-type', async () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      const options = sendRequestStub.firstCall.args[1] as any;
      expect(options.method).to.equal('POST');
      expect(options.headers['Content-Type']).to.equal('application/json');
    });

    it('should include protoLogs in payload body', async () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric(), makeMetric()]);

      const body = JSON.parse((sendRequestStub.firstCall.args[1] as any).body);
      expect(body.protoLogs).to.be.an('array').with.length(2);
      expect(body.items).to.be.an('array').that.is.empty;
      expect(body.uploadTime).to.be.a('number');
    });
  });

  describe('logging level compliance', () => {
    it('should only log at debug level', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy((context as any).logger, 'log');
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      sinon.stub(exporter as any, 'sendRequest').rejects(new Error('something went wrong'));

      const exportPromise = exporter.export([makeMetric()]);
      await clock.runAllAsync();
      await exportPromise;

      expect(logSpy.neverCalledWith(LogLevel.error, sinon.match.any)).to.be.true;
      // Note: circuit breaker logs at warn level when transitioning to OPEN, which is expected
    });
  });

  describe('Authorization header flow', () => {
    it('sends Authorization header returned by the auth provider on authenticated export', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: true } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      const init = sendRequestStub.firstCall.args[1] as any;
      expect(init.headers.Authorization).to.equal('Bearer test-token');
    });

    it('drops the batch when authenticated export is requested but auth returns no header', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: true, telemetryMaxRetries: 0 } as any);
      const registry = new CircuitBreakerRegistry(context);
      const emptyAuth = { authenticate: async () => ({}) };
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, emptyAuth as any);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      expect(sendRequestStub.called).to.be.false;
    });

    it('warns exactly once across consecutive auth-missing drops', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: true, telemetryMaxRetries: 0 } as any);
      const logSpy = sinon.spy((context as any).logger, 'log');
      const registry = new CircuitBreakerRegistry(context);
      const emptyAuth = { authenticate: async () => ({}) };
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, emptyAuth as any);
      sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);
      await exporter.export([makeMetric()]);
      await exporter.export([makeMetric()]);

      const warnCalls = logSpy
        .getCalls()
        .filter((c) => c.args[0] === LogLevel.warn && /Authorization/.test(String(c.args[1])));
      expect(warnCalls.length).to.equal(1);
    });

    it('re-arms the auth-missing warn after a successful export', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: true, telemetryMaxRetries: 0 } as any);
      const logSpy = sinon.spy((context as any).logger, 'log');
      const registry = new CircuitBreakerRegistry(context);
      let headers: Record<string, string> = {};
      const toggleAuth = { authenticate: async () => headers };
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, toggleAuth as any);
      sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]); // warns once
      headers = { Authorization: 'Bearer recovered' };
      await exporter.export([makeMetric()]); // success → re-arms
      headers = {};
      await exporter.export([makeMetric()]); // warns again

      const warnCalls = logSpy
        .getCalls()
        .filter((c) => c.args[0] === LogLevel.warn && /Authorization/.test(String(c.args[1])));
      expect(warnCalls.length).to.equal(2);
    });
  });

  describe('unauthenticated endpoint privacy', () => {
    it('omits workspace_id, session_id, statement_id from unauth payload', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: false } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([
        makeMetric({
          metricType: 'connection',
          sessionId: 'session-xyz',
          statementId: 'stmt-abc',
          workspaceId: 'ws-123',
        } as any),
      ]);

      const body = JSON.parse((sendRequestStub.firstCall.args[1] as any).body);
      const log = JSON.parse(body.protoLogs[0]);
      expect(log.workspace_id).to.be.undefined;
      expect(log.entry.sql_driver_log.session_id).to.be.undefined;
      expect(log.entry.sql_driver_log.sql_statement_id).to.be.undefined;
    });

    it('omits system_configuration from unauth payload', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: false } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([
        makeMetric({
          metricType: 'connection',
          driverConfig: {
            driverVersion: '1.x',
            driverName: 'nodejs-sql-driver',
            nodeVersion: '20.0',
            platform: 'linux',
            osVersion: '5.0',
            osArch: 'x64',
            runtimeVendor: 'v8',
            localeName: 'en_US',
            charSetEncoding: 'UTF-8',
            processName: '/home/alice/worker.js',
          },
        } as any),
      ]);

      const body = JSON.parse((sendRequestStub.firstCall.args[1] as any).body);
      const log = JSON.parse(body.protoLogs[0]);
      expect(log.entry.sql_driver_log.system_configuration).to.be.undefined;
    });

    it('strips userAgentEntry from User-Agent on unauth path', async () => {
      const context = new ClientContextStub({
        telemetryAuthenticatedExport: false,
        userAgentEntry: 'MyTenantApp/1.2.3',
      } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      const ua = (sendRequestStub.firstCall.args[1] as any).headers['User-Agent'];
      expect(ua).to.not.include('MyTenantApp');
    });

    it('blanks stack_trace on unauth error metrics', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: false } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([
        makeMetric({
          metricType: 'error',
          errorName: 'SomeError',
          errorMessage: 'Bearer leaked-token in the message',
          errorStack: 'Error: leak\n    at fn (dapi0123456789abcdef)',
        } as any),
      ]);

      const body = JSON.parse((sendRequestStub.firstCall.args[1] as any).body);
      const log = JSON.parse(body.protoLogs[0]);
      expect(log.entry.sql_driver_log.error_info.stack_trace).to.equal('');
      expect(log.entry.sql_driver_log.error_info.error_name).to.equal('SomeError');
    });
  });

  describe('errorStack flow (authenticated)', () => {
    it('redacts Bearer tokens in stack_trace before export', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: true } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([
        makeMetric({
          metricType: 'error',
          errorName: 'AuthError',
          errorMessage: 'ignored because errorStack is preferred',
          errorStack: 'Error: boom\n    at Bearer leaked-bearer-token',
        } as any),
      ]);

      const body = JSON.parse((sendRequestStub.firstCall.args[1] as any).body);
      const log = JSON.parse(body.protoLogs[0]);
      const stack = log.entry.sql_driver_log.stack_trace ?? log.entry.sql_driver_log.error_info?.stack_trace;
      expect(stack).to.include('<REDACTED>');
      expect(stack).to.not.include('leaked-bearer-token');
    });
  });

  describe('host validation', () => {
    it('drops the batch when host fails validation (malformed)', async () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, '//attacker.com', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      expect(sendRequestStub.called).to.be.false;
    });

    it('drops the batch when host is loopback', async () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, '127.0.0.1', registry, fakeAuthProvider);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      expect(sendRequestStub.called).to.be.false;
    });
  });

  describe('dispose()', () => {
    it('removes the per-host circuit breaker from the registry', () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);

      expect(registry.getAllBreakers().has('host.example.com')).to.be.true;

      exporter.dispose();

      expect(registry.getAllBreakers().has('host.example.com')).to.be.false;
    });

    it('is idempotent', () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry, fakeAuthProvider);

      exporter.dispose();
      expect(() => exporter.dispose()).to.not.throw();
    });
  });
});
