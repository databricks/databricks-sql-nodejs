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

function makeMetric(overrides: Partial<TelemetryMetric> = {}): TelemetryMetric {
  return {
    metricType: 'connection',
    timestamp: Date.now(),
    sessionId: 'session-1',
    ...overrides,
  };
}

function makeOkResponse() {
  return Promise.resolve({ ok: true, status: 200, statusText: 'OK' });
}

function makeErrorResponse(status: number, statusText: string) {
  return Promise.resolve({ ok: false, status, statusText });
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
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([]);

      expect(sendRequestStub.called).to.be.false;
    });

    it('should call sendRequest with correct endpoint for authenticated export', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: true } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
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
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      const url = sendRequestStub.firstCall.args[0] as string;
      expect(url).to.include('telemetry-unauth');
    });

    it('should preserve host protocol if already set', async () => {
      const context = new ClientContextStub({ telemetryAuthenticatedExport: true } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'https://host.example.com', registry);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      const url = sendRequestStub.firstCall.args[0] as string;
      expect(url).to.equal('https://host.example.com/telemetry-ext');
    });

    it('should never throw even when sendRequest fails', async () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
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
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
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
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeErrorResponse(400, 'Bad Request'));

      await exporter.export([makeMetric()]);

      // Only one call — no retry on terminal error
      expect(sendRequestStub.callCount).to.equal(1);
    });

    it('should not retry on terminal HTTP errors (401)', async () => {
      const context = new ClientContextStub({ telemetryMaxRetries: 3 } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
      const sendRequestStub = sinon
        .stub(exporter as any, 'sendRequest')
        .returns(makeErrorResponse(401, 'Unauthorized'));

      await exporter.export([makeMetric()]);

      expect(sendRequestStub.callCount).to.equal(1);
    });

    it('should give up after maxRetries are exhausted', async () => {
      const context = new ClientContextStub({ telemetryMaxRetries: 2 } as any);
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
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
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
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
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
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
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
      const sendRequestStub = sinon.stub(exporter as any, 'sendRequest').returns(makeOkResponse());

      await exporter.export([makeMetric()]);

      const options = sendRequestStub.firstCall.args[1] as any;
      expect(options.method).to.equal('POST');
      expect(options.headers['Content-Type']).to.equal('application/json');
    });

    it('should include protoLogs in payload body', async () => {
      const context = new ClientContextStub();
      const registry = new CircuitBreakerRegistry(context);
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
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
      const exporter = new DatabricksTelemetryExporter(context, 'host.example.com', registry);
      sinon.stub(exporter as any, 'sendRequest').rejects(new Error('something went wrong'));

      const exportPromise = exporter.export([makeMetric()]);
      await clock.runAllAsync();
      await exportPromise;

      expect(logSpy.neverCalledWith(LogLevel.error, sinon.match.any)).to.be.true;
      expect(logSpy.neverCalledWith(LogLevel.warn, sinon.match.any)).to.be.true;
    });
  });
});
