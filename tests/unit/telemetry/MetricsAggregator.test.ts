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
import MetricsAggregator from '../../../lib/telemetry/MetricsAggregator';
import DatabricksTelemetryExporter from '../../../lib/telemetry/DatabricksTelemetryExporter';
import { TelemetryEvent, TelemetryEventType } from '../../../lib/telemetry/types';
import ClientContextStub from '../.stubs/ClientContextStub';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

function makeExporterStub(): sinon.SinonStubbedInstance<DatabricksTelemetryExporter> {
  return sinon.createStubInstance(DatabricksTelemetryExporter);
}

function connectionEvent(overrides: Partial<TelemetryEvent> = {}): TelemetryEvent {
  return {
    eventType: TelemetryEventType.CONNECTION_OPEN,
    timestamp: Date.now(),
    sessionId: 'session-1',
    workspaceId: 'workspace-1',
    driverConfig: {} as any,
    ...overrides,
  };
}

function statementEvent(type: TelemetryEventType, overrides: Partial<TelemetryEvent> = {}): TelemetryEvent {
  return {
    eventType: type,
    timestamp: Date.now(),
    sessionId: 'session-1',
    statementId: 'stmt-1',
    ...overrides,
  };
}

describe('MetricsAggregator', () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
    clock = sinon.useFakeTimers();
  });

  afterEach(() => {
    clock.restore();
  });

  describe('processEvent() - connection events', () => {
    it('should emit connection events immediately without waiting for completeStatement', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.processEvent(connectionEvent());

      // flush happens when batchSize reached or timer fires — force it
      aggregator.flush();

      expect(exporter.export.calledOnce).to.be.true;
      const metrics = exporter.export.firstCall.args[0];
      expect(metrics[0].metricType).to.equal('connection');
    });
  });

  describe('processEvent() - statement events', () => {
    it('should buffer statement events until completeStatement is called', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.processEvent(statementEvent(TelemetryEventType.STATEMENT_START));
      aggregator.processEvent(statementEvent(TelemetryEventType.STATEMENT_COMPLETE, { latencyMs: 100 }));

      // Not yet flushed
      expect(exporter.export.called).to.be.false;

      aggregator.completeStatement('stmt-1');
      aggregator.flush();

      expect(exporter.export.calledOnce).to.be.true;
      const metrics = exporter.export.firstCall.args[0];
      expect(metrics[0].metricType).to.equal('statement');
      expect(metrics[0].statementId).to.equal('stmt-1');
    });

    it('should track chunk events and accumulate totals', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.processEvent(statementEvent(TelemetryEventType.STATEMENT_START));
      aggregator.processEvent(statementEvent(TelemetryEventType.CLOUDFETCH_CHUNK, { bytes: 100, compressed: true }));
      aggregator.processEvent(statementEvent(TelemetryEventType.CLOUDFETCH_CHUNK, { bytes: 200, compressed: true }));

      aggregator.completeStatement('stmt-1');
      aggregator.flush();

      const metrics = exporter.export.firstCall.args[0];
      const stmtMetric = metrics[0];
      expect(stmtMetric.chunkCount).to.equal(2);
      expect(stmtMetric.bytesDownloaded).to.equal(300);
    });

    it('should do nothing for completeStatement with unknown statementId', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      expect(() => aggregator.completeStatement('unknown-stmt')).to.not.throw();
    });

    it('aggregates chunk timing — initial is first-seen, slowest is max, sum accumulates', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.processEvent(statementEvent(TelemetryEventType.STATEMENT_START));
      aggregator.processEvent(statementEvent(TelemetryEventType.CLOUDFETCH_CHUNK, { latencyMs: 100, bytes: 10 }));
      aggregator.processEvent(statementEvent(TelemetryEventType.CLOUDFETCH_CHUNK, { latencyMs: 250, bytes: 10 }));
      aggregator.processEvent(statementEvent(TelemetryEventType.CLOUDFETCH_CHUNK, { latencyMs: 75, bytes: 10 }));

      aggregator.completeStatement('stmt-1');
      aggregator.flush();

      const stmtMetric = exporter.export.firstCall.args[0][0];
      expect(stmtMetric.chunkInitialLatencyMs).to.equal(100);
      expect(stmtMetric.chunkSlowestLatencyMs).to.equal(250);
      expect(stmtMetric.chunkSumLatencyMs).to.equal(425);
    });

    it('chunks with non-positive latency do not contribute to timing fields', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.processEvent(statementEvent(TelemetryEventType.STATEMENT_START));
      // latency=0 (cached/prefetched page) — must be ignored entirely.
      aggregator.processEvent(statementEvent(TelemetryEventType.CLOUDFETCH_CHUNK, { latencyMs: 0, bytes: 5 }));
      // latency undefined — emitter didn't set it, must be ignored.
      aggregator.processEvent(statementEvent(TelemetryEventType.CLOUDFETCH_CHUNK, { bytes: 5 }));
      // First *positive* latency wins for `initial`, even though earlier chunks already arrived.
      aggregator.processEvent(statementEvent(TelemetryEventType.CLOUDFETCH_CHUNK, { latencyMs: 60, bytes: 5 }));

      aggregator.completeStatement('stmt-1');
      aggregator.flush();

      const stmtMetric = exporter.export.firstCall.args[0][0];
      expect(stmtMetric.chunkInitialLatencyMs).to.equal(60);
      expect(stmtMetric.chunkSlowestLatencyMs).to.equal(60);
      expect(stmtMetric.chunkSumLatencyMs).to.equal(60);
      expect(stmtMetric.chunkCount).to.equal(3); // chunkCount counts all chunks regardless of latency
    });
  });

  describe('processEvent() - CONNECTION_CLOSE', () => {
    it('emits a DELETE_SESSION connection metric immediately', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.processEvent(connectionEvent({ eventType: TelemetryEventType.CONNECTION_CLOSE, latencyMs: 42 }));
      aggregator.flush();

      expect(exporter.export.calledOnce).to.be.true;
      const metric = exporter.export.firstCall.args[0][0];
      expect(metric.metricType).to.equal('connection');
      expect(metric.operationType).to.equal('DELETE_SESSION');
      expect(metric.latencyMs).to.equal(42);
    });

    it('CONNECTION_OPEN and CONNECTION_CLOSE produce distinct operation types in the same batch', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.processEvent(connectionEvent({ eventType: TelemetryEventType.CONNECTION_OPEN, latencyMs: 100 }));
      aggregator.processEvent(connectionEvent({ eventType: TelemetryEventType.CONNECTION_CLOSE, latencyMs: 5 }));
      aggregator.flush();

      const batch = exporter.export.firstCall.args[0];
      expect(batch).to.have.lengthOf(2);
      expect(batch.map((m: any) => m.operationType)).to.deep.equal(['CREATE_SESSION', 'DELETE_SESSION']);
    });
  });

  describe('processEvent() - error events', () => {
    it('should flush immediately for terminal errors', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      const errEvent: TelemetryEvent = {
        eventType: TelemetryEventType.ERROR,
        timestamp: Date.now(),
        sessionId: 'session-1',
        errorName: 'AuthError',
        errorMessage: 'auth failed',
        isTerminal: true,
      };

      aggregator.processEvent(errEvent);

      // Terminal error should trigger an immediate flush
      expect(exporter.export.called).to.be.true;
      const metrics = exporter.export.firstCall.args[0];
      expect(metrics[0].metricType).to.equal('error');
    });

    it('should buffer retryable errors until statement completes', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      const retryableErr: TelemetryEvent = {
        eventType: TelemetryEventType.ERROR,
        timestamp: Date.now(),
        sessionId: 'session-1',
        statementId: 'stmt-1',
        errorName: 'NetworkError',
        errorMessage: 'timeout',
        isTerminal: false,
      };

      aggregator.processEvent(retryableErr);
      // Not flushed yet
      expect(exporter.export.called).to.be.false;

      aggregator.completeStatement('stmt-1');
      aggregator.flush();

      expect(exporter.export.called).to.be.true;
    });
  });

  describe('flush() - batch size trigger', () => {
    it('should flush when batchSize is reached', () => {
      const context = new ClientContextStub({ telemetryBatchSize: 3 } as any);
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      // Add 2 connection events — no flush yet
      aggregator.processEvent(connectionEvent({ sessionId: 's1' }));
      aggregator.processEvent(connectionEvent({ sessionId: 's2' }));
      expect(exporter.export.called).to.be.false;

      // 3rd event reaches batchSize
      aggregator.processEvent(connectionEvent({ sessionId: 's3' }));
      expect(exporter.export.calledOnce).to.be.true;
    });
  });

  describe('flush() - periodic timer', () => {
    it('should flush periodically based on flushIntervalMs', () => {
      const context = new ClientContextStub({ telemetryFlushIntervalMs: 5000 } as any);
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.processEvent(connectionEvent());
      expect(exporter.export.called).to.be.false;

      clock.tick(5000);

      expect(exporter.export.calledOnce).to.be.true;
    });

    it('should not flush if there are no pending metrics', () => {
      const context = new ClientContextStub({ telemetryFlushIntervalMs: 5000 } as any);
      const exporter = makeExporterStub();
      new MetricsAggregator(context, exporter as any);

      clock.tick(5000);

      expect(exporter.export.called).to.be.false;
    });
  });

  describe('maxPendingMetrics bound', () => {
    it('should drop oldest metrics when buffer exceeds maxPendingMetrics', () => {
      const context = new ClientContextStub({ telemetryMaxPendingMetrics: 3, telemetryBatchSize: 1000 } as any);
      const logSpy = sinon.spy((context as any).logger, 'log');
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      // Add 5 events — should be capped at 3
      for (let i = 0; i < 5; i++) {
        aggregator.processEvent(connectionEvent({ sessionId: `s${i}` }));
      }

      aggregator.flush();

      const metrics = exporter.export.firstCall.args[0];
      expect(metrics.length).to.equal(3);
      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/Dropped/))).to.be.true;

      logSpy.restore();
    });
  });

  describe('close()', () => {
    it('should flush remaining metrics on close', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.processEvent(connectionEvent());
      aggregator.close();

      expect(exporter.export.called).to.be.true;
    });

    it('should stop the flush timer on close', () => {
      const context = new ClientContextStub({ telemetryFlushIntervalMs: 5000 } as any);
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.close();
      exporter.export.reset();

      // Advance time — timer should no longer fire
      clock.tick(10000);
      aggregator.processEvent(connectionEvent());
      // Timer stopped, so no auto-flush
      expect(exporter.export.called).to.be.false;
    });

    it('should complete any in-progress statements on close', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.processEvent(statementEvent(TelemetryEventType.STATEMENT_START));
      aggregator.close();

      expect(exporter.export.called).to.be.true;
      const metrics = exporter.export.firstCall.args[0];
      expect(metrics[0].metricType).to.equal('statement');
    });

    it('awaits in-flight export before resolving — prevents process.exit truncation', async () => {
      clock.restore();
      const context = new ClientContextStub();
      let resolveExport!: () => void;
      const pendingExport = new Promise<void>((r) => {
        resolveExport = r;
      });
      const exporter: any = { export: sinon.stub().returns(pendingExport) };
      const aggregator = new MetricsAggregator(context, exporter);

      aggregator.processEvent(connectionEvent());

      const done = aggregator.close();
      expect(done).to.be.an.instanceof(Promise);

      let resolved = false;
      done.then(() => {
        resolved = true;
      });
      await Promise.resolve();
      await Promise.resolve();
      expect(resolved, 'close() should wait for exporter promise before resolving').to.be.false;

      resolveExport();
      await done;
      expect(resolved).to.be.true;
    });

    it('does not resurrect the flush timer after close', async () => {
      clock.restore();
      const context = new ClientContextStub({ telemetryBatchSize: 1 } as any);
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      aggregator.processEvent(statementEvent(TelemetryEventType.STATEMENT_START));
      await aggregator.close();

      expect((aggregator as any).flushTimer, 'flushTimer should be null after close').to.equal(null);
      expect((aggregator as any).closed).to.be.true;
    });
  });

  describe('exception swallowing', () => {
    it('should never throw from processEvent', () => {
      const context = new ClientContextStub();
      const exporter = makeExporterStub();
      const aggregator = new MetricsAggregator(context, exporter as any);

      expect(() =>
        aggregator.processEvent({ eventType: 'unknown.event' as any, timestamp: Date.now() }),
      ).to.not.throw();
    });
  });
});
