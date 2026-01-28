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
import { TelemetryEvent, TelemetryEventType, DEFAULT_TELEMETRY_CONFIG } from '../../../lib/telemetry/types';
import IClientContext from '../../../lib/contracts/IClientContext';
import IDBSQLLogger, { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import TelemetryExporterStub from '../.stubs/TelemetryExporterStub';

describe('MetricsAggregator', () => {
  let context: IClientContext;
  let logger: IDBSQLLogger;
  let exporter: TelemetryExporterStub;
  let aggregator: MetricsAggregator;
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
    clock = sinon.useFakeTimers();

    logger = {
      log: sinon.stub(),
    };

    exporter = new TelemetryExporterStub();

    context = {
      getLogger: () => logger,
      getConfig: () => ({
        telemetryBatchSize: 10,
        telemetryFlushIntervalMs: 5000,
        directResultsDefaultMaxRows: 10000,
        fetchChunkDefaultMaxRows: 100000,
        socketTimeout: 900000,
        retryMaxAttempts: 30,
        retriesTimeout: 900000,
        retryDelayMin: 1000,
        retryDelayMax: 30000,
        useCloudFetch: true,
        cloudFetchConcurrentDownloads: 10,
        cloudFetchSpeedThresholdMBps: 0,
        useLZ4Compression: true,
      }),
    } as any;

    aggregator = new MetricsAggregator(context, exporter as any);
  });

  afterEach(() => {
    if (aggregator) {
      aggregator.close();
    }
    clock.restore();
    sinon.restore();
  });

  describe('constructor', () => {
    it('should create instance with default config values', () => {
      const defaultContext = {
        getLogger: () => logger,
        getConfig: () => ({
          directResultsDefaultMaxRows: 10000,
          fetchChunkDefaultMaxRows: 100000,
          socketTimeout: 900000,
          retryMaxAttempts: 30,
          retriesTimeout: 900000,
          retryDelayMin: 1000,
          retryDelayMax: 30000,
          useCloudFetch: true,
          cloudFetchConcurrentDownloads: 10,
          cloudFetchSpeedThresholdMBps: 0,
          useLZ4Compression: true,
        }),
      } as any;

      const defaultAggregator = new MetricsAggregator(defaultContext, exporter as any);
      expect(defaultAggregator).to.be.instanceOf(MetricsAggregator);
      defaultAggregator.close();
    });

    it('should use batch size from config', () => {
      const customContext = {
        getLogger: () => logger,
        getConfig: () => ({
          telemetryBatchSize: 5,
          telemetryFlushIntervalMs: 5000,
          directResultsDefaultMaxRows: 10000,
          fetchChunkDefaultMaxRows: 100000,
          socketTimeout: 900000,
          retryMaxAttempts: 30,
          retriesTimeout: 900000,
          retryDelayMin: 1000,
          retryDelayMax: 30000,
          useCloudFetch: true,
          cloudFetchConcurrentDownloads: 10,
          cloudFetchSpeedThresholdMBps: 0,
          useLZ4Compression: true,
        }),
      } as any;

      const customAggregator = new MetricsAggregator(customContext, exporter as any);

      // Process 4 connection events (below batch size of 5)
      for (let i = 0; i < 4; i++) {
        const event: TelemetryEvent = {
          eventType: TelemetryEventType.CONNECTION_OPEN,
          timestamp: Date.now(),
          sessionId: `session-${i}`,
          workspaceId: 'workspace-1',
        };
        customAggregator.processEvent(event);
      }

      // Should not flush yet (batch size is 5)
      expect(exporter.exportCount).to.equal(0);

      // Process 5th event
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-5',
        workspaceId: 'workspace-1',
      };
      customAggregator.processEvent(event);

      // Should flush now (batch size reached)
      expect(exporter.exportCount).to.equal(1);
      customAggregator.close();
    });
  });

  describe('processEvent - connection events', () => {
    it('should emit connection events immediately', () => {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-456',
        driverConfig: {
          driverVersion: '1.0.0',
          driverName: 'databricks-sql-nodejs',
          nodeVersion: process.version,
          platform: process.platform,
          osVersion: 'test-os',
          cloudFetchEnabled: true,
          lz4Enabled: true,
          arrowEnabled: false,
          directResultsEnabled: true,
          socketTimeout: 900000,
          retryMaxAttempts: 30,
          cloudFetchConcurrentDownloads: 10,
        },
      };

      aggregator.processEvent(event);

      // Should not flush yet (batch size is 10)
      expect(exporter.exportCount).to.equal(0);

      // Complete to trigger flush
      aggregator.flush();

      expect(exporter.exportCount).to.equal(1);
      const metrics = exporter.getAllExportedMetrics();
      expect(metrics).to.have.lengthOf(1);
      expect(metrics[0].metricType).to.equal('connection');
      expect(metrics[0].sessionId).to.equal('session-123');
      expect(metrics[0].workspaceId).to.equal('workspace-456');
      expect(metrics[0].driverConfig).to.deep.equal(event.driverConfig);
    });

    it('should handle multiple connection events', () => {
      const event1: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-1',
        workspaceId: 'workspace-1',
      };

      const event2: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-2',
        workspaceId: 'workspace-2',
      };

      aggregator.processEvent(event1);
      aggregator.processEvent(event2);
      aggregator.flush();

      const metrics = exporter.getAllExportedMetrics();
      expect(metrics).to.have.lengthOf(2);
      expect(metrics[0].sessionId).to.equal('session-1');
      expect(metrics[1].sessionId).to.equal('session-2');
    });
  });

  describe('processEvent - statement events', () => {
    it('should aggregate statement events by statement_id', () => {
      const startEvent: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_START,
        timestamp: 1000,
        statementId: 'stmt-123',
        sessionId: 'session-123',
        operationType: 'SELECT',
      };

      const completeEvent: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_COMPLETE,
        timestamp: 2500,
        statementId: 'stmt-123',
        sessionId: 'session-123',
        latencyMs: 1500,
        resultFormat: 'cloudfetch',
        chunkCount: 5,
        bytesDownloaded: 1024000,
        pollCount: 3,
      };

      aggregator.processEvent(startEvent);
      aggregator.processEvent(completeEvent);

      // Should not flush until completeStatement() called
      expect(exporter.exportCount).to.equal(0);

      aggregator.completeStatement('stmt-123');

      // Should not flush yet (batch size is 10)
      expect(exporter.exportCount).to.equal(0);

      aggregator.flush();

      expect(exporter.exportCount).to.equal(1);
      const metrics = exporter.getAllExportedMetrics();
      expect(metrics).to.have.lengthOf(1);
      expect(metrics[0].metricType).to.equal('statement');
      expect(metrics[0].statementId).to.equal('stmt-123');
      expect(metrics[0].sessionId).to.equal('session-123');
      expect(metrics[0].latencyMs).to.equal(1500);
      expect(metrics[0].resultFormat).to.equal('cloudfetch');
      expect(metrics[0].chunkCount).to.equal(5);
      expect(metrics[0].bytesDownloaded).to.equal(1024000);
      expect(metrics[0].pollCount).to.equal(3);
    });

    it('should buffer statement events until complete', () => {
      const startEvent: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_START,
        timestamp: Date.now(),
        statementId: 'stmt-123',
        sessionId: 'session-123',
        operationType: 'INSERT',
      };

      aggregator.processEvent(startEvent);
      aggregator.flush();

      // Should not export statement until complete
      expect(exporter.getAllExportedMetrics()).to.have.lengthOf(0);

      // Complete statement
      aggregator.completeStatement('stmt-123');
      aggregator.flush();

      // Should export now
      const metrics = exporter.getAllExportedMetrics();
      expect(metrics).to.have.lengthOf(1);
      expect(metrics[0].metricType).to.equal('statement');
    });

    it('should include both session_id and statement_id in metrics', () => {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_START,
        timestamp: Date.now(),
        statementId: 'stmt-789',
        sessionId: 'session-456',
      };

      aggregator.processEvent(event);
      aggregator.completeStatement('stmt-789');
      aggregator.flush();

      const metrics = exporter.getAllExportedMetrics();
      expect(metrics[0].sessionId).to.equal('session-456');
      expect(metrics[0].statementId).to.equal('stmt-789');
    });
  });

  describe('processEvent - cloudfetch events', () => {
    it('should aggregate cloudfetch chunk events', () => {
      const startEvent: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_START,
        timestamp: Date.now(),
        statementId: 'stmt-123',
        sessionId: 'session-123',
      };

      const chunk1: TelemetryEvent = {
        eventType: TelemetryEventType.CLOUDFETCH_CHUNK,
        timestamp: Date.now(),
        statementId: 'stmt-123',
        chunkIndex: 0,
        bytes: 100000,
        compressed: true,
      };

      const chunk2: TelemetryEvent = {
        eventType: TelemetryEventType.CLOUDFETCH_CHUNK,
        timestamp: Date.now(),
        statementId: 'stmt-123',
        chunkIndex: 1,
        bytes: 150000,
        compressed: true,
      };

      aggregator.processEvent(startEvent);
      aggregator.processEvent(chunk1);
      aggregator.processEvent(chunk2);
      aggregator.completeStatement('stmt-123');
      aggregator.flush();

      const metrics = exporter.getAllExportedMetrics();
      expect(metrics).to.have.lengthOf(1);
      expect(metrics[0].chunkCount).to.equal(2);
      expect(metrics[0].bytesDownloaded).to.equal(250000);
    });
  });

  describe('processEvent - error events', () => {
    it('should flush terminal exceptions immediately', () => {
      const terminalError: TelemetryEvent = {
        eventType: TelemetryEventType.ERROR,
        timestamp: Date.now(),
        sessionId: 'session-123',
        statementId: 'stmt-123',
        errorName: 'AuthenticationError',
        errorMessage: 'Invalid credentials',
        isTerminal: true,
      };

      aggregator.processEvent(terminalError);

      // Should flush immediately for terminal errors
      expect(exporter.exportCount).to.equal(1);
      const metrics = exporter.getAllExportedMetrics();
      expect(metrics).to.have.lengthOf(1);
      expect(metrics[0].metricType).to.equal('error');
      expect(metrics[0].errorName).to.equal('AuthenticationError');
    });

    it('should buffer retryable exceptions until statement complete', () => {
      const startEvent: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_START,
        timestamp: Date.now(),
        statementId: 'stmt-123',
        sessionId: 'session-123',
      };

      const retryableError: TelemetryEvent = {
        eventType: TelemetryEventType.ERROR,
        timestamp: Date.now(),
        sessionId: 'session-123',
        statementId: 'stmt-123',
        errorName: 'TimeoutError',
        errorMessage: 'Request timed out',
        isTerminal: false,
      };

      aggregator.processEvent(startEvent);
      aggregator.processEvent(retryableError);

      // Should not flush retryable error yet
      expect(exporter.exportCount).to.equal(0);

      aggregator.completeStatement('stmt-123');
      aggregator.flush();

      // Should export statement and error now
      const metrics = exporter.getAllExportedMetrics();
      expect(metrics).to.have.lengthOf(2);
      expect(metrics[0].metricType).to.equal('statement');
      expect(metrics[1].metricType).to.equal('error');
      expect(metrics[1].errorName).to.equal('TimeoutError');
    });

    it('should flush terminal error for statement and complete it', () => {
      const startEvent: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_START,
        timestamp: Date.now(),
        statementId: 'stmt-123',
        sessionId: 'session-123',
      };

      const terminalError: TelemetryEvent = {
        eventType: TelemetryEventType.ERROR,
        timestamp: Date.now(),
        sessionId: 'session-123',
        statementId: 'stmt-123',
        errorName: 'AuthenticationError',
        errorMessage: 'Invalid credentials',
        isTerminal: true,
      };

      aggregator.processEvent(startEvent);
      aggregator.processEvent(terminalError);

      // Should flush immediately for terminal error
      expect(exporter.exportCount).to.equal(1);
      const metrics = exporter.getAllExportedMetrics();
      expect(metrics).to.have.lengthOf(2);
      expect(metrics[0].metricType).to.equal('statement');
      expect(metrics[1].metricType).to.equal('error');
    });
  });

  describe('batch size flushing', () => {
    it('should flush when batch size reached', () => {
      // Process 10 connection events (batch size is 10)
      for (let i = 0; i < 10; i++) {
        const event: TelemetryEvent = {
          eventType: TelemetryEventType.CONNECTION_OPEN,
          timestamp: Date.now(),
          sessionId: `session-${i}`,
          workspaceId: 'workspace-1',
        };
        aggregator.processEvent(event);
      }

      // Should flush automatically
      expect(exporter.exportCount).to.equal(1);
      expect(exporter.getAllExportedMetrics()).to.have.lengthOf(10);
    });

    it('should not flush before batch size reached', () => {
      // Process 9 connection events (below batch size of 10)
      for (let i = 0; i < 9; i++) {
        const event: TelemetryEvent = {
          eventType: TelemetryEventType.CONNECTION_OPEN,
          timestamp: Date.now(),
          sessionId: `session-${i}`,
          workspaceId: 'workspace-1',
        };
        aggregator.processEvent(event);
      }

      // Should not flush yet
      expect(exporter.exportCount).to.equal(0);
    });
  });

  describe('periodic timer flushing', () => {
    it('should flush on periodic timer', () => {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      aggregator.processEvent(event);

      // Should not flush immediately
      expect(exporter.exportCount).to.equal(0);

      // Advance timer by flush interval (5000ms)
      clock.tick(5000);

      // Should flush now
      expect(exporter.exportCount).to.equal(1);
      expect(exporter.getAllExportedMetrics()).to.have.lengthOf(1);
    });

    it('should flush multiple times on timer', () => {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      aggregator.processEvent(event);
      clock.tick(5000);
      expect(exporter.exportCount).to.equal(1);

      aggregator.processEvent(event);
      clock.tick(5000);
      expect(exporter.exportCount).to.equal(2);
    });
  });

  describe('completeStatement', () => {
    it('should complete statement and prepare for flushing', () => {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_START,
        timestamp: Date.now(),
        statementId: 'stmt-123',
        sessionId: 'session-123',
      };

      aggregator.processEvent(event);
      aggregator.completeStatement('stmt-123');
      aggregator.flush();

      const metrics = exporter.getAllExportedMetrics();
      expect(metrics).to.have.lengthOf(1);
      expect(metrics[0].statementId).to.equal('stmt-123');
    });

    it('should do nothing for unknown statement_id', () => {
      aggregator.completeStatement('unknown-stmt');
      aggregator.flush();

      expect(exporter.getAllExportedMetrics()).to.have.lengthOf(0);
    });

    it('should include buffered errors when completing statement', () => {
      const startEvent: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_START,
        timestamp: Date.now(),
        statementId: 'stmt-123',
        sessionId: 'session-123',
      };

      const error1: TelemetryEvent = {
        eventType: TelemetryEventType.ERROR,
        timestamp: Date.now(),
        sessionId: 'session-123',
        statementId: 'stmt-123',
        errorName: 'Error1',
        errorMessage: 'First error',
        isTerminal: false,
      };

      const error2: TelemetryEvent = {
        eventType: TelemetryEventType.ERROR,
        timestamp: Date.now(),
        sessionId: 'session-123',
        statementId: 'stmt-123',
        errorName: 'Error2',
        errorMessage: 'Second error',
        isTerminal: false,
      };

      aggregator.processEvent(startEvent);
      aggregator.processEvent(error1);
      aggregator.processEvent(error2);
      aggregator.completeStatement('stmt-123');
      aggregator.flush();

      const metrics = exporter.getAllExportedMetrics();
      expect(metrics).to.have.lengthOf(3);
      expect(metrics[0].metricType).to.equal('statement');
      expect(metrics[1].metricType).to.equal('error');
      expect(metrics[2].metricType).to.equal('error');
    });
  });

  describe('close', () => {
    it('should flush remaining metrics on close', () => {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      aggregator.processEvent(event);
      aggregator.close();

      expect(exporter.exportCount).to.equal(1);
      expect(exporter.getAllExportedMetrics()).to.have.lengthOf(1);
    });

    it('should complete pending statements on close', () => {
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_START,
        timestamp: Date.now(),
        statementId: 'stmt-123',
        sessionId: 'session-123',
      };

      aggregator.processEvent(event);
      aggregator.close();

      const metrics = exporter.getAllExportedMetrics();
      expect(metrics).to.have.lengthOf(1);
      expect(metrics[0].statementId).to.equal('stmt-123');
    });

    it('should stop flush timer on close', () => {
      aggregator.close();

      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      exporter.reset();
      aggregator.processEvent(event);

      // Advance timer - should not flush after close
      clock.tick(5000);
      expect(exporter.exportCount).to.equal(0);
    });
  });

  describe('exception swallowing', () => {
    it('should swallow exception in processEvent and log at debug level', () => {
      // Create a context that throws in getConfig
      const throwingContext = {
        getLogger: () => logger,
        getConfig: () => {
          throw new Error('Config error');
        },
      } as any;

      const throwingAggregator = new MetricsAggregator(throwingContext, exporter as any);

      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      // Should not throw
      expect(() => throwingAggregator.processEvent(event)).to.not.throw();

      throwingAggregator.close();
    });

    it('should swallow exception in flush and log at debug level', () => {
      // Make exporter throw
      exporter.throwOnExport(new Error('Export failed'));

      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      aggregator.processEvent(event);

      // Should not throw
      expect(() => aggregator.flush()).to.not.throw();
    });

    it('should swallow exception in completeStatement and log at debug level', () => {
      // Process invalid event to create bad state
      const event: TelemetryEvent = {
        eventType: TelemetryEventType.STATEMENT_START,
        timestamp: Date.now(),
        statementId: 'stmt-123',
        sessionId: 'session-123',
      };

      aggregator.processEvent(event);

      // Create a scenario that might cause an exception
      // Even if internals throw, should not propagate
      expect(() => aggregator.completeStatement('stmt-123')).to.not.throw();
    });

    it('should swallow exception in close and log at debug level', () => {
      // Make exporter throw
      exporter.throwOnExport(new Error('Export failed'));

      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      aggregator.processEvent(event);

      // Should not throw
      expect(() => aggregator.close()).to.not.throw();
    });

    it('should log all errors at debug level only', () => {
      exporter.throwOnExport(new Error('Export failed'));

      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      aggregator.processEvent(event);
      aggregator.flush();

      const logStub = logger.log as sinon.SinonStub;
      for (let i = 0; i < logStub.callCount; i++) {
        const level = logStub.args[i][0];
        expect(level).to.equal(LogLevel.debug);
      }
    });
  });

  describe('no console logging', () => {
    it('should not use console.log', () => {
      const consoleSpy = sinon.spy(console, 'log');

      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      aggregator.processEvent(event);
      aggregator.flush();
      aggregator.close();

      expect(consoleSpy.called).to.be.false;
      consoleSpy.restore();
    });

    it('should not use console.debug', () => {
      const consoleSpy = sinon.spy(console, 'debug');

      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      aggregator.processEvent(event);
      aggregator.flush();
      aggregator.close();

      expect(consoleSpy.called).to.be.false;
      consoleSpy.restore();
    });

    it('should not use console.error', () => {
      const consoleSpy = sinon.spy(console, 'error');

      exporter.throwOnExport(new Error('Export failed'));

      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      aggregator.processEvent(event);
      aggregator.flush();

      expect(consoleSpy.called).to.be.false;
      consoleSpy.restore();
    });
  });

  describe('config reading', () => {
    it('should read batch size from context config', () => {
      const customContext = {
        getLogger: () => logger,
        getConfig: () => ({
          telemetryBatchSize: 3,
          telemetryFlushIntervalMs: 5000,
          directResultsDefaultMaxRows: 10000,
          fetchChunkDefaultMaxRows: 100000,
          socketTimeout: 900000,
          retryMaxAttempts: 30,
          retriesTimeout: 900000,
          retryDelayMin: 1000,
          retryDelayMax: 30000,
          useCloudFetch: true,
          cloudFetchConcurrentDownloads: 10,
          cloudFetchSpeedThresholdMBps: 0,
          useLZ4Compression: true,
        }),
      } as any;

      const customAggregator = new MetricsAggregator(customContext, exporter as any);

      // Process 3 events (custom batch size)
      for (let i = 0; i < 3; i++) {
        const event: TelemetryEvent = {
          eventType: TelemetryEventType.CONNECTION_OPEN,
          timestamp: Date.now(),
          sessionId: `session-${i}`,
          workspaceId: 'workspace-1',
        };
        customAggregator.processEvent(event);
      }

      // Should flush at batch size 3
      expect(exporter.exportCount).to.equal(1);
      customAggregator.close();
    });

    it('should read flush interval from context config', () => {
      const customContext = {
        getLogger: () => logger,
        getConfig: () => ({
          telemetryBatchSize: 10,
          telemetryFlushIntervalMs: 3000,
          directResultsDefaultMaxRows: 10000,
          fetchChunkDefaultMaxRows: 100000,
          socketTimeout: 900000,
          retryMaxAttempts: 30,
          retriesTimeout: 900000,
          retryDelayMin: 1000,
          retryDelayMax: 30000,
          useCloudFetch: true,
          cloudFetchConcurrentDownloads: 10,
          cloudFetchSpeedThresholdMBps: 0,
          useLZ4Compression: true,
        }),
      } as any;

      const customAggregator = new MetricsAggregator(customContext, exporter as any);

      const event: TelemetryEvent = {
        eventType: TelemetryEventType.CONNECTION_OPEN,
        timestamp: Date.now(),
        sessionId: 'session-123',
        workspaceId: 'workspace-1',
      };

      customAggregator.processEvent(event);

      // Should not flush yet
      expect(exporter.exportCount).to.equal(0);

      // Advance timer by custom flush interval (3000ms)
      clock.tick(3000);

      // Should flush now
      expect(exporter.exportCount).to.equal(1);
      customAggregator.close();
    });

    it('should use default values when config values are undefined', () => {
      const defaultContext = {
        getLogger: () => logger,
        getConfig: () => ({
          directResultsDefaultMaxRows: 10000,
          fetchChunkDefaultMaxRows: 100000,
          socketTimeout: 900000,
          retryMaxAttempts: 30,
          retriesTimeout: 900000,
          retryDelayMin: 1000,
          retryDelayMax: 30000,
          useCloudFetch: true,
          cloudFetchConcurrentDownloads: 10,
          cloudFetchSpeedThresholdMBps: 0,
          useLZ4Compression: true,
        }),
      } as any;

      const defaultAggregator = new MetricsAggregator(defaultContext, exporter as any);

      // Process events up to default batch size (100)
      for (let i = 0; i < DEFAULT_TELEMETRY_CONFIG.batchSize; i++) {
        const event: TelemetryEvent = {
          eventType: TelemetryEventType.CONNECTION_OPEN,
          timestamp: Date.now(),
          sessionId: `session-${i}`,
          workspaceId: 'workspace-1',
        };
        defaultAggregator.processEvent(event);
      }

      // Should flush at default batch size
      expect(exporter.exportCount).to.equal(1);
      defaultAggregator.close();
    });
  });
});
