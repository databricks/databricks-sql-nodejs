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
import TelemetryEventEmitter from '../../../lib/telemetry/TelemetryEventEmitter';
import { TelemetryEventType, TelemetryEvent, DriverConfiguration } from '../../../lib/telemetry/types';
import IClientContext from '../../../lib/contracts/IClientContext';
import IDBSQLLogger, { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

describe('TelemetryEventEmitter', () => {
  let context: IClientContext;
  let logger: IDBSQLLogger;
  let emitter: TelemetryEventEmitter;

  beforeEach(() => {
    logger = {
      log: sinon.stub(),
    };

    context = {
      getLogger: () => logger,
      getConfig: () => ({
        telemetryEnabled: true,
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

    emitter = new TelemetryEventEmitter(context);
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('constructor', () => {
    it('should create instance with telemetry enabled', () => {
      expect(emitter).to.be.instanceOf(TelemetryEventEmitter);
    });

    it('should create instance with telemetry disabled', () => {
      const disabledContext = {
        getLogger: () => logger,
        getConfig: () => ({
          telemetryEnabled: false,
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

      const disabledEmitter = new TelemetryEventEmitter(disabledContext);
      expect(disabledEmitter).to.be.instanceOf(TelemetryEventEmitter);
    });

    it('should default to disabled when telemetryEnabled is undefined', () => {
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

      const defaultEmitter = new TelemetryEventEmitter(defaultContext);
      expect(defaultEmitter).to.be.instanceOf(TelemetryEventEmitter);
    });
  });

  describe('emitConnectionOpen', () => {
    it('should emit connection.open event with correct data', (done) => {
      const driverConfig: DriverConfiguration = {
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
      };

      emitter.on(TelemetryEventType.CONNECTION_OPEN, (event: TelemetryEvent) => {
        expect(event.eventType).to.equal(TelemetryEventType.CONNECTION_OPEN);
        expect(event.sessionId).to.equal('session-123');
        expect(event.workspaceId).to.equal('workspace-456');
        expect(event.driverConfig).to.deep.equal(driverConfig);
        expect(event.timestamp).to.be.a('number');
        done();
      });

      emitter.emitConnectionOpen({
        sessionId: 'session-123',
        workspaceId: 'workspace-456',
        driverConfig,
      });
    });

    it('should not emit when telemetry is disabled', () => {
      const disabledContext = {
        getLogger: () => logger,
        getConfig: () => ({
          telemetryEnabled: false,
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

      const disabledEmitter = new TelemetryEventEmitter(disabledContext);
      let eventEmitted = false;

      disabledEmitter.on(TelemetryEventType.CONNECTION_OPEN, () => {
        eventEmitted = true;
      });

      disabledEmitter.emitConnectionOpen({
        sessionId: 'session-123',
        workspaceId: 'workspace-456',
        driverConfig: {} as DriverConfiguration,
      });

      expect(eventEmitted).to.be.false;
    });

    it('should swallow exceptions and log at debug level', () => {
      // Force an exception by emitting before adding any listeners
      // Then make emit throw by adding a throwing listener
      emitter.on(TelemetryEventType.CONNECTION_OPEN, () => {
        throw new Error('Test error');
      });

      emitter.emitConnectionOpen({
        sessionId: 'session-123',
        workspaceId: 'workspace-456',
        driverConfig: {} as DriverConfiguration,
      });

      expect((logger.log as sinon.SinonStub).calledWith(LogLevel.debug)).to.be.true;
      expect((logger.log as sinon.SinonStub).args[0][1]).to.include('Error emitting connection event');
    });

    it('should not log at warn or error level', () => {
      emitter.on(TelemetryEventType.CONNECTION_OPEN, () => {
        throw new Error('Test error');
      });

      emitter.emitConnectionOpen({
        sessionId: 'session-123',
        workspaceId: 'workspace-456',
        driverConfig: {} as DriverConfiguration,
      });

      const logStub = logger.log as sinon.SinonStub;
      for (let i = 0; i < logStub.callCount; i++) {
        const level = logStub.args[i][0];
        expect(level).to.not.equal(LogLevel.warn);
        expect(level).to.not.equal(LogLevel.error);
      }
    });
  });

  describe('emitStatementStart', () => {
    it('should emit statement.start event with correct data', (done) => {
      emitter.on(TelemetryEventType.STATEMENT_START, (event: TelemetryEvent) => {
        expect(event.eventType).to.equal(TelemetryEventType.STATEMENT_START);
        expect(event.statementId).to.equal('stmt-789');
        expect(event.sessionId).to.equal('session-123');
        expect(event.operationType).to.equal('SELECT');
        expect(event.timestamp).to.be.a('number');
        done();
      });

      emitter.emitStatementStart({
        statementId: 'stmt-789',
        sessionId: 'session-123',
        operationType: 'SELECT',
      });
    });

    it('should emit without operationType', (done) => {
      emitter.on(TelemetryEventType.STATEMENT_START, (event: TelemetryEvent) => {
        expect(event.eventType).to.equal(TelemetryEventType.STATEMENT_START);
        expect(event.statementId).to.equal('stmt-789');
        expect(event.sessionId).to.equal('session-123');
        expect(event.operationType).to.be.undefined;
        done();
      });

      emitter.emitStatementStart({
        statementId: 'stmt-789',
        sessionId: 'session-123',
      });
    });

    it('should not emit when telemetry is disabled', () => {
      const disabledContext = {
        getLogger: () => logger,
        getConfig: () => ({ telemetryEnabled: false }),
      } as any;

      const disabledEmitter = new TelemetryEventEmitter(disabledContext);
      let eventEmitted = false;

      disabledEmitter.on(TelemetryEventType.STATEMENT_START, () => {
        eventEmitted = true;
      });

      disabledEmitter.emitStatementStart({
        statementId: 'stmt-789',
        sessionId: 'session-123',
      });

      expect(eventEmitted).to.be.false;
    });

    it('should swallow exceptions and log at debug level', () => {
      emitter.on(TelemetryEventType.STATEMENT_START, () => {
        throw new Error('Test error');
      });

      emitter.emitStatementStart({
        statementId: 'stmt-789',
        sessionId: 'session-123',
      });

      expect((logger.log as sinon.SinonStub).calledWith(LogLevel.debug)).to.be.true;
      expect((logger.log as sinon.SinonStub).args[0][1]).to.include('Error emitting statement start');
    });
  });

  describe('emitStatementComplete', () => {
    it('should emit statement.complete event with all data fields', (done) => {
      emitter.on(TelemetryEventType.STATEMENT_COMPLETE, (event: TelemetryEvent) => {
        expect(event.eventType).to.equal(TelemetryEventType.STATEMENT_COMPLETE);
        expect(event.statementId).to.equal('stmt-789');
        expect(event.sessionId).to.equal('session-123');
        expect(event.latencyMs).to.equal(1500);
        expect(event.resultFormat).to.equal('cloudfetch');
        expect(event.chunkCount).to.equal(5);
        expect(event.bytesDownloaded).to.equal(1024000);
        expect(event.pollCount).to.equal(3);
        expect(event.timestamp).to.be.a('number');
        done();
      });

      emitter.emitStatementComplete({
        statementId: 'stmt-789',
        sessionId: 'session-123',
        latencyMs: 1500,
        resultFormat: 'cloudfetch',
        chunkCount: 5,
        bytesDownloaded: 1024000,
        pollCount: 3,
      });
    });

    it('should emit with minimal data', (done) => {
      emitter.on(TelemetryEventType.STATEMENT_COMPLETE, (event: TelemetryEvent) => {
        expect(event.eventType).to.equal(TelemetryEventType.STATEMENT_COMPLETE);
        expect(event.statementId).to.equal('stmt-789');
        expect(event.sessionId).to.equal('session-123');
        expect(event.latencyMs).to.be.undefined;
        expect(event.resultFormat).to.be.undefined;
        done();
      });

      emitter.emitStatementComplete({
        statementId: 'stmt-789',
        sessionId: 'session-123',
      });
    });

    it('should not emit when telemetry is disabled', () => {
      const disabledContext = {
        getLogger: () => logger,
        getConfig: () => ({ telemetryEnabled: false }),
      } as any;

      const disabledEmitter = new TelemetryEventEmitter(disabledContext);
      let eventEmitted = false;

      disabledEmitter.on(TelemetryEventType.STATEMENT_COMPLETE, () => {
        eventEmitted = true;
      });

      disabledEmitter.emitStatementComplete({
        statementId: 'stmt-789',
        sessionId: 'session-123',
      });

      expect(eventEmitted).to.be.false;
    });

    it('should swallow exceptions and log at debug level', () => {
      emitter.on(TelemetryEventType.STATEMENT_COMPLETE, () => {
        throw new Error('Test error');
      });

      emitter.emitStatementComplete({
        statementId: 'stmt-789',
        sessionId: 'session-123',
      });

      expect((logger.log as sinon.SinonStub).calledWith(LogLevel.debug)).to.be.true;
      expect((logger.log as sinon.SinonStub).args[0][1]).to.include('Error emitting statement complete');
    });
  });

  describe('emitCloudFetchChunk', () => {
    it('should emit cloudfetch.chunk event with correct data', (done) => {
      emitter.on(TelemetryEventType.CLOUDFETCH_CHUNK, (event: TelemetryEvent) => {
        expect(event.eventType).to.equal(TelemetryEventType.CLOUDFETCH_CHUNK);
        expect(event.statementId).to.equal('stmt-789');
        expect(event.chunkIndex).to.equal(2);
        expect(event.latencyMs).to.equal(250);
        expect(event.bytes).to.equal(204800);
        expect(event.compressed).to.be.true;
        expect(event.timestamp).to.be.a('number');
        done();
      });

      emitter.emitCloudFetchChunk({
        statementId: 'stmt-789',
        chunkIndex: 2,
        latencyMs: 250,
        bytes: 204800,
        compressed: true,
      });
    });

    it('should emit without optional fields', (done) => {
      emitter.on(TelemetryEventType.CLOUDFETCH_CHUNK, (event: TelemetryEvent) => {
        expect(event.eventType).to.equal(TelemetryEventType.CLOUDFETCH_CHUNK);
        expect(event.statementId).to.equal('stmt-789');
        expect(event.chunkIndex).to.equal(0);
        expect(event.bytes).to.equal(100000);
        expect(event.latencyMs).to.be.undefined;
        expect(event.compressed).to.be.undefined;
        done();
      });

      emitter.emitCloudFetchChunk({
        statementId: 'stmt-789',
        chunkIndex: 0,
        bytes: 100000,
      });
    });

    it('should not emit when telemetry is disabled', () => {
      const disabledContext = {
        getLogger: () => logger,
        getConfig: () => ({ telemetryEnabled: false }),
      } as any;

      const disabledEmitter = new TelemetryEventEmitter(disabledContext);
      let eventEmitted = false;

      disabledEmitter.on(TelemetryEventType.CLOUDFETCH_CHUNK, () => {
        eventEmitted = true;
      });

      disabledEmitter.emitCloudFetchChunk({
        statementId: 'stmt-789',
        chunkIndex: 0,
        bytes: 100000,
      });

      expect(eventEmitted).to.be.false;
    });

    it('should swallow exceptions and log at debug level', () => {
      emitter.on(TelemetryEventType.CLOUDFETCH_CHUNK, () => {
        throw new Error('Test error');
      });

      emitter.emitCloudFetchChunk({
        statementId: 'stmt-789',
        chunkIndex: 0,
        bytes: 100000,
      });

      expect((logger.log as sinon.SinonStub).calledWith(LogLevel.debug)).to.be.true;
      expect((logger.log as sinon.SinonStub).args[0][1]).to.include('Error emitting cloudfetch chunk');
    });
  });

  describe('emitError', () => {
    it('should emit error event with all fields', (done) => {
      emitter.on(TelemetryEventType.ERROR, (event: TelemetryEvent) => {
        expect(event.eventType).to.equal(TelemetryEventType.ERROR);
        expect(event.statementId).to.equal('stmt-789');
        expect(event.sessionId).to.equal('session-123');
        expect(event.errorName).to.equal('AuthenticationError');
        expect(event.errorMessage).to.equal('Invalid credentials');
        expect(event.isTerminal).to.be.true;
        expect(event.timestamp).to.be.a('number');
        done();
      });

      emitter.emitError({
        statementId: 'stmt-789',
        sessionId: 'session-123',
        errorName: 'AuthenticationError',
        errorMessage: 'Invalid credentials',
        isTerminal: true,
      });
    });

    it('should emit error event with minimal fields', (done) => {
      emitter.on(TelemetryEventType.ERROR, (event: TelemetryEvent) => {
        expect(event.eventType).to.equal(TelemetryEventType.ERROR);
        expect(event.errorName).to.equal('TimeoutError');
        expect(event.errorMessage).to.equal('Request timed out');
        expect(event.isTerminal).to.be.false;
        expect(event.statementId).to.be.undefined;
        expect(event.sessionId).to.be.undefined;
        done();
      });

      emitter.emitError({
        errorName: 'TimeoutError',
        errorMessage: 'Request timed out',
        isTerminal: false,
      });
    });

    it('should not emit when telemetry is disabled', () => {
      const disabledContext = {
        getLogger: () => logger,
        getConfig: () => ({ telemetryEnabled: false }),
      } as any;

      const disabledEmitter = new TelemetryEventEmitter(disabledContext);
      let eventEmitted = false;

      disabledEmitter.on(TelemetryEventType.ERROR, () => {
        eventEmitted = true;
      });

      disabledEmitter.emitError({
        errorName: 'Error',
        errorMessage: 'Test',
        isTerminal: false,
      });

      expect(eventEmitted).to.be.false;
    });

    it('should swallow exceptions and log at debug level', () => {
      emitter.on(TelemetryEventType.ERROR, () => {
        throw new Error('Test error');
      });

      emitter.emitError({
        errorName: 'Error',
        errorMessage: 'Test',
        isTerminal: false,
      });

      expect((logger.log as sinon.SinonStub).calledWith(LogLevel.debug)).to.be.true;
      expect((logger.log as sinon.SinonStub).args[0][1]).to.include('Error emitting error event');
    });
  });

  describe('exception swallowing', () => {
    it('should never propagate exceptions to caller', () => {
      emitter.on(TelemetryEventType.CONNECTION_OPEN, () => {
        throw new Error('Critical error');
      });

      expect(() => {
        emitter.emitConnectionOpen({
          sessionId: 'session-123',
          workspaceId: 'workspace-456',
          driverConfig: {} as DriverConfiguration,
        });
      }).to.not.throw();
    });

    it('should swallow multiple listener exceptions', () => {
      emitter.on(TelemetryEventType.STATEMENT_START, () => {
        throw new Error('First listener error');
      });
      emitter.on(TelemetryEventType.STATEMENT_START, () => {
        throw new Error('Second listener error');
      });

      expect(() => {
        emitter.emitStatementStart({
          statementId: 'stmt-789',
          sessionId: 'session-123',
        });
      }).to.not.throw();
    });

    it('should log only at debug level, never at warn or error', () => {
      emitter.on(TelemetryEventType.STATEMENT_COMPLETE, () => {
        throw new Error('Test error');
      });
      emitter.on(TelemetryEventType.CLOUDFETCH_CHUNK, () => {
        throw new Error('Test error');
      });
      emitter.on(TelemetryEventType.ERROR, () => {
        throw new Error('Test error');
      });

      emitter.emitStatementComplete({
        statementId: 'stmt-1',
        sessionId: 'session-1',
      });
      emitter.emitCloudFetchChunk({
        statementId: 'stmt-1',
        chunkIndex: 0,
        bytes: 1000,
      });
      emitter.emitError({
        errorName: 'Error',
        errorMessage: 'Test',
        isTerminal: false,
      });

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

      emitter.on(TelemetryEventType.CONNECTION_OPEN, () => {
        throw new Error('Test error');
      });

      emitter.emitConnectionOpen({
        sessionId: 'session-123',
        workspaceId: 'workspace-456',
        driverConfig: {} as DriverConfiguration,
      });

      expect(consoleSpy.called).to.be.false;
      consoleSpy.restore();
    });

    it('should not use console.debug', () => {
      const consoleSpy = sinon.spy(console, 'debug');

      emitter.on(TelemetryEventType.STATEMENT_START, () => {
        throw new Error('Test error');
      });

      emitter.emitStatementStart({
        statementId: 'stmt-789',
        sessionId: 'session-123',
      });

      expect(consoleSpy.called).to.be.false;
      consoleSpy.restore();
    });

    it('should not use console.error', () => {
      const consoleSpy = sinon.spy(console, 'error');

      emitter.on(TelemetryEventType.ERROR, () => {
        throw new Error('Test error');
      });

      emitter.emitError({
        errorName: 'Error',
        errorMessage: 'Test',
        isTerminal: true,
      });

      expect(consoleSpy.called).to.be.false;
      consoleSpy.restore();
    });
  });

  describe('respects telemetryEnabled flag', () => {
    it('should respect flag from context.getConfig()', () => {
      const customContext = {
        getLogger: () => logger,
        getConfig: () => ({
          telemetryEnabled: true,
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

      const customEmitter = new TelemetryEventEmitter(customContext);
      let eventCount = 0;

      customEmitter.on(TelemetryEventType.CONNECTION_OPEN, () => {
        eventCount++;
      });

      customEmitter.emitConnectionOpen({
        sessionId: 'session-123',
        workspaceId: 'workspace-456',
        driverConfig: {} as DriverConfiguration,
      });

      expect(eventCount).to.equal(1);
    });

    it('should not emit when explicitly disabled', () => {
      const disabledContext = {
        getLogger: () => logger,
        getConfig: () => ({
          telemetryEnabled: false,
        }),
      } as any;

      const disabledEmitter = new TelemetryEventEmitter(disabledContext);
      let eventCount = 0;

      disabledEmitter.on(TelemetryEventType.CONNECTION_OPEN, () => {
        eventCount++;
      });
      disabledEmitter.on(TelemetryEventType.STATEMENT_START, () => {
        eventCount++;
      });
      disabledEmitter.on(TelemetryEventType.STATEMENT_COMPLETE, () => {
        eventCount++;
      });
      disabledEmitter.on(TelemetryEventType.CLOUDFETCH_CHUNK, () => {
        eventCount++;
      });
      disabledEmitter.on(TelemetryEventType.ERROR, () => {
        eventCount++;
      });

      disabledEmitter.emitConnectionOpen({
        sessionId: 'session-123',
        workspaceId: 'workspace-456',
        driverConfig: {} as DriverConfiguration,
      });
      disabledEmitter.emitStatementStart({
        statementId: 'stmt-789',
        sessionId: 'session-123',
      });
      disabledEmitter.emitStatementComplete({
        statementId: 'stmt-789',
        sessionId: 'session-123',
      });
      disabledEmitter.emitCloudFetchChunk({
        statementId: 'stmt-789',
        chunkIndex: 0,
        bytes: 1000,
      });
      disabledEmitter.emitError({
        errorName: 'Error',
        errorMessage: 'Test',
        isTerminal: false,
      });

      expect(eventCount).to.equal(0);
    });
  });
});
