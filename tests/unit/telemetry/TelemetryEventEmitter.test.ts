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
import { TelemetryEventType } from '../../../lib/telemetry/types';
import ClientContextStub from '../.stubs/ClientContextStub';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

function makeEmitter(enabled: boolean): TelemetryEventEmitter {
  const context = new ClientContextStub({ telemetryEnabled: enabled } as any);
  return new TelemetryEventEmitter(context);
}

describe('TelemetryEventEmitter', () => {
  describe('when telemetry is disabled', () => {
    it('should not emit any events', () => {
      const emitter = makeEmitter(false);
      const listener = sinon.stub();
      emitter.on(TelemetryEventType.CONNECTION_OPEN, listener);

      emitter.emitConnectionOpen({ sessionId: 's1', workspaceId: 'w1', driverConfig: {} as any });

      expect(listener.called).to.be.false;
    });

    it('should not emit statement start', () => {
      const emitter = makeEmitter(false);
      const listener = sinon.stub();
      emitter.on(TelemetryEventType.STATEMENT_START, listener);

      emitter.emitStatementStart({ statementId: 'st1', sessionId: 's1' });

      expect(listener.called).to.be.false;
    });

    it('should not emit error events', () => {
      const emitter = makeEmitter(false);
      const listener = sinon.stub();
      emitter.on(TelemetryEventType.ERROR, listener);

      emitter.emitError({ errorName: 'SomeError', errorMessage: 'msg', isTerminal: false });

      expect(listener.called).to.be.false;
    });
  });

  describe('emitConnectionOpen()', () => {
    it('should emit a CONNECTION_OPEN event with correct fields', () => {
      const emitter = makeEmitter(true);
      const listener = sinon.stub();
      emitter.on(TelemetryEventType.CONNECTION_OPEN, listener);

      emitter.emitConnectionOpen({ sessionId: 's1', workspaceId: 'w1', driverConfig: {} as any });

      expect(listener.calledOnce).to.be.true;
      const event = listener.firstCall.args[0];
      expect(event.eventType).to.equal(TelemetryEventType.CONNECTION_OPEN);
      expect(event.sessionId).to.equal('s1');
      expect(event.workspaceId).to.equal('w1');
      expect(event.timestamp).to.be.a('number');
    });

    it('should swallow and log exceptions from listeners', () => {
      const context = new ClientContextStub({ telemetryEnabled: true } as any);
      const logSpy = sinon.spy(context.logger, 'log');
      const emitter = new TelemetryEventEmitter(context);

      emitter.on(TelemetryEventType.CONNECTION_OPEN, () => {
        throw new Error('listener boom');
      });

      expect(() =>
        emitter.emitConnectionOpen({ sessionId: 's1', workspaceId: 'w1', driverConfig: {} as any }),
      ).to.not.throw();
      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/listener boom/))).to.be.true;

      logSpy.restore();
    });
  });

  describe('emitStatementStart()', () => {
    it('should emit a STATEMENT_START event with correct fields', () => {
      const emitter = makeEmitter(true);
      const listener = sinon.stub();
      emitter.on(TelemetryEventType.STATEMENT_START, listener);

      emitter.emitStatementStart({ statementId: 'st1', sessionId: 's1', operationType: 'SELECT' });

      expect(listener.calledOnce).to.be.true;
      const event = listener.firstCall.args[0];
      expect(event.eventType).to.equal(TelemetryEventType.STATEMENT_START);
      expect(event.statementId).to.equal('st1');
      expect(event.operationType).to.equal('SELECT');
    });
  });

  describe('emitStatementComplete()', () => {
    it('should emit a STATEMENT_COMPLETE event with correct fields', () => {
      const emitter = makeEmitter(true);
      const listener = sinon.stub();
      emitter.on(TelemetryEventType.STATEMENT_COMPLETE, listener);

      emitter.emitStatementComplete({
        statementId: 'st1',
        sessionId: 's1',
        latencyMs: 123,
        resultFormat: 'arrow',
        chunkCount: 2,
        bytesDownloaded: 1024,
        pollCount: 3,
      });

      expect(listener.calledOnce).to.be.true;
      const event = listener.firstCall.args[0];
      expect(event.eventType).to.equal(TelemetryEventType.STATEMENT_COMPLETE);
      expect(event.latencyMs).to.equal(123);
      expect(event.chunkCount).to.equal(2);
    });
  });

  describe('emitCloudFetchChunk()', () => {
    it('should emit a CLOUDFETCH_CHUNK event with correct fields', () => {
      const emitter = makeEmitter(true);
      const listener = sinon.stub();
      emitter.on(TelemetryEventType.CLOUDFETCH_CHUNK, listener);

      emitter.emitCloudFetchChunk({ statementId: 'st1', chunkIndex: 0, bytes: 512, compressed: true });

      expect(listener.calledOnce).to.be.true;
      const event = listener.firstCall.args[0];
      expect(event.eventType).to.equal(TelemetryEventType.CLOUDFETCH_CHUNK);
      expect(event.bytes).to.equal(512);
      expect(event.compressed).to.be.true;
    });
  });

  describe('emitError()', () => {
    it('should emit an ERROR event with correct fields', () => {
      const emitter = makeEmitter(true);
      const listener = sinon.stub();
      emitter.on(TelemetryEventType.ERROR, listener);

      emitter.emitError({
        statementId: 'st1',
        sessionId: 's1',
        errorName: 'NetworkError',
        errorMessage: 'timeout',
        isTerminal: false,
      });

      expect(listener.calledOnce).to.be.true;
      const event = listener.firstCall.args[0];
      expect(event.eventType).to.equal(TelemetryEventType.ERROR);
      expect(event.errorName).to.equal('NetworkError');
      expect(event.isTerminal).to.be.false;
    });

    it('should emit a terminal ERROR event', () => {
      const emitter = makeEmitter(true);
      const listener = sinon.stub();
      emitter.on(TelemetryEventType.ERROR, listener);

      emitter.emitError({ errorName: 'AuthenticationError', errorMessage: 'auth failed', isTerminal: true });

      const event = listener.firstCall.args[0];
      expect(event.isTerminal).to.be.true;
    });
  });

  describe('logging level compliance', () => {
    it('should never log at warn or error level', () => {
      const context = new ClientContextStub({ telemetryEnabled: true } as any);
      const logSpy = sinon.spy(context.logger, 'log');
      const emitter = new TelemetryEventEmitter(context);

      emitter.on(TelemetryEventType.CONNECTION_OPEN, () => {
        throw new Error('boom');
      });

      emitter.emitConnectionOpen({ sessionId: 's1', workspaceId: 'w1', driverConfig: {} as any });

      expect(logSpy.neverCalledWith(LogLevel.error, sinon.match.any)).to.be.true;
      expect(logSpy.neverCalledWith(LogLevel.warn, sinon.match.any)).to.be.true;

      logSpy.restore();
    });
  });
});
