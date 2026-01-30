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
import TelemetryClient from '../../../lib/telemetry/TelemetryClient';
import ClientContextStub from '../.stubs/ClientContextStub';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

describe('TelemetryClient', () => {
  const HOST = 'workspace.cloud.databricks.com';

  describe('Constructor', () => {
    it('should create client with host', () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);

      expect(client.getHost()).to.equal(HOST);
      expect(client.isClosed()).to.be.false;
    });

    it('should log creation at debug level', () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');

      new TelemetryClient(context, HOST);

      expect(logSpy.calledWith(LogLevel.debug, `Created TelemetryClient for host: ${HOST}`)).to.be.true;
    });
  });

  describe('getHost', () => {
    it('should return the host identifier', () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);

      expect(client.getHost()).to.equal(HOST);
    });
  });

  describe('isClosed', () => {
    it('should return false initially', () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);

      expect(client.isClosed()).to.be.false;
    });

    it('should return true after close', async () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);

      await client.close();

      expect(client.isClosed()).to.be.true;
    });
  });

  describe('close', () => {
    it('should set closed flag', async () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);

      await client.close();

      expect(client.isClosed()).to.be.true;
    });

    it('should log closure at debug level', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const client = new TelemetryClient(context, HOST);

      await client.close();

      expect(logSpy.calledWith(LogLevel.debug, `Closing TelemetryClient for host: ${HOST}`)).to.be.true;
    });

    it('should be idempotent', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const client = new TelemetryClient(context, HOST);

      await client.close();
      const firstCallCount = logSpy.callCount;

      await client.close();

      // Should not log again on second close
      expect(logSpy.callCount).to.equal(firstCallCount);
      expect(client.isClosed()).to.be.true;
    });

    it('should swallow all exceptions', async () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);

      // Force an error by stubbing the logger
      const error = new Error('Logger error');
      sinon.stub(context.logger, 'log').throws(error);

      // Should not throw
      await client.close();
      // If we get here without throwing, the test passes
      expect(true).to.be.true;
    });

    it('should log errors at debug level only', async () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);
      const error = new Error('Test error');

      // Stub logger to throw on first call, succeed on second
      const logStub = sinon.stub(context.logger, 'log');
      logStub.onFirstCall().throws(error);
      logStub.onSecondCall().returns();

      await client.close();

      // Second call should log the error at debug level
      expect(logStub.secondCall.args[0]).to.equal(LogLevel.debug);
      expect(logStub.secondCall.args[1]).to.include('Error closing TelemetryClient');
    });
  });

  describe('Context usage', () => {
    it('should use logger from context', () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');

      new TelemetryClient(context, HOST);

      expect(logSpy.called).to.be.true;
    });

    it('should log all messages at debug level only', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const client = new TelemetryClient(context, HOST);

      await client.close();

      logSpy.getCalls().forEach((call) => {
        expect(call.args[0]).to.equal(LogLevel.debug);
      });
    });
  });
});
