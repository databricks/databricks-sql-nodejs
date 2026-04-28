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

      client.close();

      expect(client.isClosed()).to.be.true;
    });
  });

  describe('close', () => {
    it('should set closed flag', async () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);

      client.close();

      expect(client.isClosed()).to.be.true;
    });

    it('should log closure at debug level', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const client = new TelemetryClient(context, HOST);

      client.close();

      expect(logSpy.calledWith(LogLevel.debug, `Closing TelemetryClient for host: ${HOST}`)).to.be.true;
    });

    it('should be idempotent', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const client = new TelemetryClient(context, HOST);

      client.close();
      const firstCallCount = logSpy.callCount;

      client.close();

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

      expect(() => client.close()).to.not.throw();
    });

    it('should still set closed when logger throws', () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);

      sinon.stub(context.logger, 'log').throws(new Error('Logger error'));

      client.close();

      expect(client.isClosed()).to.be.true;
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
        expect(call.args[0]).to.satisfy((lvl: string) => lvl === LogLevel.debug || lvl === LogLevel.warn);
      });
    });
  });

  describe('feature-flag context registration (F1)', () => {
    it('should register the host with the feature-flag cache on construction', () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);

      // Without F1's wiring, isTelemetryEnabled returns false because the
      // contexts map is empty. With F1, the constructor registers the host
      // so the cache is ready to fetch the flag.
      const cache = client.getFeatureFlagCache();
      // Internal access for assertion only — tests on getInstance/resetInstance
      // would otherwise leak across the singleton.
      const ctx = (cache as any).contexts.get(HOST);
      expect(ctx, 'context should exist after TelemetryClient construction').to.exist;
      expect(ctx.refCount, 'refCount should be 1 after registration').to.equal(1);
    });

    it('should release the feature-flag context on close', async () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);
      const cache = client.getFeatureFlagCache();

      await client.close();

      const ctx = (cache as any).contexts.get(HOST);
      expect(ctx, 'context should be removed on close (refCount → 0)').to.be.undefined;
    });
  });

  describe('multi-context FIFO', () => {
    it('registerContext appends contexts in registration order', () => {
      const ctxA = new ClientContextStub();
      const ctxB = new ClientContextStub();
      const client = new TelemetryClient(ctxA, HOST);

      client.registerContext(ctxB);

      const internal = client as any;
      expect(internal.contexts).to.have.lengthOf(2);
      expect(internal.contexts[0]).to.equal(ctxA);
      expect(internal.contexts[1]).to.equal(ctxB);
    });

    it('registerContext is idempotent for the same context', () => {
      const ctxA = new ClientContextStub();
      const client = new TelemetryClient(ctxA, HOST);

      client.registerContext(ctxA);
      client.registerContext(ctxA);

      expect((client as any).contexts).to.have.lengthOf(1);
    });

    it('unregisterContext removes the context', () => {
      const ctxA = new ClientContextStub();
      const ctxB = new ClientContextStub();
      const client = new TelemetryClient(ctxA, HOST);
      client.registerContext(ctxB);

      client.unregisterContext(ctxA);

      const internal = client as any;
      expect(internal.contexts).to.have.lengthOf(1);
      expect(internal.contexts[0]).to.equal(ctxB);
    });

    it('warns when a registered context has divergent telemetry config (F12)', () => {
      const ctxA = new ClientContextStub({ telemetryAuthenticatedExport: true });
      const ctxB = new ClientContextStub({ telemetryAuthenticatedExport: false });
      const client = new TelemetryClient(ctxA, HOST);
      const logSpy = sinon.spy(ctxA.logger, 'log');

      client.registerContext(ctxB);

      const warnCall = logSpy
        .getCalls()
        .find((c) => c.args[0] === LogLevel.warn && /telemetry settings .* differ/.test(c.args[1] as string));
      expect(warnCall, 'should warn about divergent telemetryAuthenticatedExport').to.exist;
    });
  });

  describe('async close()', () => {
    it('returns a Promise that resolves only after aggregator close', async () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);
      const aggregator = client.getAggregator();
      const aggCloseStub = sinon.stub(aggregator, 'close').callsFake(
        () =>
          new Promise<void>((resolve) => {
            setTimeout(resolve, 5);
          }),
      );

      const closePromise = client.close();
      expect(client.isClosed(), 'closed flag is set synchronously').to.be.true;
      expect(aggCloseStub.calledOnce).to.be.true;
      await closePromise;
    });

    it('is idempotent — second close awaits without re-running aggregator close', async () => {
      const context = new ClientContextStub();
      const client = new TelemetryClient(context, HOST);
      const aggCloseStub = sinon.stub(client.getAggregator(), 'close').resolves();

      await client.close();
      await client.close();

      expect(aggCloseStub.calledOnce, 'aggregator.close should run exactly once').to.be.true;
    });
  });
});
