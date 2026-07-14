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
import TelemetryClientProvider from '../../../lib/telemetry/TelemetryClientProvider';
import TelemetryClient from '../../../lib/telemetry/TelemetryClient';
import ClientContextStub from '../.stubs/ClientContextStub';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

describe('TelemetryClientProvider', () => {
  const HOST1 = 'workspace1.cloud.databricks.com';
  const HOST2 = 'workspace2.cloud.databricks.com';

  describe('Constructor', () => {
    it('should create provider with empty client map', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      expect(provider.getActiveClients().size).to.equal(0);
    });
  });

  describe('getOrCreateClient', () => {
    it('should create one client per host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client1 = provider.getOrCreateClient(context, HOST1);
      const client2 = provider.getOrCreateClient(context, HOST2);

      expect(client1).to.be.instanceOf(TelemetryClient);
      expect(client2).to.be.instanceOf(TelemetryClient);
      expect(client1).to.not.equal(client2);
      expect(provider.getActiveClients().size).to.equal(2);
    });

    it('should share client across multiple connections to same host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client1 = provider.getOrCreateClient(context, HOST1);
      const client2 = provider.getOrCreateClient(context, HOST1);
      const client3 = provider.getOrCreateClient(context, HOST1);

      expect(client1).to.equal(client2);
      expect(client2).to.equal(client3);
      expect(provider.getActiveClients().size).to.equal(1);
    });

    it('should increment reference count on each call', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      provider.getOrCreateClient(context, HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(1);

      provider.getOrCreateClient(context, HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(2);

      provider.getOrCreateClient(context, HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(3);
    });

    it('should log client creation at debug level', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();
      const logSpy = sinon.spy(context.logger, 'log');

      provider.getOrCreateClient(context, HOST1);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/created new telemetryclient/i))).to.be.true;
    });

    it('should log reference count at debug level', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();
      const logSpy = sinon.spy(context.logger, 'log');

      provider.getOrCreateClient(context, HOST1);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/reference count for/i).and(sinon.match(/: 1$/)))).to.be
        .true;
    });

    it('should pass context to TelemetryClient', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client = provider.getOrCreateClient(context, HOST1);

      expect(client.getHost()).to.equal(HOST1);
    });
  });

  describe('releaseClient', () => {
    it('should decrement reference count on release', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      provider.getOrCreateClient(context, HOST1);
      provider.getOrCreateClient(context, HOST1);
      provider.getOrCreateClient(context, HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(3);

      await provider.releaseClient(context, HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(2);

      await provider.releaseClient(context, HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(1);
    });

    it('should close client when reference count reaches zero', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client = provider.getOrCreateClient(context, HOST1);
      const closeSpy = sinon.spy(client, 'close');

      await provider.releaseClient(context, HOST1);

      expect(closeSpy.calledOnce).to.be.true;
      expect(client.isClosed()).to.be.true;
    });

    it('should remove client from map when reference count reaches zero', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      provider.getOrCreateClient(context, HOST1);
      expect(provider.getActiveClients().size).to.equal(1);

      await provider.releaseClient(context, HOST1);

      expect(provider.getActiveClients().size).to.equal(0);
      expect(provider.getRefCount(HOST1)).to.equal(0);
    });

    it('should NOT close client while other connections exist', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client = provider.getOrCreateClient(context, HOST1);
      provider.getOrCreateClient(context, HOST1);
      provider.getOrCreateClient(context, HOST1);
      const closeSpy = sinon.spy(client, 'close');

      await provider.releaseClient(context, HOST1);

      expect(closeSpy.called).to.be.false;
      expect(client.isClosed()).to.be.false;
      expect(provider.getActiveClients().size).to.equal(1);
    });

    it('should handle releasing non-existent client gracefully', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();
      const logSpy = sinon.spy(context.logger, 'log');

      await provider.releaseClient(context, HOST1);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/no telemetryclient found/i))).to.be.true;
    });

    it('should log reference count decrease at debug level', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();
      const logSpy = sinon.spy(context.logger, 'log');

      provider.getOrCreateClient(context, HOST1);
      provider.getOrCreateClient(context, HOST1);

      await provider.releaseClient(context, HOST1);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/reference count for/i).and(sinon.match(/: 1$/)))).to.be
        .true;
    });

    it('should log client closure at debug level', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();
      const logSpy = sinon.spy(context.logger, 'log');

      provider.getOrCreateClient(context, HOST1);
      await provider.releaseClient(context, HOST1);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/closed and removed telemetryclient/i))).to.be.true;
    });

    it('should swallow Error during client closure', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client = provider.getOrCreateClient(context, HOST1);
      const error = new Error('Close error');
      sinon.stub(client, 'close').throws(error);
      const logSpy = sinon.spy(context.logger, 'log');

      await provider.releaseClient(context, HOST1);

      expect(
        logSpy.calledWith(
          LogLevel.debug,
          sinon.match(/error releasing telemetryclient/i).and(sinon.match(/close error/i)),
        ),
      ).to.be.true;
    });

    it('should swallow non-Error throws during client closure', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client = provider.getOrCreateClient(context, HOST1);
      // Non-Error throws — string, null, undefined — must not escape the catch.
      sinon.stub(client, 'close').callsFake(() => {
        // eslint-disable-next-line no-throw-literal
        throw 'stringy-error';
      });
      const logSpy = sinon.spy(context.logger, 'log');

      expect(() => provider.releaseClient(context, HOST1)).to.not.throw();
      expect(
        logSpy.calledWith(
          LogLevel.debug,
          sinon.match(/error releasing telemetryclient/i).and(sinon.match(/stringy-error/)),
        ),
      ).to.be.true;
    });

    it('should not throw or corrupt state on double-release', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      // One get, two releases — the second must not throw and must not
      // leave the provider in a state where refCount is negative.
      provider.getOrCreateClient(context, HOST1);
      await provider.releaseClient(context, HOST1);

      expect(() => provider.releaseClient(context, HOST1)).to.not.throw();
      expect(provider.getRefCount(HOST1)).to.equal(0);
      expect(provider.getActiveClients().size).to.equal(0);
    });

    it('should return a fresh non-closed client after full release', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const first = provider.getOrCreateClient(context, HOST1);
      await provider.releaseClient(context, HOST1);
      expect(first.isClosed()).to.be.true;

      const second = provider.getOrCreateClient(context, HOST1);
      expect(second).to.not.equal(first);
      expect(second.isClosed()).to.be.false;
      expect(provider.getRefCount(HOST1)).to.equal(1);
    });

    it('keeps the context registered through close() on last refcount', async () => {
      // Regression: releaseClient used to call unregisterContext before
      // close(), which dropped the only FIFO entry and left the final
      // flush without an auth provider — metrics were dropped with
      // "missing Authorization header".
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client = provider.getOrCreateClient(context, HOST1);
      let authProviderAtClose: unknown = 'unset';
      sinon.stub(client, 'close').callsFake(async () => {
        // The TelemetryClient is its own IClientContext. While close() runs,
        // getAuthProvider() must still resolve (the FIFO entry survives).
        authProviderAtClose = client.getAuthProvider?.();
      });

      await provider.releaseClient(context, HOST1);

      // The FIFO walk hits the (still-registered) context; the stub's
      // getAuthProvider returns undefined but the lookup itself completes.
      // What we're really asserting is that the FIFO wasn't pre-emptied:
      // pre-fix, this assertion would not even fire because close() runs
      // against an empty FIFO and the auth-provider walk short-circuits
      // before close()'s callsFake. Here we verify the spy ran AND the
      // context is still registered at that moment.
      expect(authProviderAtClose).to.not.equal('unset');
      expect((client as any).contexts.length).to.equal(1);
    });

    it('drops the context immediately when other refcounts remain', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client = provider.getOrCreateClient(context, HOST1);
      provider.getOrCreateClient(context, HOST1); // refcount=2

      await provider.releaseClient(context, HOST1);

      // Multi-refcount path: unregisterContext runs immediately; the
      // (single) FIFO entry tracking this context was removed.
      expect((client as any).contexts.length).to.equal(0);
      expect(provider.getRefCount(HOST1)).to.equal(1);
    });
  });

  describe('Host normalization', () => {
    it('should treat scheme, case, port and trailing slash as the same host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const a = provider.getOrCreateClient(context, 'workspace.cloud.databricks.com');
      const b = provider.getOrCreateClient(context, 'https://workspace.cloud.databricks.com');
      const c = provider.getOrCreateClient(context, 'https://WorkSpace.CLOUD.databricks.com/');
      const d = provider.getOrCreateClient(context, 'workspace.cloud.databricks.com:443');
      const e = provider.getOrCreateClient(context, '  workspace.cloud.databricks.com.  ');

      expect(a).to.equal(b);
      expect(a).to.equal(c);
      expect(a).to.equal(d);
      expect(a).to.equal(e);
      expect(provider.getActiveClients().size).to.equal(1);
      expect(provider.getRefCount('workspace.cloud.databricks.com')).to.equal(5);
    });

    it('should release under an alias correctly', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      provider.getOrCreateClient(context, 'example.com');
      await provider.releaseClient(context, 'HTTPS://Example.COM/');

      expect(provider.getRefCount('example.com')).to.equal(0);
      expect(provider.getActiveClients().size).to.equal(0);
    });
  });

  describe('Reference counting', () => {
    it('should track reference counts independently per host', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      provider.getOrCreateClient(context, HOST1);
      provider.getOrCreateClient(context, HOST1);
      provider.getOrCreateClient(context, HOST2);
      provider.getOrCreateClient(context, HOST2);
      provider.getOrCreateClient(context, HOST2);

      expect(provider.getRefCount(HOST1)).to.equal(2);
      expect(provider.getRefCount(HOST2)).to.equal(3);

      await provider.releaseClient(context, HOST1);

      expect(provider.getRefCount(HOST1)).to.equal(1);
      expect(provider.getRefCount(HOST2)).to.equal(3);
    });

    it('should close only last connection for each host', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client1 = provider.getOrCreateClient(context, HOST1);
      provider.getOrCreateClient(context, HOST1);
      const client2 = provider.getOrCreateClient(context, HOST2);

      await provider.releaseClient(context, HOST1);
      expect(client1.isClosed()).to.be.false;
      expect(provider.getActiveClients().size).to.equal(2);

      await provider.releaseClient(context, HOST1);
      expect(client1.isClosed()).to.be.true;
      expect(provider.getActiveClients().size).to.equal(1);

      await provider.releaseClient(context, HOST2);
      expect(client2.isClosed()).to.be.true;
      expect(provider.getActiveClients().size).to.equal(0);
    });
  });

  describe('Per-host isolation', () => {
    it('should isolate clients by host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client1 = provider.getOrCreateClient(context, HOST1);
      const client2 = provider.getOrCreateClient(context, HOST2);

      expect(client1.getHost()).to.equal(HOST1);
      expect(client2.getHost()).to.equal(HOST2);
      expect(client1).to.not.equal(client2);
    });

    it('should allow closing one host without affecting others', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client1 = provider.getOrCreateClient(context, HOST1);
      const client2 = provider.getOrCreateClient(context, HOST2);

      await provider.releaseClient(context, HOST1);

      expect(client1.isClosed()).to.be.true;
      expect(client2.isClosed()).to.be.false;
      expect(provider.getActiveClients().size).to.equal(1);
    });
  });

  describe('getRefCount', () => {
    it('should return 0 for non-existent host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      expect(provider.getRefCount(HOST1)).to.equal(0);
    });

    it('should return current reference count for existing host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      provider.getOrCreateClient(context, HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(1);

      provider.getOrCreateClient(context, HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(2);
    });
  });

  describe('getActiveClients', () => {
    it('should return empty map initially', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const clients = provider.getActiveClients();

      expect(clients.size).to.equal(0);
    });

    it('should return all active clients', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      const client1 = provider.getOrCreateClient(context, HOST1);
      const client2 = provider.getOrCreateClient(context, HOST2);

      const clients = provider.getActiveClients();

      expect(clients.size).to.equal(2);
      expect(clients.get(HOST1)).to.equal(client1);
      expect(clients.get(HOST2)).to.equal(client2);
    });

    it('should not include closed clients', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();

      provider.getOrCreateClient(context, HOST1);
      provider.getOrCreateClient(context, HOST2);

      await provider.releaseClient(context, HOST1);

      const clients = provider.getActiveClients();

      expect(clients.size).to.equal(1);
      expect(clients.has(HOST1)).to.be.false;
      expect(clients.has(HOST2)).to.be.true;
    });
  });

  describe('Context usage', () => {
    it('should use logger from context for all logging', () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const provider = new TelemetryClientProvider();

      provider.getOrCreateClient(context, HOST1);

      expect(logSpy.called).to.be.true;
      logSpy.getCalls().forEach((call) => {
        expect(call.args[0]).to.equal(LogLevel.debug);
      });
    });

    it('should log close errors at debug level', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider();
      const logSpy = sinon.spy(context.logger, 'log');

      const client = provider.getOrCreateClient(context, HOST1);
      sinon.stub(client, 'close').throws(new Error('Test error'));

      await provider.releaseClient(context, HOST1);

      const errorLogs = logSpy.getCalls().filter((call) => /error releasing/i.test(String(call.args[1])));
      expect(errorLogs.length).to.be.greaterThan(0);
      errorLogs.forEach((call) => {
        expect(call.args[0]).to.equal(LogLevel.debug);
      });
    });
  });
});
