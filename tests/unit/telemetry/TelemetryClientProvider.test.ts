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
      const provider = new TelemetryClientProvider(context);

      expect(provider.getActiveClients().size).to.equal(0);
    });

    it('should log creation at debug level', () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');

      new TelemetryClientProvider(context);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/created.*telemetryclientprovider/i))).to.be.true;
    });
  });

  describe('getOrCreateClient', () => {
    it('should create one client per host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const client1 = provider.getOrCreateClient(HOST1);
      const client2 = provider.getOrCreateClient(HOST2);

      expect(client1).to.be.instanceOf(TelemetryClient);
      expect(client2).to.be.instanceOf(TelemetryClient);
      expect(client1).to.not.equal(client2);
      expect(provider.getActiveClients().size).to.equal(2);
    });

    it('should share client across multiple connections to same host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const client1 = provider.getOrCreateClient(HOST1);
      const client2 = provider.getOrCreateClient(HOST1);
      const client3 = provider.getOrCreateClient(HOST1);

      expect(client1).to.equal(client2);
      expect(client2).to.equal(client3);
      expect(provider.getActiveClients().size).to.equal(1);
    });

    it('should increment reference count on each call', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      provider.getOrCreateClient(HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(1);

      provider.getOrCreateClient(HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(2);

      provider.getOrCreateClient(HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(3);
    });

    it('should log client creation at debug level', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);
      const logSpy = sinon.spy(context.logger, 'log');

      provider.getOrCreateClient(HOST1);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/created new telemetryclient/i))).to.be.true;
    });

    it('should log reference count at debug level', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);
      const logSpy = sinon.spy(context.logger, 'log');

      provider.getOrCreateClient(HOST1);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/reference count for/i).and(sinon.match(/: 1$/)))).to.be
        .true;
    });

    it('should pass context to TelemetryClient', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const client = provider.getOrCreateClient(HOST1);

      expect(client.getHost()).to.equal(HOST1);
    });
  });

  describe('releaseClient', () => {
    it('should decrement reference count on release', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      provider.getOrCreateClient(HOST1);
      provider.getOrCreateClient(HOST1);
      provider.getOrCreateClient(HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(3);

      provider.releaseClient(HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(2);

      provider.releaseClient(HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(1);
    });

    it('should close client when reference count reaches zero', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const client = provider.getOrCreateClient(HOST1);
      const closeSpy = sinon.spy(client, 'close');

      provider.releaseClient(HOST1);

      expect(closeSpy.calledOnce).to.be.true;
      expect(client.isClosed()).to.be.true;
    });

    it('should remove client from map when reference count reaches zero', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      provider.getOrCreateClient(HOST1);
      expect(provider.getActiveClients().size).to.equal(1);

      provider.releaseClient(HOST1);

      expect(provider.getActiveClients().size).to.equal(0);
      expect(provider.getRefCount(HOST1)).to.equal(0);
    });

    it('should NOT close client while other connections exist', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const client = provider.getOrCreateClient(HOST1);
      provider.getOrCreateClient(HOST1);
      provider.getOrCreateClient(HOST1);
      const closeSpy = sinon.spy(client, 'close');

      provider.releaseClient(HOST1);

      expect(closeSpy.called).to.be.false;
      expect(client.isClosed()).to.be.false;
      expect(provider.getActiveClients().size).to.equal(1);
    });

    it('should handle releasing non-existent client gracefully', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);
      const logSpy = sinon.spy(context.logger, 'log');

      provider.releaseClient(HOST1);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/no telemetryclient found/i))).to.be.true;
    });

    it('should log reference count decrease at debug level', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);
      const logSpy = sinon.spy(context.logger, 'log');

      provider.getOrCreateClient(HOST1);
      provider.getOrCreateClient(HOST1);

      provider.releaseClient(HOST1);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/reference count for/i).and(sinon.match(/: 1$/)))).to.be
        .true;
    });

    it('should log client closure at debug level', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);
      const logSpy = sinon.spy(context.logger, 'log');

      provider.getOrCreateClient(HOST1);
      provider.releaseClient(HOST1);

      expect(logSpy.calledWith(LogLevel.debug, sinon.match(/closed and removed telemetryclient/i))).to.be.true;
    });

    it('should swallow Error during client closure', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const client = provider.getOrCreateClient(HOST1);
      const error = new Error('Close error');
      sinon.stub(client, 'close').throws(error);
      const logSpy = sinon.spy(context.logger, 'log');

      provider.releaseClient(HOST1);

      expect(
        logSpy.calledWith(
          LogLevel.debug,
          sinon.match(/error releasing telemetryclient/i).and(sinon.match(/close error/i)),
        ),
      ).to.be.true;
    });

    it('should swallow non-Error throws during client closure', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const client = provider.getOrCreateClient(HOST1);
      // Non-Error throws — string, null, undefined — must not escape the catch.
      sinon.stub(client, 'close').callsFake(() => {
        // eslint-disable-next-line no-throw-literal
        throw 'stringy-error';
      });
      const logSpy = sinon.spy(context.logger, 'log');

      expect(() => provider.releaseClient(HOST1)).to.not.throw();
      expect(
        logSpy.calledWith(
          LogLevel.debug,
          sinon.match(/error releasing telemetryclient/i).and(sinon.match(/stringy-error/)),
        ),
      ).to.be.true;
    });

    it('should not throw or corrupt state on double-release', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      // One get, two releases — the second must not throw and must not
      // leave the provider in a state where refCount is negative.
      provider.getOrCreateClient(HOST1);
      provider.releaseClient(HOST1);

      expect(() => provider.releaseClient(HOST1)).to.not.throw();
      expect(provider.getRefCount(HOST1)).to.equal(0);
      expect(provider.getActiveClients().size).to.equal(0);
    });

    it('should return a fresh non-closed client after full release', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const first = provider.getOrCreateClient(HOST1);
      provider.releaseClient(HOST1);
      expect(first.isClosed()).to.be.true;

      const second = provider.getOrCreateClient(HOST1);
      expect(second).to.not.equal(first);
      expect(second.isClosed()).to.be.false;
      expect(provider.getRefCount(HOST1)).to.equal(1);
    });
  });

  describe('Host normalization', () => {
    it('should treat scheme, case, port and trailing slash as the same host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const a = provider.getOrCreateClient('workspace.cloud.databricks.com');
      const b = provider.getOrCreateClient('https://workspace.cloud.databricks.com');
      const c = provider.getOrCreateClient('https://WorkSpace.CLOUD.databricks.com/');
      const d = provider.getOrCreateClient('workspace.cloud.databricks.com:443');
      const e = provider.getOrCreateClient('  workspace.cloud.databricks.com.  ');

      expect(a).to.equal(b);
      expect(a).to.equal(c);
      expect(a).to.equal(d);
      expect(a).to.equal(e);
      expect(provider.getActiveClients().size).to.equal(1);
      expect(provider.getRefCount('workspace.cloud.databricks.com')).to.equal(5);
    });

    it('should release under an alias correctly', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      provider.getOrCreateClient('example.com');
      provider.releaseClient('HTTPS://Example.COM/');

      expect(provider.getRefCount('example.com')).to.equal(0);
      expect(provider.getActiveClients().size).to.equal(0);
    });
  });

  describe('Reference counting', () => {
    it('should track reference counts independently per host', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      provider.getOrCreateClient(HOST1);
      provider.getOrCreateClient(HOST1);
      provider.getOrCreateClient(HOST2);
      provider.getOrCreateClient(HOST2);
      provider.getOrCreateClient(HOST2);

      expect(provider.getRefCount(HOST1)).to.equal(2);
      expect(provider.getRefCount(HOST2)).to.equal(3);

      provider.releaseClient(HOST1);

      expect(provider.getRefCount(HOST1)).to.equal(1);
      expect(provider.getRefCount(HOST2)).to.equal(3);
    });

    it('should close only last connection for each host', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const client1 = provider.getOrCreateClient(HOST1);
      provider.getOrCreateClient(HOST1);
      const client2 = provider.getOrCreateClient(HOST2);

      provider.releaseClient(HOST1);
      expect(client1.isClosed()).to.be.false;
      expect(provider.getActiveClients().size).to.equal(2);

      provider.releaseClient(HOST1);
      expect(client1.isClosed()).to.be.true;
      expect(provider.getActiveClients().size).to.equal(1);

      provider.releaseClient(HOST2);
      expect(client2.isClosed()).to.be.true;
      expect(provider.getActiveClients().size).to.equal(0);
    });
  });

  describe('Per-host isolation', () => {
    it('should isolate clients by host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const client1 = provider.getOrCreateClient(HOST1);
      const client2 = provider.getOrCreateClient(HOST2);

      expect(client1.getHost()).to.equal(HOST1);
      expect(client2.getHost()).to.equal(HOST2);
      expect(client1).to.not.equal(client2);
    });

    it('should allow closing one host without affecting others', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const client1 = provider.getOrCreateClient(HOST1);
      const client2 = provider.getOrCreateClient(HOST2);

      provider.releaseClient(HOST1);

      expect(client1.isClosed()).to.be.true;
      expect(client2.isClosed()).to.be.false;
      expect(provider.getActiveClients().size).to.equal(1);
    });
  });

  describe('getRefCount', () => {
    it('should return 0 for non-existent host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      expect(provider.getRefCount(HOST1)).to.equal(0);
    });

    it('should return current reference count for existing host', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      provider.getOrCreateClient(HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(1);

      provider.getOrCreateClient(HOST1);
      expect(provider.getRefCount(HOST1)).to.equal(2);
    });
  });

  describe('getActiveClients', () => {
    it('should return empty map initially', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const clients = provider.getActiveClients();

      expect(clients.size).to.equal(0);
    });

    it('should return all active clients', () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      const client1 = provider.getOrCreateClient(HOST1);
      const client2 = provider.getOrCreateClient(HOST2);

      const clients = provider.getActiveClients();

      expect(clients.size).to.equal(2);
      expect(clients.get(HOST1)).to.equal(client1);
      expect(clients.get(HOST2)).to.equal(client2);
    });

    it('should not include closed clients', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);

      provider.getOrCreateClient(HOST1);
      provider.getOrCreateClient(HOST2);

      provider.releaseClient(HOST1);

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
      const provider = new TelemetryClientProvider(context);

      provider.getOrCreateClient(HOST1);

      expect(logSpy.called).to.be.true;
      logSpy.getCalls().forEach((call) => {
        expect(call.args[0]).to.equal(LogLevel.debug);
      });
    });

    it('should log close errors at debug level', async () => {
      const context = new ClientContextStub();
      const provider = new TelemetryClientProvider(context);
      const logSpy = sinon.spy(context.logger, 'log');

      const client = provider.getOrCreateClient(HOST1);
      sinon.stub(client, 'close').throws(new Error('Test error'));

      provider.releaseClient(HOST1);

      const errorLogs = logSpy.getCalls().filter((call) => /error releasing/i.test(String(call.args[1])));
      expect(errorLogs.length).to.be.greaterThan(0);
      errorLogs.forEach((call) => {
        expect(call.args[0]).to.equal(LogLevel.debug);
      });
    });
  });
});
