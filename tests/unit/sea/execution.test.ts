// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { expect } from 'chai';
import sinon from 'sinon';
import SeaBackend from '../../../lib/sea/SeaBackend';
import SeaSessionBackend from '../../../lib/sea/SeaSessionBackend';
import SeaOperationBackend from '../../../lib/sea/SeaOperationBackend';
import {
  SeaNativeBinding,
  SeaNativeConnection,
  SeaNativeStatement,
  SeaExecuteOptions,
} from '../../../lib/sea/SeaNativeLoader';
import IClientContext, { ClientConfig } from '../../../lib/contracts/IClientContext';
import IDBSQLLogger, { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';

// -----------------------------------------------------------------------------
// Fakes — minimal stand-ins for the napi-rs generated surface and the
// IClientContext side of the abstraction. Keeping them inline avoids
// pulling in test-only fixtures from outside the sea/ namespace.
// -----------------------------------------------------------------------------

class FakeNativeStatement implements SeaNativeStatement {
  public closed = false;

  public cancelled = false;

  public async fetchNextBatch() {
    return null;
  }

  public async schema() {
    return { ipcBytes: Buffer.alloc(0) };
  }

  public async cancel() {
    this.cancelled = true;
  }

  public async close() {
    this.closed = true;
  }
}

class FakeNativeConnection implements SeaNativeConnection {
  public closed = false;

  public lastSql?: string;

  public lastOptions?: SeaExecuteOptions;

  public throwOnExecute: Error | null = null;

  public statementToReturn: FakeNativeStatement = new FakeNativeStatement();

  public async executeStatement(sql: string, options: SeaExecuteOptions): Promise<SeaNativeStatement> {
    if (this.throwOnExecute) {
      throw this.throwOnExecute;
    }
    this.lastSql = sql;
    this.lastOptions = options;
    return this.statementToReturn;
  }

  public async close(): Promise<void> {
    this.closed = true;
  }
}

function makeBinding(connection: SeaNativeConnection): SeaNativeBinding & {
  openSessionStub: sinon.SinonStub;
} {
  const openSessionStub = sinon.stub().resolves(connection);
  const binding: SeaNativeBinding = {
    version: () => 'test',
    openSession: openSessionStub,
    Connection: function Connection() {},
    Statement: function Statement() {},
  };
  return Object.assign(binding, { openSessionStub });
}

function makeContext(): IClientContext {
  const logger: IDBSQLLogger = {
    log(_level: LogLevel, _message: string): void {
      // no-op
    },
  };
  const config = {} as ClientConfig;
  return {
    getConfig: () => config,
    getLogger: () => logger,
    getConnectionProvider: async () => {
      throw new Error('not used by SEA backend');
    },
    getClient: async () => {
      throw new Error('not used by SEA backend');
    },
    getDriver: async () => {
      throw new Error('not used by SEA backend');
    },
  };
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

describe('SeaBackend', () => {
  it('connect() captures the connection options and validates PAT auth', async () => {
    const connection = new FakeNativeConnection();
    const binding = makeBinding(connection);
    const backend = new SeaBackend({ context: makeContext(), nativeBinding: binding });

    await backend.connect({
      host: 'example.databricks.com',
      path: '/sql/1.0/warehouses/abc',
      token: 'dapi-token',
    } as ConnectionOptions);

    // openSession should not have been called by connect()
    expect(binding.openSessionStub.called).to.equal(false);
  });

  it('connect() rejects non-PAT auth (M0 PAT-only)', async () => {
    const connection = new FakeNativeConnection();
    const binding = makeBinding(connection);
    const backend = new SeaBackend({ context: makeContext(), nativeBinding: binding });

    let thrown: unknown;
    try {
      await backend.connect({
        host: 'example.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
      } as ConnectionOptions);
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
    expect((thrown as Error).message).to.match(/access-token/);
  });

  it('connect() rejects missing token', async () => {
    const connection = new FakeNativeConnection();
    const binding = makeBinding(connection);
    const backend = new SeaBackend({ context: makeContext(), nativeBinding: binding });

    let thrown: unknown;
    try {
      await backend.connect({
        host: 'example.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: '',
      } as ConnectionOptions);
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
    expect((thrown as Error).message).to.match(/token is required/);
  });

  it('openSession() throws if connect() was not called', async () => {
    const connection = new FakeNativeConnection();
    const binding = makeBinding(connection);
    const backend = new SeaBackend({ context: makeContext(), nativeBinding: binding });

    let thrown: unknown;
    try {
      await backend.openSession({});
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
    expect((thrown as Error).message).to.match(/not connected/);
  });

  it('openSession() forwards hostName / httpPath / token to napi binding', async () => {
    const connection = new FakeNativeConnection();
    const binding = makeBinding(connection);
    const backend = new SeaBackend({ context: makeContext(), nativeBinding: binding });

    await backend.connect({
      host: 'workspace.example',
      path: '/sql/1.0/warehouses/xyz',
      token: 'dapi-token',
    } as ConnectionOptions);

    await backend.openSession({});

    expect(binding.openSessionStub.calledOnce).to.equal(true);
    const args = binding.openSessionStub.firstCall.args[0];
    expect(args).to.deep.equal({
      hostName: 'workspace.example',
      httpPath: '/sql/1.0/warehouses/xyz',
      token: 'dapi-token',
    });
  });

  it('openSession() returns a SeaSessionBackend wrapping the napi Connection', async () => {
    const connection = new FakeNativeConnection();
    const binding = makeBinding(connection);
    const backend = new SeaBackend({ context: makeContext(), nativeBinding: binding });

    await backend.connect({
      host: 'h',
      path: '/p',
      token: 't',
    } as ConnectionOptions);

    const sessionBackend = await backend.openSession({});
    expect(sessionBackend).to.be.instanceOf(SeaSessionBackend);
    expect(sessionBackend.id).to.be.a('string').and.have.length.greaterThan(0);
  });

  it('openSession() propagates initialCatalog / initialSchema / sessionConfig through to executeStatement', async () => {
    const connection = new FakeNativeConnection();
    const binding = makeBinding(connection);
    const backend = new SeaBackend({ context: makeContext(), nativeBinding: binding });

    await backend.connect({
      host: 'h',
      path: '/p',
      token: 't',
    } as ConnectionOptions);

    const session = await backend.openSession({
      initialCatalog: 'main',
      initialSchema: 'default',
      configuration: { 'spark.sql.execution.arrow.enabled': 'true' },
    });

    await session.executeStatement('SELECT 1', {});

    expect(connection.lastSql).to.equal('SELECT 1');
    expect(connection.lastOptions).to.deep.equal({
      initialCatalog: 'main',
      initialSchema: 'default',
      sessionConfig: { 'spark.sql.execution.arrow.enabled': 'true' },
    });
  });

  it('close() clears connection state without throwing', async () => {
    const connection = new FakeNativeConnection();
    const binding = makeBinding(connection);
    const backend = new SeaBackend({ context: makeContext(), nativeBinding: binding });
    await backend.connect({ host: 'h', path: '/p', token: 't' } as ConnectionOptions);
    await backend.close();

    let thrown: unknown;
    try {
      await backend.openSession({});
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
  });
});

describe('SeaSessionBackend', () => {
  function makeSession(connection: SeaNativeConnection, defaults = {}) {
    return new SeaSessionBackend({ connection, context: makeContext(), defaults });
  }

  it('executeStatement passes sql through verbatim', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.executeStatement('SELECT * FROM foo', {});
    expect(connection.lastSql).to.equal('SELECT * FROM foo');
  });

  it('executeStatement returns a SeaOperationBackend with an id', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    const op = await session.executeStatement('SELECT 1', {});
    expect(op).to.be.instanceOf(SeaOperationBackend);
    expect(op.id).to.be.a('string').and.have.length.greaterThan(0);
  });

  it('executeStatement merges session defaults into ExecuteOptions', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection, {
      initialCatalog: 'main',
      initialSchema: 'default',
      sessionConfig: { foo: 'bar' },
    });
    await session.executeStatement('SELECT 1', {});
    expect(connection.lastOptions).to.deep.equal({
      initialCatalog: 'main',
      initialSchema: 'default',
      sessionConfig: { foo: 'bar' },
    });
  });

  it('executeStatement rejects namedParameters (M1)', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    let thrown: unknown;
    try {
      await session.executeStatement('SELECT :x', { namedParameters: { x: 1 } });
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
    expect((thrown as Error).message).to.match(/parameters/);
  });

  it('executeStatement rejects ordinalParameters (M1)', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    let thrown: unknown;
    try {
      await session.executeStatement('SELECT ?', { ordinalParameters: [1] });
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
  });

  it('executeStatement rejects queryTimeout (M1)', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    let thrown: unknown;
    try {
      await session.executeStatement('SELECT 1', { queryTimeout: 30 });
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
    expect((thrown as Error).message).to.match(/queryTimeout/);
  });

  it('metadata methods throw deferred-M1 errors', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    for (const method of [
      'getInfo',
      'getTypeInfo',
      'getCatalogs',
      'getSchemas',
      'getTables',
      'getTableTypes',
      'getColumns',
      'getFunctions',
      'getPrimaryKeys',
      'getCrossReference',
    ] as const) {
      let thrown: unknown;
      try {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        await (session as any)[method]({});
      } catch (err) {
        thrown = err;
      }
      expect(thrown, `expected ${method} to throw`).to.be.instanceOf(HiveDriverError);
      expect((thrown as Error).message).to.match(/M1|not implemented/);
    }
  });

  it('close() forwards to the native connection', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    const status = await session.close();
    expect(connection.closed).to.equal(true);
    expect(status.isSuccess).to.equal(true);
  });

  it('close() is idempotent', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.close();
    // Second call should not re-invoke connection.close
    connection.closed = false;
    const status = await session.close();
    expect(connection.closed).to.equal(false);
    expect(status.isSuccess).to.equal(true);
  });

  it('executeStatement fails after close()', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.close();
    let thrown: unknown;
    try {
      await session.executeStatement('SELECT 1', {});
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
  });
});

describe('SeaOperationBackend', () => {
  function makeOperation(statement: SeaNativeStatement = new FakeNativeStatement()) {
    return new SeaOperationBackend({ statement, context: makeContext() });
  }

  it('id is a stable string', () => {
    const op = makeOperation();
    expect(op.id).to.equal(op.id);
    expect(op.id).to.be.a('string').and.have.length.greaterThan(0);
  });

  it('hasResultSet is true for M0', () => {
    const op = makeOperation();
    expect(op.hasResultSet).to.equal(true);
  });

  it('cancel() forwards to napi Statement', async () => {
    const stmt = new FakeNativeStatement();
    const op = makeOperation(stmt);
    await op.cancel();
    expect(stmt.cancelled).to.equal(true);
  });

  it('cancel() is idempotent', async () => {
    const stmt = new FakeNativeStatement();
    const op = makeOperation(stmt);
    await op.cancel();
    stmt.cancelled = false;
    await op.cancel();
    expect(stmt.cancelled).to.equal(false);
  });

  it('close() forwards to napi Statement', async () => {
    const stmt = new FakeNativeStatement();
    const op = makeOperation(stmt);
    await op.close();
    expect(stmt.closed).to.equal(true);
  });

  it('waitUntilReady() is a no-op (kernel internalises polling)', async () => {
    const op = makeOperation();
    await op.waitUntilReady();
  });

  it('fetchChunk() throws M1-deferred error (owned by sea-results)', async () => {
    const op = makeOperation();
    let thrown: unknown;
    try {
      await op.fetchChunk({ limit: 100 });
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
    expect((thrown as Error).message).to.match(/sea-results/);
  });
});
