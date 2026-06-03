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
import { SeaNativeBinding, SeaConnection, SeaStatement } from '../../../lib/sea/SeaNativeLoader';
import IClientContext, { ClientConfig } from '../../../lib/contracts/IClientContext';
import IDBSQLLogger, { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import ParameterError from '../../../lib/errors/ParameterError';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';

// -----------------------------------------------------------------------------
// Fakes — minimal stand-ins for the napi-rs generated surface and the
// IClientContext side of the abstraction. Keeping them inline avoids
// pulling in test-only fixtures from outside the sea/ namespace.
// -----------------------------------------------------------------------------

class FakeNativeStatement implements SeaStatement {
  public closed = false;

  public cancelled = false;

  // Mirrors the kernel `Statement.statementId` getter.
  public readonly statementId = '01ef-fake-statement-id';

  public async fetchNextBatch() {
    return null;
  }

  // schema() is synchronous on the merged-kernel binding.
  public schema() {
    return { ipcBytes: Buffer.alloc(0) };
  }

  public async cancel() {
    this.cancelled = true;
  }

  public async close() {
    this.closed = true;
  }

  // Status accessors added by the kernel's status-fields surface.
  public async numModifiedRows(): Promise<number | null> {
    return null;
  }

  public async displayMessage(): Promise<string | null> {
    return null;
  }

  public async diagnosticInfo(): Promise<string | null> {
    return null;
  }

  public async errorDetailsJson(): Promise<string | null> {
    return null;
  }
}

class FakeNativeConnection implements SeaConnection {
  public closed = false;

  public lastSql?: string;

  // Records the per-statement options object passed to executeStatement
  // (undefined for the no-options path) so param-forwarding can be asserted.
  public lastOptions?: unknown;

  // Records every metadata call as `[method, ...args]` so the session
  // backend's request → napi-argument mapping can be asserted.
  public metadataCalls: Array<unknown[]> = [];

  public throwOnExecute: Error | null = null;

  public statementToReturn: FakeNativeStatement = new FakeNativeStatement();

  // Mirrors the kernel `Connection.sessionId` getter.
  public readonly sessionId = '01ef-fake-session-id';

  // The merged kernel binding takes an optional per-statement `ExecuteOptions`
  // (positional/named params, statementConf, …). Record it for assertions.
  public async executeStatement(sql: string, options?: unknown): Promise<SeaStatement> {
    if (this.throwOnExecute) {
      throw this.throwOnExecute;
    }
    this.lastSql = sql;
    this.lastOptions = options;
    return this.statementToReturn;
  }

  // Async-submit path (PR 2 territory); present only so the fake satisfies
  // the full `Connection` surface. Not exercised by these tests.
  public submitStatement(): Promise<never> {
    throw new Error('submitStatement not used in this test');
  }

  private recordMetadata(method: string, args: unknown[]): Promise<SeaStatement> {
    this.metadataCalls.push([method, ...args]);
    return Promise.resolve(this.statementToReturn);
  }

  public listProcedures(catalog?: unknown, schemaPattern?: unknown, procedurePattern?: unknown) {
    return this.recordMetadata('listProcedures', [catalog, schemaPattern, procedurePattern]);
  }

  public listCatalogs() {
    return this.recordMetadata('listCatalogs', []);
  }

  public listSchemas(catalog?: unknown, schemaPattern?: unknown) {
    return this.recordMetadata('listSchemas', [catalog, schemaPattern]);
  }

  public listTables(catalog?: unknown, schemaPattern?: unknown, tablePattern?: unknown, tableTypes?: unknown) {
    return this.recordMetadata('listTables', [catalog, schemaPattern, tablePattern, tableTypes]);
  }

  public listColumns(catalog?: unknown, schemaPattern?: unknown, tablePattern?: unknown, columnPattern?: unknown) {
    return this.recordMetadata('listColumns', [catalog, schemaPattern, tablePattern, columnPattern]);
  }

  public listFunctions(catalog?: unknown, schemaPattern?: unknown, functionPattern?: unknown) {
    return this.recordMetadata('listFunctions', [catalog, schemaPattern, functionPattern]);
  }

  public listTableTypes() {
    return this.recordMetadata('listTableTypes', []);
  }

  public listTypeInfo() {
    return this.recordMetadata('listTypeInfo', []);
  }

  public getPrimaryKeys(catalog: unknown, schema: unknown, table: unknown) {
    return this.recordMetadata('getPrimaryKeys', [catalog, schema, table]);
  }

  public getCrossReference(
    parentCatalog: unknown,
    parentSchema: unknown,
    parentTable: unknown,
    foreignCatalog: unknown,
    foreignSchema: unknown,
    foreignTable: unknown,
  ) {
    return this.recordMetadata('getCrossReference', [
      parentCatalog,
      parentSchema,
      parentTable,
      foreignCatalog,
      foreignSchema,
      foreignTable,
    ]);
  }

  public async close(): Promise<void> {
    this.closed = true;
  }
}

function makeBinding(connection: SeaConnection): SeaNativeBinding & {
  openSessionStub: sinon.SinonStub;
} {
  const openSessionStub = sinon.stub().resolves(connection);
  // Structural cast through `unknown`: the binding type carries an `AuthMode`
  // const enum that can't be produced as a runtime value, so the whole fake
  // is cast rather than each member.
  const binding = {
    version: () => 'test',
    openSession: openSessionStub,
    Connection: function Connection() {},
    Statement: function Statement() {},
  } as unknown as SeaNativeBinding;
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

  // sea-auth-u2m: `databricks-oauth` with no id/secret is now the U2M happy
  // path (M0 was PAT-only, but the OAuth M2M+U2M feature on sea-auth-u2m
  // accepts the full set of `databricks-oauth` variants). M2M/U2M flow-
  // dispatch coverage lives in auth-m2m.test.ts / auth-u2m.test.ts;
  // out-of-scope auth modes are now whatever neither PAT nor
  // `databricks-oauth` covers (e.g. `token-provider`, `external-token`).
  it('connect() rejects unsupported auth modes (non-PAT, non-OAuth)', async () => {
    const connection = new FakeNativeConnection();
    const binding = makeBinding(connection);
    const backend = new SeaBackend({ context: makeContext(), nativeBinding: binding });

    let thrown: unknown;
    try {
      await backend.connect({
        host: 'example.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        authType: 'token-provider',
      } as any);
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
    expect((thrown as Error).message).to.match(/unsupported auth mode/);
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
    // After sea-integration merge, missing-token validation goes through
    // SeaAuth.buildSeaConnectionOptions which throws AuthenticationError
    // (extends HiveDriverError) with the "non-empty PAT" message.
    expect((thrown as Error).message).to.match(/non-empty PAT/);
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
    // sea-auth-u2m introduced the discriminated SeaNativeConnectionOptions
    // shape with a leading `authMode` tag — `'Pat'` for the PAT branch.
    expect(args).to.deep.equal({
      hostName: 'workspace.example',
      httpPath: '/sql/1.0/warehouses/xyz',
      authMode: 'Pat',
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

  it('openSession() forwards initialCatalog / initialSchema / configuration to the napi openSession call (not per-statement)', async () => {
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

    // The defaults reach the kernel via `Session::builder().defaults()` +
    // `.session_conf()`, applied on `CreateSession`. Assert they were
    // folded into the napi `openSession` arg.
    expect(binding.openSessionStub.calledOnce).to.equal(true);
    expect(binding.openSessionStub.firstCall.args[0]).to.deep.include({
      authMode: 'Pat',
      token: 't',
      catalog: 'main',
      schema: 'default',
      sessionConf: { 'spark.sql.execution.arrow.enabled': 'true' },
    });

    // And the SQL still threads through executeStatement (now with no
    // per-statement options).
    await session.executeStatement('SELECT 1', {});
    expect(connection.lastSql).to.equal('SELECT 1');
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
  function makeSession(connection: SeaConnection) {
    return new SeaSessionBackend({ connection, context: makeContext() });
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

  it('executeStatement forwards ordinalParameters as napi positionalParams', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.executeStatement('SELECT ?', { ordinalParameters: [42, 'hi'] });
    const options = connection.lastOptions as { positionalParams?: Array<{ sqlType: string; value?: string }> };
    expect(options, 'options should be passed').to.not.equal(undefined);
    expect(options.positionalParams).to.have.length(2);
    expect(options.positionalParams?.[0]).to.deep.equal({ sqlType: 'INTEGER', value: '42' });
    expect(options.positionalParams?.[1]).to.deep.equal({ sqlType: 'STRING', value: 'hi' });
  });

  it('executeStatement forwards namedParameters as napi namedParams (:name carried)', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.executeStatement('SELECT :x', { namedParameters: { x: 7 } });
    const options = connection.lastOptions as { namedParams?: Array<{ name: string; sqlType: string; value?: string }> };
    expect(options.namedParams).to.have.length(1);
    expect(options.namedParams?.[0]).to.deep.equal({ name: 'x', sqlType: 'INTEGER', value: '7' });
  });

  it('executeStatement sends no options object on the no-params path', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.executeStatement('SELECT 1', {});
    expect(connection.lastOptions).to.equal(undefined);
  });

  it('executeStatement rejects mixing ordinal and named parameters with the same ParameterError as Thrift', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    let thrown: unknown;
    try {
      await session.executeStatement('SELECT ?, :x', { ordinalParameters: [1], namedParameters: { x: 2 } });
    } catch (err) {
      thrown = err;
    }
    // Cross-backend parity: ThriftSessionBackend throws ParameterError with this
    // exact message, so a caller catching ParameterError behaves identically.
    expect(thrown).to.be.instanceOf(ParameterError);
    expect((thrown as Error).message).to.equal('Driver does not support both ordinal and named parameters.');
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

  // These Thrift-path options are not honored on SEA M0. Rejecting them
  // (rather than silently ignoring) is the contract a caller/agent needs:
  // a silent no-op gives zero signal to debug.
  for (const { name, options, re } of [
    { name: 'queryTags', options: { queryTags: { team: 'x' } }, re: /queryTags/ },
    { name: 'useLZ4Compression', options: { useLZ4Compression: true }, re: /useLZ4Compression/ },
    { name: 'stagingAllowedLocalPath', options: { stagingAllowedLocalPath: '/tmp' }, re: /stagingAllowedLocalPath/ },
  ] as const) {
    it(`executeStatement rejects ${name} rather than silently ignoring it`, async () => {
      const connection = new FakeNativeConnection();
      const session = makeSession(connection);
      let thrown: unknown;
      try {
        await session.executeStatement('SELECT 1', options);
      } catch (err) {
        thrown = err;
      }
      expect(thrown).to.be.instanceOf(HiveDriverError);
      expect((thrown as Error).message).to.match(re);
    });
  }

  // Metadata calls forward to the kernel's metadata surface and wrap the
  // returned napi `Statement` as a `SeaOperationBackend`. Each case asserts
  // the request → napi-argument mapping (the only logic the driver owns).
  it('metadata calls forward to the napi binding with mapped arguments', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);

    const op = await session.getCatalogs({});
    expect(op).to.be.instanceOf(SeaOperationBackend);

    await session.getSchemas({ catalogName: 'main', schemaName: 'def%' });
    await session.getTables({ catalogName: 'main', schemaName: 'def', tableName: 't%', tableTypes: ['TABLE', 'VIEW'] });
    await session.getColumns({ catalogName: 'main', schemaName: 'def', tableName: 't', columnName: 'c%' });
    await session.getFunctions({ catalogName: 'main', schemaName: 'def', functionName: 'f%' });
    await session.getTableTypes({});
    await session.getTypeInfo({});
    await session.getPrimaryKeys({ catalogName: 'main', schemaName: 'def', tableName: 't' });
    await session.getCrossReference({
      parentCatalogName: 'pc',
      parentSchemaName: 'ps',
      parentTableName: 'pt',
      foreignCatalogName: 'fc',
      foreignSchemaName: 'fs',
      foreignTableName: 'ft',
    });

    expect(connection.metadataCalls).to.deep.equal([
      ['listCatalogs'],
      ['listSchemas', 'main', 'def%'],
      ['listTables', 'main', 'def', 't%', ['TABLE', 'VIEW']],
      ['listColumns', 'main', 'def', 't', 'c%'],
      ['listFunctions', 'main', 'def', 'f%'],
      ['listTableTypes'],
      ['listTypeInfo'],
      ['getPrimaryKeys', 'main', 'def', 't'],
      ['getCrossReference', 'pc', 'ps', 'pt', 'fc', 'fs', 'ft'],
    ]);
  });

  it('getPrimaryKeys rejects an omitted catalog up front (the kernel requires one)', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    for (const request of [{ schemaName: 'def', tableName: 't' }, { catalogName: '', schemaName: 'def', tableName: 't' }]) {
      let thrown: unknown;
      try {
        // eslint-disable-next-line no-await-in-loop
        await session.getPrimaryKeys(request);
      } catch (err) {
        thrown = err;
      }
      expect(thrown, `expected reject for ${JSON.stringify(request)}`).to.be.instanceOf(HiveDriverError);
      expect((thrown as Error).message).to.match(/requires a catalog/);
    }
    // The kernel call must NOT be reached (no empty-identifier sent over FFI).
    expect(connection.metadataCalls.filter((c) => c[0] === 'getPrimaryKeys')).to.have.length(0);
  });

  it('getInfo synthesizes the three server-answered info types and rejects the rest', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    // CLI_DBMS_NAME (17) → "Spark SQL".
    const info = await session.getInfo(17);
    expect(info.getValue()).to.equal('Spark SQL');
    // An unsupported info type (e.g. CLI_MAX_DRIVER_CONNECTIONS) is rejected,
    // mirroring the Thrift server's reject-unsupported behaviour.
    let thrown: unknown;
    try {
      await session.getInfo(0);
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
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
  function makeOperation(statement: SeaStatement = new FakeNativeStatement()) {
    return new SeaOperationBackend({ statement, context: makeContext() });
  }

  it('id is a stable string', () => {
    const op = makeOperation();
    expect(op.id).to.equal(op.id);
    expect(op.id).to.be.a('string').and.have.length.greaterThan(0);
  });

  it('hasResultSet is true for M0', () => {
    const op = makeOperation();
    expect(op.hasResultSet()).to.equal(true);
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

  // Note: after sea-integration merge, fetchChunk is no longer a stub —
  // the sea-results SeaResultsProvider + ArrowResultConverter pipeline
  // implements the real fetch path. Full coverage lives in
  // tests/unit/sea/SeaOperationBackend.test.ts and the parity-gate e2e
  // at tests/e2e/sea/results-e2e.test.ts.
});
