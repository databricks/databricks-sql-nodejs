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
import Int64 from 'node-int64';
import expectNativeConnectionOptions from './_helpers/nativeOptions';
import SeaBackend from '../../../lib/sea/SeaBackend';
import SeaSessionBackend from '../../../lib/sea/SeaSessionBackend';
import SeaOperationBackend from '../../../lib/sea/SeaOperationBackend';
import { SeaNativeBinding, SeaConnection, SeaStatement } from '../../../lib/sea/SeaNativeLoader';
import IClientContext, { ClientConfig } from '../../../lib/contracts/IClientContext';
import IDBSQLLogger, { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import ParameterError from '../../../lib/errors/ParameterError';
import OperationStateError, { OperationStateErrorCode } from '../../../lib/errors/OperationStateError';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import { OperationState } from '../../../lib/contracts/OperationStatus';

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

  // Status accessors added by the kernel's status-fields surface. The values
  // are configurable so a test can assert non-null rich-status (e.g. a DML
  // `numModifiedRows`) propagates through `op.status()`; they default to all-
  // null, matching a SELECT / metadata statement that carries none.
  public rich: SeaRichStatusValues = {
    numModifiedRows: null,
    displayMessage: null,
    diagnosticInfo: null,
    errorDetailsJson: null,
  };

  // Counts every rich-field accessor call so a test can assert the backend
  // memoizes the read on a terminal statement (re-`status()` must not re-hit
  // the FFI accessors).
  public richReads = 0;

  public async numModifiedRows(): Promise<number | null> {
    this.richReads += 1;
    return this.rich.numModifiedRows;
  }

  public async displayMessage(): Promise<string | null> {
    this.richReads += 1;
    return this.rich.displayMessage;
  }

  public async diagnosticInfo(): Promise<string | null> {
    this.richReads += 1;
    return this.rich.diagnosticInfo;
  }

  public async errorDetailsJson(): Promise<string | null> {
    this.richReads += 1;
    return this.rich.errorDetailsJson;
  }
}

interface SeaRichStatusValues {
  numModifiedRows: number | null;
  displayMessage: string | null;
  diagnosticInfo: string | null;
  errorDetailsJson: string | null;
}

/**
 * Fake `AsyncStatement` (the `submitStatement` return). `status()` reports a
 * configurable state (default Succeeded); `awaitResult()` yields a fetch handle
 * (reuses `FakeNativeStatement`'s fetchNextBatch/schema surface).
 */
class FakeAsyncStatement {
  public cancelled = false;

  public closed = false;

  public statusCalls = 0;

  public awaitResultError: Error | null = null;

  // Successive status() returns drain this queue; the last value sticks.
  private readonly states: string[];

  public readonly statementId = '01ef-fake-async-id';

  constructor(
    statusValue: string | string[] = 'Succeeded',
    public readonly resultHandle: FakeNativeStatement = new FakeNativeStatement(),
  ) {
    this.states = Array.isArray(statusValue) ? [...statusValue] : [statusValue];
  }

  public async status(): Promise<string> {
    this.statusCalls += 1;
    return this.states.length > 1 ? (this.states.shift() as string) : this.states[0];
  }

  public async awaitResult(): Promise<FakeNativeStatement> {
    if (this.awaitResultError) {
      throw this.awaitResultError;
    }
    return this.resultHandle;
  }

  public async cancel(): Promise<void> {
    this.cancelled = true;
  }

  public async close(): Promise<void> {
    this.closed = true;
  }

  // Extended status accessors exposed on the napi AsyncStatement. The kernel
  // populates them off the terminal GetStatement poll (a DML's count rides on
  // that response); configurable here to assert the driver surfaces them
  // through op.status() on the async path.
  public rich: SeaRichStatusValues = {
    numModifiedRows: null,
    displayMessage: null,
    diagnosticInfo: null,
    errorDetailsJson: null,
  };

  public async numModifiedRows(): Promise<number | null> {
    return this.rich.numModifiedRows;
  }

  public async displayMessage(): Promise<string | null> {
    return this.rich.displayMessage;
  }

  public async diagnosticInfo(): Promise<string | null> {
    return this.rich.diagnosticInfo;
  }

  public async errorDetailsJson(): Promise<string | null> {
    return this.rich.errorDetailsJson;
  }
}

/**
 * Fake `CancellableExecution` (the `executeStatementCancellable` return — the
 * sync `runAsync: false` query path). `result()` drives the (already-terminal,
 * in the fake) execution and yields the terminal statement fetch handle;
 * `cancel()` flips a flag and, if armed, makes a pending `result()` reject with
 * a Cancelled-shaped kernel error to model mid-compute interruption.
 */
class FakeCancellableExecution {
  public cancelled = false;

  public resultCalls = 0;

  public resultError: Error | null = null;

  // Mirrors the real `CancellableExecution.statementId`: `null` until the
  // initial execute round-trip publishes the server id mid-`result()`. The
  // resolved `Statement` (resultHandle) carries the id (`FakeNativeStatement`).
  public readonly statementId: string | null = null;

  // When set, the result() promise stays pending until cancel() rejects it,
  // modelling a still-running blocking execute that a concurrent cancel aborts.
  private pendingResolve?: (stmt: FakeNativeStatement) => void;

  private pendingReject?: (err: Error) => void;

  constructor(public readonly resultHandle: FakeNativeStatement = new FakeNativeStatement()) {}

  // When true, result() does not resolve until cancel()/an error fires.
  public block = false;

  public async result(): Promise<FakeNativeStatement> {
    this.resultCalls += 1;
    if (this.resultError) {
      throw this.resultError;
    }
    if (this.block) {
      return new Promise<FakeNativeStatement>((resolve, reject) => {
        this.pendingResolve = resolve;
        this.pendingReject = reject;
      });
    }
    return this.resultHandle;
  }

  public async cancel(): Promise<void> {
    this.cancelled = true;
    // Model the server flipping the statement terminal: a parked result()
    // rejects with the kernel's Cancelled error envelope.
    if (this.pendingReject) {
      const err = new Error('statement cancelled');
      this.pendingReject(err);
      this.pendingReject = undefined;
      this.pendingResolve = undefined;
    }
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

  // Last AsyncStatement handed out by submitStatement (the async query path).
  public lastAsyncStatement?: FakeAsyncStatement;

  // The async submit state(s) the next FakeAsyncStatement should report.
  public submitStatusValue: string | string[] = 'Succeeded';

  // Last CancellableExecution handed out by executeStatementCancellable (the
  // sync `runAsync: false` query path — the DEFAULT).
  public lastCancellableExecution?: FakeCancellableExecution;

  // The bare blocking executeStatement path: the SEA backend's sync default
  // routes through executeStatementCancellable (below), but the binding still
  // exposes this for completeness.
  public async executeStatement(sql: string, options?: unknown): Promise<SeaStatement> {
    if (this.throwOnExecute) {
      throw this.throwOnExecute;
    }
    this.lastSql = sql;
    this.lastOptions = options;
    return this.statementToReturn;
  }

  // Rich status the next sync-execute's terminal Statement should report
  // (e.g. a DML `numModifiedRows`). Defaults to all-null (a SELECT).
  public richStatus?: SeaRichStatusValues;

  // Sync (`runAsync: false`, the DEFAULT) query path: records sql + options and
  // returns a pending CancellableExecution whose result() drives the execute.
  public async executeStatementCancellable(sql: string, options?: unknown): Promise<any> {
    if (this.throwOnExecute) {
      throw this.throwOnExecute;
    }
    this.lastSql = sql;
    this.lastOptions = options;
    const resultHandle = new FakeNativeStatement();
    if (this.richStatus) {
      resultHandle.rich = this.richStatus;
    }
    this.lastCancellableExecution = new FakeCancellableExecution(resultHandle);
    return this.lastCancellableExecution;
  }

  // directResults (`runAsync: false`, the DEFAULT) query path: records sql +
  // options and returns either a terminal `Statement` (Completed arm) or — when
  // `directReturnsRunning` is set — a pending `AsyncStatement` (Running arm),
  // the two arms `SeaSessionBackend.executeStatement` feature-detects via
  // `awaitResult`.
  public directReturnsRunning = false;

  public async executeStatementDirect(sql: string, options?: unknown): Promise<any> {
    if (this.throwOnExecute) {
      throw this.throwOnExecute;
    }
    this.lastSql = sql;
    this.lastOptions = options;
    if (this.directReturnsRunning) {
      this.lastAsyncStatement = new FakeAsyncStatement(this.submitStatusValue);
      return this.lastAsyncStatement;
    }
    return this.statementToReturn;
  }

  // Async-submit path: records sql + per-statement options (for forwarding
  // assertions) and returns a pending AsyncStatement.
  public async submitStatement(sql: string, options?: unknown): Promise<any> {
    if (this.throwOnExecute) {
      throw this.throwOnExecute;
    }
    this.lastSql = sql;
    this.lastOptions = options;
    this.lastAsyncStatement = new FakeAsyncStatement(this.submitStatusValue);
    return this.lastAsyncStatement;
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

function makeContext(logger?: IDBSQLLogger): IClientContext {
  const log: IDBSQLLogger = logger ?? {
    log(_level: LogLevel, _message: string): void {
      // no-op
    },
  };
  const config = {} as ClientConfig;
  return {
    getConfig: () => config,
    getLogger: () => log,
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
    // `intervalsAsString: true` is always set so the SEA result shape is a
    // byte-compatible drop-in for the Thrift backend (interval-as-string).
    expectNativeConnectionOptions(args, {
      hostName: 'workspace.example',
      httpPath: '/sql/1.0/warehouses/xyz',
      authMode: 'Pat',
      token: 'dapi-token',
      intervalsAsString: true,
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

  it('executeStatement (sync default) routes a still-running query through the AsyncStatement arm', async () => {
    // The directResults Running arm — a query that did NOT finish within the
    // server inline wait comes back as an AsyncStatement (poll/cancel handle).
    // This is the branch the whole PR exists to add (Node mid-run cancel).
    const connection = new FakeNativeConnection();
    connection.directReturnsRunning = true;
    const session = makeSession(connection);
    const op = await session.executeStatement('SELECT slow', {});
    expect(op).to.be.instanceOf(SeaOperationBackend);
    // The Running arm was taken: an AsyncStatement was constructed + wired
    // (not the terminal `statement` arm).
    expect(connection.lastAsyncStatement, 'AsyncStatement (Running) arm should be taken').to.not.equal(undefined);
    // Driving the op polls the async handle's status() — the polling arm.
    await op.waitUntilReady();
    expect(connection.lastAsyncStatement!.statusCalls, 'async handle polled via status()').to.be.greaterThan(0);
  });

  it('executeStatement (sync default) AsyncStatement arm: op.cancel() reaches the running statement', async () => {
    // The point of directResults on single-threaded Node: the returned op holds
    // a handle to the still-running statement, so op.cancel() can abort it.
    const connection = new FakeNativeConnection();
    connection.directReturnsRunning = true;
    const session = makeSession(connection);
    const op = await session.executeStatement('SELECT slow', {});
    await op.cancel();
    expect(connection.lastAsyncStatement!.cancelled, 'cancel reaches the running statement').to.equal(true);
  });

  it('executeStatement (sync default) routes a fast query through the terminal Statement arm', async () => {
    // Contrast: a query that finished within the inline wait comes back as a
    // terminal Statement (result inline) — no AsyncStatement is created.
    const connection = new FakeNativeConnection(); // directReturnsRunning = false (default)
    const session = makeSession(connection);
    const op = await session.executeStatement('SELECT 1', {});
    expect(connection.lastAsyncStatement, 'no AsyncStatement arm for a terminal query').to.equal(undefined);
    await op.cancel();
    expect(connection.statementToReturn.cancelled, 'cancel reaches the terminal statement').to.equal(true);
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
    const options = connection.lastOptions as {
      namedParams?: Array<{ name: string; sqlType: string; value?: string }>;
    };
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

  it('executeStatement (sync default) does NOT forward queryTimeout — no-op on SEA', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.executeStatement('SELECT 1', { queryTimeout: 30 });
    // queryTimeout is a no-op on SEA (SQL Warehouses use STATEMENT_TIMEOUT). It
    // must NOT be mapped to the kernel's `wait_timeout` (the inline-hold window),
    // so nothing is forwarded onto the napi options.
    expect((connection.lastOptions as { queryTimeoutSecs?: number } | undefined)?.queryTimeoutSecs).to.equal(undefined);
  });

  it('executeStatement (runAsync: true) does NOT forward queryTimeout — no-op on SEA', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.executeStatement('SELECT 1', { queryTimeout: 30, runAsync: true });
    expect((connection.lastOptions as { queryTimeoutSecs?: number } | undefined)?.queryTimeoutSecs).to.equal(undefined);
  });

  it('executeStatement forwards rowLimit', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.executeStatement('SELECT 1', { rowLimit: 100 });
    expect((connection.lastOptions as { rowLimit?: number }).rowLimit).to.equal(100);
  });

  it('executeStatement serialises queryTags into statementConf.query_tags', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.executeStatement('SELECT 1', { queryTags: { team: 'x', env: 'prod' } });
    const conf = (connection.lastOptions as { statementConf?: Record<string, string> }).statementConf;
    expect(conf).to.have.property('query_tags');
    expect(conf?.query_tags).to.contain('team:x').and.to.contain('env:prod');
  });

  it('executeStatement merges explicit statementConf with serialised queryTags', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.executeStatement('SELECT 1', {
      statementConf: { 'spark.sql.ansi.enabled': 'true' },
      queryTags: { team: 'x' },
    });
    const conf = (connection.lastOptions as { statementConf?: Record<string, string> }).statementConf;
    expect(conf?.['spark.sql.ansi.enabled']).to.equal('true');
    expect(conf?.query_tags).to.contain('team:x');
  });

  it('queryTags wins over a query_tags key in statementConf (precedence on collision)', async () => {
    const connection = new FakeNativeConnection();
    const session = makeSession(connection);
    await session.executeStatement('SELECT 1', {
      statementConf: { query_tags: 'manual-raw-value' },
      queryTags: { team: 'x' },
    });
    const conf = (connection.lastOptions as { statementConf?: Record<string, string> }).statementConf;
    // The structured `queryTags` option overwrites a raw `query_tags` conf key —
    // a single, predictable wire value rather than two competing ones.
    expect(conf?.query_tags).to.contain('team:x').and.to.not.equal('manual-raw-value');
  });

  it('maps a submit-time kernel error via logAndMapError on both paths', async () => {
    const envelope = `__databricks_error__:${JSON.stringify({ code: 'SqlError', message: 'SUBMIT_BOOM' })}`;
    for (const opts of [{}, { runAsync: true }]) {
      const connection = new FakeNativeConnection();
      connection.throwOnExecute = new Error(envelope); // fails executeStatementDirect / submitStatement
      const session = makeSession(connection);
      let thrown: unknown;
      try {
        // eslint-disable-next-line no-await-in-loop
        await session.executeStatement('SELECT 1', opts);
      } catch (err) {
        thrown = err;
      }
      expect(thrown, `path ${JSON.stringify(opts)}`).to.be.instanceOf(HiveDriverError);
      expect((thrown as Error).message).to.match(/SUBMIT_BOOM/);
    }
  });

  // Genuinely unsupported on SEA — rejected (rather than silently ignored) so
  // a caller/agent gets signal instead of a no-op. queryTags / queryTimeout /
  // rowLimit are NOT here — they are forwarded (asserted above).
  for (const { name, options, re } of [
    { name: 'useCloudFetch', options: { useCloudFetch: true }, re: /useCloudFetch/ },
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
    for (const request of [
      { schemaName: 'def', tableName: 't' },
      { catalogName: '', schemaName: 'def', tableName: 't' },
    ]) {
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

describe('SeaOperationBackend — async (submitStatement) path', () => {
  const makeAsyncOp = (asyncStatement: FakeAsyncStatement) =>
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    new SeaOperationBackend({ asyncStatement: asyncStatement as any, context: makeContext() });

  it('rejects when neither asyncStatement nor statement is provided', () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(() => new SeaOperationBackend({ context: makeContext() } as any)).to.throw(HiveDriverError, /exactly one/);
  });

  it('rejects when BOTH asyncStatement and statement are provided', () => {
    expect(
      () =>
        new SeaOperationBackend({
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          asyncStatement: new FakeAsyncStatement() as any,
          statement: new FakeNativeStatement(),
          context: makeContext(),
        }),
    ).to.throw(HiveDriverError, /exactly one/);
  });

  it('id defaults to the async statement id', () => {
    const op = makeAsyncOp(new FakeAsyncStatement());
    expect(op.id).to.equal('01ef-fake-async-id');
  });

  it('status() reports the real kernel state', async () => {
    const running = makeAsyncOp(new FakeAsyncStatement('Running'));
    expect((await running.status(false)).state).to.equal(OperationState.Running);
    const ok = makeAsyncOp(new FakeAsyncStatement('Succeeded'));
    expect((await ok.status(false)).state).to.equal(OperationState.Succeeded);
  });

  it('surfaces rich-status fields (numModifiedRows etc.) through op.status() once Succeeded', async () => {
    // The kernel derives a DML's count off the terminal GetStatement poll and
    // exposes it on the AsyncStatement accessors; the backend surfaces it on
    // op.status() at the Succeeded state — parity with the sync path.
    const stmt = new FakeAsyncStatement('Succeeded');
    stmt.rich = {
      numModifiedRows: 7,
      displayMessage: 'UPDATE 0 7',
      diagnosticInfo: 'ok',
      errorDetailsJson: null,
    };
    const op = makeAsyncOp(stmt);
    const status = await op.status(false);
    expect(status.state).to.equal(OperationState.Succeeded);
    expect(status.numModifiedRows).to.equal(7);
    expect(status.displayMessage).to.equal('UPDATE 0 7');
    expect(status.diagnosticInfo).to.equal('ok');
    expect(status.errorDetailsJson).to.equal(null);
  });

  it('does not read rich-status fields before the async statement is terminal', async () => {
    // A Running async statement must not surface (or fabricate) a count.
    const stmt = new FakeAsyncStatement('Running');
    stmt.rich = { numModifiedRows: 99, displayMessage: null, diagnosticInfo: null, errorDetailsJson: null };
    const op = makeAsyncOp(stmt);
    const status = await op.status(false);
    expect(status.state).to.equal(OperationState.Running);
    expect(status.numModifiedRows).to.equal(undefined);
  });

  it('waitUntilReady() polls status() until terminal, firing the progress callback each tick', async () => {
    const stmt = new FakeAsyncStatement(['Pending', 'Running', 'Succeeded']);
    const op = makeAsyncOp(stmt);
    const states: OperationState[] = [];
    await op.waitUntilReady({ callback: (s) => states.push(s.state) });
    expect(stmt.statusCalls).to.equal(3);
    expect(states).to.deep.equal([OperationState.Pending, OperationState.Running, OperationState.Succeeded]);
  });

  it('waitUntilReady() surfaces the kernel error envelope on a Failed statement', async () => {
    const stmt = new FakeAsyncStatement('Failed');
    // The kernel rejects awaitResult() with a sentinel-framed structured error;
    // decodeNapiKernelError turns it into a typed HiveDriverError.
    stmt.awaitResultError = new Error(
      `__databricks_error__:${JSON.stringify({ code: 'SqlError', message: 'TABLE_OR_VIEW_NOT_FOUND' })}`,
    );
    const op = makeAsyncOp(stmt);
    let thrown: unknown;
    try {
      await op.waitUntilReady();
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
    expect((thrown as Error).message).to.match(/TABLE_OR_VIEW_NOT_FOUND/);
  });

  // A server-driven terminal state MUST throw OperationStateError (not a plain
  // HiveDriverError) so the DBSQLOperation facade — which only mirrors its
  // cancelled/closed flags when `err instanceof OperationStateError` — stays in
  // sync. Asserting the subclass + errorCode is what catches a regression to
  // the bare HiveDriverError (which would pass an `instanceOf HiveDriverError`
  // check since OperationStateError extends it).
  it('waitUntilReady() throws OperationStateError(Canceled) on a server-side Cancelled statement', async () => {
    const op = makeAsyncOp(new FakeAsyncStatement('Cancelled'));
    let thrown: unknown;
    try {
      await op.waitUntilReady();
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(OperationStateError);
    expect((thrown as OperationStateError).errorCode).to.equal(OperationStateErrorCode.Canceled);
  });

  it('best-effort close()s the kernel statement on a server-driven terminal error (no leak)', async () => {
    // P1.5: the poll loop must release the statement handle on terminal errors,
    // not just throw (otherwise the kernel-side statement leaks until session close).
    for (const state of ['Cancelled', 'Closed', 'Unknown']) {
      const stmt = new FakeAsyncStatement(state);
      const op = makeAsyncOp(stmt);
      // eslint-disable-next-line no-await-in-loop
      await op.waitUntilReady().catch(() => undefined);
      expect(stmt.closed, `closed after ${state}`).to.equal(true);
    }
  });

  it('waitUntilReady() throws OperationStateError(Closed) on a server-side Closed statement', async () => {
    const op = makeAsyncOp(new FakeAsyncStatement('Closed'));
    let thrown: unknown;
    try {
      await op.waitUntilReady();
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(OperationStateError);
    expect((thrown as OperationStateError).errorCode).to.equal(OperationStateErrorCode.Closed);
  });

  it('cancel() forwards to the async statement and short-circuits a subsequent poll', async () => {
    const stmt = new FakeAsyncStatement(['Running', 'Running', 'Succeeded']);
    const op = makeAsyncOp(stmt);
    await op.cancel();
    expect(stmt.cancelled).to.equal(true);
    // A JS-side cancel makes waitUntilReady fail fast without further polling.
    let thrown: unknown;
    try {
      await op.waitUntilReady();
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.an('error');
  });

  it('close() forwards to the async statement', async () => {
    const stmt = new FakeAsyncStatement();
    const op = makeAsyncOp(stmt);
    await op.close();
    expect(stmt.closed).to.equal(true);
  });
});

describe('SeaOperationBackend — sync (executeStatementCancellable) path', () => {
  const makeSyncOp = (cancellableExecution: FakeCancellableExecution) =>
    new SeaOperationBackend({
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      cancellableExecution: cancellableExecution as any,
      context: makeContext(),
    });

  it('rejects when more than one handle kind is provided', () => {
    expect(
      () =>
        new SeaOperationBackend({
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          cancellableExecution: new FakeCancellableExecution() as any,
          statement: new FakeNativeStatement(),
          context: makeContext(),
        }),
    ).to.throw(HiveDriverError, /exactly one/);
  });

  it('keeps op.id stable across the sync execute and logs the resolved server statement id', async () => {
    // op.id MUST stay stable (the facade keys telemetry start/complete on it —
    // a mid-flight flip to the server id would split the records and drop the
    // summary). The server statement_id is surfaced via a debug log instead.
    const logs: Array<{ level: LogLevel; message: string }> = [];
    const logger: IDBSQLLogger = { log: (level, message) => logs.push({ level, message }) };
    const op = new SeaOperationBackend({
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      cancellableExecution: new FakeCancellableExecution() as any,
      context: makeContext(logger),
    });
    const idBefore = op.id;
    expect(idBefore).to.be.a('string').and.have.length.greaterThan(0);
    await op.waitUntilReady();
    // Stable: driving result() to terminal does NOT mutate the id.
    expect(op.id).to.equal(idBefore);
    // But the now-known server statement_id is logged for correlation.
    expect(logs.some((l) => l.message.includes('01ef-fake-statement-id'))).to.equal(true);
  });

  it('surfaces the kernel SQL-error envelope when a sync result() rejects (Failed)', async () => {
    const exec = new FakeCancellableExecution();
    // The kernel rejects result() with a sentinel-framed structured error;
    // decodeNapiKernelError turns it into a typed HiveDriverError (sync path).
    exec.resultError = new Error(
      `__databricks_error__:${JSON.stringify({ code: 'SqlError', message: 'TABLE_OR_VIEW_NOT_FOUND' })}`,
    );
    const op = makeSyncOp(exec);
    let thrown: unknown;
    try {
      await op.waitUntilReady();
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(HiveDriverError);
    expect((thrown as Error).message).to.match(/TABLE_OR_VIEW_NOT_FOUND/);
  });

  it('status() reports Running before result() and Succeeded after', async () => {
    const exec = new FakeCancellableExecution();
    const op = makeSyncOp(exec);
    // Before waitUntilReady drives result(), the blocking execute is still in
    // flight from the JS side's perspective.
    expect((await op.status(false)).state).to.equal(OperationState.Running);
    await op.waitUntilReady();
    expect((await op.status(false)).state).to.equal(OperationState.Succeeded);
  });

  it('waitUntilReady() drives result() to the terminal statement and fires the callback once', async () => {
    const exec = new FakeCancellableExecution();
    const op = makeSyncOp(exec);
    const states: OperationState[] = [];
    await op.waitUntilReady({ callback: (s) => states.push(s.state) });
    expect(exec.resultCalls).to.equal(1);
    expect(states).to.deep.equal([OperationState.Succeeded]);
  });

  it('cancel() forwards to the cancellable execution (mid-compute)', async () => {
    const exec = new FakeCancellableExecution();
    const op = makeSyncOp(exec);
    await op.cancel();
    expect(exec.cancelled).to.equal(true);
  });

  it('close() on a still-running sync op cancels the server execution (no compute leak)', async () => {
    const exec = new FakeCancellableExecution();
    const op = makeSyncOp(exec);
    // close() before result() resolved: with no terminal statement to close,
    // it must proactively cancel the running execution rather than no-op
    // (otherwise server compute runs on until the kernel drop-guard fires at GC).
    await op.close();
    expect(exec.cancelled).to.equal(true);
  });

  it('cancel() interrupts an in-flight result(), surfacing OperationStateError(Canceled)', async () => {
    const exec = new FakeCancellableExecution();
    exec.block = true; // result() stays pending until cancel() rejects it
    const op = makeSyncOp(exec);
    // Start the wait (drives result(), which parks), then cancel mid-compute.
    const waitPromise = op.waitUntilReady();
    // Let the microtask queue run so result() is dispatched and parked.
    await Promise.resolve();
    await op.cancel();
    let thrown: unknown;
    try {
      await waitPromise;
    } catch (err) {
      thrown = err;
    }
    expect(exec.cancelled).to.equal(true);
    expect(thrown).to.be.instanceOf(OperationStateError);
    expect((thrown as OperationStateError).errorCode).to.equal(OperationStateErrorCode.Canceled);
  });

  it('close() routes to the resolved statement once result() has produced it', async () => {
    const exec = new FakeCancellableExecution();
    const op = makeSyncOp(exec);
    await op.waitUntilReady(); // resolves the terminal statement
    await op.close();
    expect(exec.resultHandle.closed).to.equal(true);
  });

  it('close() before result() resolves is a no-op (nothing server-side to close yet)', async () => {
    const exec = new FakeCancellableExecution();
    const op = makeSyncOp(exec);
    // Should not throw even though result() never ran.
    const status = await op.close();
    expect(status.isSuccess).to.equal(true);
    expect(exec.resultHandle.closed).to.equal(false);
  });

  it('surfaces the kernel rich-status fields (numModifiedRows etc.) through op.status() and memoizes the read', async () => {
    // A DML statement's terminal kernel `Statement` carries numModifiedRows /
    // displayMessage / diagnosticInfo / errorDetailsJson. Drive the sync execute
    // to terminal and assert each non-null field propagates through op.status()
    // (previously the fakes returned all-null, so this propagation was untested).
    const resultHandle = new FakeNativeStatement();
    resultHandle.rich = {
      numModifiedRows: 42,
      displayMessage: 'INSERT 0 42',
      diagnosticInfo: 'stage 1/1 finished',
      errorDetailsJson: '{"detail":"none"}',
    };
    const exec = new FakeCancellableExecution(resultHandle);
    const op = makeSyncOp(exec);
    await op.waitUntilReady();

    const status = await op.status(false);
    expect(status.state).to.equal(OperationState.Succeeded);
    expect(status.numModifiedRows).to.equal(42);
    expect(status.displayMessage).to.equal('INSERT 0 42');
    expect(status.diagnosticInfo).to.equal('stage 1/1 finished');
    expect(status.errorDetailsJson).to.equal('{"detail":"none"}');

    // C5: re-status()-ing a completed op reuses the memoized read — the four FFI
    // accessors fire exactly once across both status() calls (4 reads, not 8).
    await op.status(false);
    expect(resultHandle.richReads).to.equal(4);
  });

  it('reports all-null rich-status for a SELECT (no rows modified) — the default', async () => {
    // A read-only statement carries no numModifiedRows; the backend surfaces
    // null rather than fabricating a value.
    const op = makeSyncOp(new FakeCancellableExecution());
    await op.waitUntilReady();
    const status = await op.status(false);
    expect(status.numModifiedRows).to.equal(null);
    expect(status.displayMessage).to.equal(null);
    expect(status.diagnosticInfo).to.equal(null);
    expect(status.errorDetailsJson).to.equal(null);
  });
});
