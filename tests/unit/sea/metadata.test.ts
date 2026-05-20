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
import SeaSessionBackend from '../../../lib/sea/SeaSessionBackend';
import SeaOperationBackend from '../../../lib/sea/SeaOperationBackend';
import SeaTableTypeFilter from '../../../lib/sea/SeaTableTypeFilter';
import {
  SeaNativeConnection,
  SeaNativeStatement,
  SeaExecuteOptions,
} from '../../../lib/sea/SeaNativeLoader';
import IClientContext, { ClientConfig } from '../../../lib/contracts/IClientContext';
import IDBSQLLogger, { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

// ─── Fakes ───────────────────────────────────────────────────────────────────

class FakeNativeStatement implements SeaNativeStatement {
  public async fetchNextBatch() { return null; }
  public async schema() { return { ipcBytes: Buffer.alloc(0) }; }
  public async cancel() {}
  public async close() {}
}

interface RecordedMetadataCall {
  method: string;
  args: unknown[];
  returnStatement: FakeNativeStatement;
}

/**
 * Connection fake that records every metadata call and the args passed
 * so tests can assert on both the argument routing and the return-value
 * wrapping path.
 */
class FakeMetadataConnection implements SeaNativeConnection {
  public readonly calls: RecordedMetadataCall[] = [];

  public throwNextCall: Error | null = null;

  private record(method: string, args: unknown[]): FakeNativeStatement {
    if (this.throwNextCall) {
      const err = this.throwNextCall;
      this.throwNextCall = null;
      throw err;
    }
    const returnStatement = new FakeNativeStatement();
    this.calls.push({ method, args, returnStatement });
    return returnStatement;
  }

  public async executeStatement(_sql: string, _options: SeaExecuteOptions): Promise<SeaNativeStatement> {
    return this.record('executeStatement', [_sql, _options]);
  }

  public async listCatalogs(): Promise<SeaNativeStatement> {
    return this.record('listCatalogs', []);
  }

  public async listSchemas(
    catalog: string | undefined,
    schemaPattern: string | undefined,
  ): Promise<SeaNativeStatement> {
    return this.record('listSchemas', [catalog, schemaPattern]);
  }

  public async listTables(
    catalog: string | undefined,
    schemaPattern: string | undefined,
    tablePattern: string | undefined,
    tableTypes: string[] | undefined,
  ): Promise<SeaNativeStatement> {
    return this.record('listTables', [catalog, schemaPattern, tablePattern, tableTypes]);
  }

  public async listColumns(
    catalog: string | undefined,
    schemaPattern: string | undefined,
    tablePattern: string | undefined,
    columnPattern: string | undefined,
  ): Promise<SeaNativeStatement> {
    return this.record('listColumns', [catalog, schemaPattern, tablePattern, columnPattern]);
  }

  public async listFunctions(
    catalog: string | undefined,
    schemaPattern: string | undefined,
    functionPattern: string | undefined,
  ): Promise<SeaNativeStatement> {
    return this.record('listFunctions', [catalog, schemaPattern, functionPattern]);
  }

  public async listTableTypes(): Promise<SeaNativeStatement> {
    return this.record('listTableTypes', []);
  }

  public async listTypeInfo(): Promise<SeaNativeStatement> {
    return this.record('listTypeInfo', []);
  }

  public async getPrimaryKeys(
    catalog: string,
    schema: string,
    table: string,
  ): Promise<SeaNativeStatement> {
    return this.record('getPrimaryKeys', [catalog, schema, table]);
  }

  public async getCrossReference(
    parentCatalog: string | undefined | null,
    parentSchema: string | undefined | null,
    parentTable: string | undefined | null,
    foreignCatalog: string,
    foreignSchema: string,
    foreignTable: string,
  ): Promise<SeaNativeStatement> {
    return this.record('getCrossReference', [
      parentCatalog, parentSchema, parentTable,
      foreignCatalog, foreignSchema, foreignTable,
    ]);
  }

  public async close(): Promise<void> {}
}

function makeContext(): IClientContext {
  const logger: IDBSQLLogger = { log(_level: LogLevel, _message: string): void {} };
  const config = {} as ClientConfig;
  return {
    getConfig: () => config,
    getLogger: () => logger,
    getConnectionProvider: async () => { throw new Error('unused'); },
    getClient: async () => { throw new Error('unused'); },
    getDriver: async () => { throw new Error('unused'); },
  };
}

function makeSession(connection: SeaNativeConnection): SeaSessionBackend {
  return new SeaSessionBackend({ connection, context: makeContext() });
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe('SeaSessionBackend metadata methods', () => {
  // ── getCatalogs ──────────────────────────────────────────────────────────

  describe('getCatalogs', () => {
    it('calls listCatalogs() with no args and returns SeaOperationBackend', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      const op = await session.getCatalogs({});
      expect(op).to.be.instanceOf(SeaOperationBackend);
      expect(conn.calls).to.have.length(1);
      expect(conn.calls[0].method).to.equal('listCatalogs');
      expect(conn.calls[0].args).to.deep.equal([]);
    });

    it('rejects when session is closed', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.close();
      let thrown: unknown;
      try { await session.getCatalogs({}); } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
      expect((thrown as Error).message).to.match(/closed/);
    });

    it('wraps kernel error via decodeNapiKernelError', async () => {
      const conn = new FakeMetadataConnection();
      conn.throwNextCall = new Error('napi-err');
      const session = makeSession(conn);
      let thrown: unknown;
      try { await session.getCatalogs({}); } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(Error);
    });
  });

  // ── getSchemas ───────────────────────────────────────────────────────────

  describe('getSchemas', () => {
    it('routes catalogName and schemaName to listSchemas', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      const op = await session.getSchemas({ catalogName: 'main', schemaName: 'info%' });
      expect(op).to.be.instanceOf(SeaOperationBackend);
      expect(conn.calls[0].method).to.equal('listSchemas');
      expect(conn.calls[0].args).to.deep.equal(['main', 'info%']);
    });

    it('passes undefined when request fields are absent', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.getSchemas({});
      expect(conn.calls[0].args).to.deep.equal([undefined, undefined]);
    });

    it('rejects when session is closed', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.close();
      let thrown: unknown;
      try { await session.getSchemas({}); } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
    });
  });

  // ── getTables ────────────────────────────────────────────────────────────

  describe('getTables', () => {
    it('routes all four args to listTables', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.getTables({
        catalogName: 'cat',
        schemaName: 'sch%',
        tableName: 'tbl%',
        tableTypes: ['TABLE', 'VIEW'],
      });
      expect(conn.calls[0].method).to.equal('listTables');
      expect(conn.calls[0].args).to.deep.equal(['cat', 'sch%', 'tbl%', ['TABLE', 'VIEW']]);
    });

    it('passes undefined for absent fields', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.getTables({});
      expect(conn.calls[0].args).to.deep.equal([undefined, undefined, undefined, undefined]);
    });

    it('returns SeaOperationBackend when tableTypes is absent', async () => {
      const conn = new FakeMetadataConnection();
      const op = await makeSession(conn).getTables({});
      expect(op).to.be.instanceOf(SeaOperationBackend);
    });

    it('wraps in SeaTableTypeFilter when tableTypes is provided', async () => {
      const conn = new FakeMetadataConnection();
      const op = await makeSession(conn).getTables({ tableTypes: ['TABLE'] });
      expect(op).to.be.instanceOf(SeaTableTypeFilter);
    });

    it('wraps in SeaTableTypeFilter when tableTypes is empty array', async () => {
      const conn = new FakeMetadataConnection();
      const op = await makeSession(conn).getTables({ tableTypes: [] });
      expect(op).to.be.instanceOf(SeaTableTypeFilter);
    });

    it('rejects when session is closed', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.close();
      let thrown: unknown;
      try { await session.getTables({}); } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
    });
  });

  // ── getTableTypes ────────────────────────────────────────────────────────

  describe('getTableTypes', () => {
    it('calls listTableTypes() with no args', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      const op = await session.getTableTypes({});
      expect(op).to.be.instanceOf(SeaOperationBackend);
      expect(conn.calls[0].method).to.equal('listTableTypes');
      expect(conn.calls[0].args).to.deep.equal([]);
    });

    it('rejects when session is closed', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.close();
      let thrown: unknown;
      try { await session.getTableTypes({}); } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
    });
  });

  // ── getTypeInfo ──────────────────────────────────────────────────────────

  describe('getTypeInfo', () => {
    it('calls listTypeInfo() and returns SeaOperationBackend', async () => {
      const conn = new FakeMetadataConnection();
      const op = await makeSession(conn).getTypeInfo({});
      expect(op).to.be.instanceOf(SeaOperationBackend);
      expect(conn.calls[0].method).to.equal('listTypeInfo');
    });

    it('rejects when session is closed', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.close();
      let thrown: unknown;
      try { await session.getTypeInfo({}); } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
    });
  });

  // ── getColumns ───────────────────────────────────────────────────────────

  describe('getColumns', () => {
    it('routes all four args to listColumns', async () => {
      const conn = new FakeMetadataConnection();
      await makeSession(conn).getColumns({
        catalogName: 'c',
        schemaName: 's',
        tableName: 't',
        columnName: 'col%',
      });
      expect(conn.calls[0].method).to.equal('listColumns');
      expect(conn.calls[0].args).to.deep.equal(['c', 's', 't', 'col%']);
    });

    it('passes undefined for absent fields', async () => {
      const conn = new FakeMetadataConnection();
      await makeSession(conn).getColumns({});
      expect(conn.calls[0].args).to.deep.equal([undefined, undefined, undefined, undefined]);
    });

    it('rejects when session is closed', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.close();
      let thrown: unknown;
      try { await session.getColumns({}); } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
    });
  });

  // ── getFunctions ─────────────────────────────────────────────────────────

  describe('getFunctions', () => {
    it('routes catalogName, schemaName, functionName to listFunctions', async () => {
      const conn = new FakeMetadataConnection();
      await makeSession(conn).getFunctions({
        catalogName: 'c',
        schemaName: 's%',
        functionName: 'fn%',
      });
      expect(conn.calls[0].method).to.equal('listFunctions');
      expect(conn.calls[0].args).to.deep.equal(['c', 's%', 'fn%']);
    });

    it('passes undefined catalogName when absent', async () => {
      const conn = new FakeMetadataConnection();
      await makeSession(conn).getFunctions({ functionName: 'myfn' });
      expect(conn.calls[0].args).to.deep.equal([undefined, undefined, 'myfn']);
    });

    it('rejects when session is closed', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.close();
      let thrown: unknown;
      try { await session.getFunctions({ functionName: 'f' }); } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
    });
  });

  // ── getPrimaryKeys ───────────────────────────────────────────────────────

  describe('getPrimaryKeys', () => {
    it('routes catalogName, schemaName, tableName to getPrimaryKeys', async () => {
      const conn = new FakeMetadataConnection();
      await makeSession(conn).getPrimaryKeys({
        catalogName: 'cat',
        schemaName: 'myschema',
        tableName: 'orders',
      });
      expect(conn.calls[0].method).to.equal('getPrimaryKeys');
      expect(conn.calls[0].args).to.deep.equal(['cat', 'myschema', 'orders']);
    });

    it('throws HiveDriverError when catalogName is absent', async () => {
      const conn = new FakeMetadataConnection();
      let thrown: unknown;
      try { await makeSession(conn).getPrimaryKeys({ schemaName: 'sch', tableName: 'tbl' }); } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
      expect((thrown as Error).message).to.match(/catalogName is required/);
    });

    it('returns SeaOperationBackend when all args present', async () => {
      const conn = new FakeMetadataConnection();
      const op = await makeSession(conn).getPrimaryKeys({ catalogName: 'cat', schemaName: 's', tableName: 't' });
      expect(op).to.be.instanceOf(SeaOperationBackend);
    });

    it('rejects when session is closed', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.close();
      let thrown: unknown;
      try { await session.getPrimaryKeys({ catalogName: 'cat', schemaName: 's', tableName: 't' }); } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
    });

    it('wraps kernel error via decodeNapiKernelError', async () => {
      const conn = new FakeMetadataConnection();
      conn.throwNextCall = new Error('kernel-pk-error');
      let thrown: unknown;
      try { await makeSession(conn).getPrimaryKeys({ catalogName: 'cat', schemaName: 's', tableName: 't' }); } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(Error);
    });
  });

  // ── getCrossReference ────────────────────────────────────────────────────

  describe('getCrossReference', () => {
    it('routes all 6 fields to getCrossReference in the right order', async () => {
      const conn = new FakeMetadataConnection();
      await makeSession(conn).getCrossReference({
        parentCatalogName: 'pc',
        parentSchemaName: 'ps',
        parentTableName: 'pt',
        foreignCatalogName: 'fc',
        foreignSchemaName: 'fs',
        foreignTableName: 'ft',
      });
      expect(conn.calls[0].method).to.equal('getCrossReference');
      expect(conn.calls[0].args).to.deep.equal(['pc', 'ps', 'pt', 'fc', 'fs', 'ft']);
    });

    it('returns SeaOperationBackend', async () => {
      const conn = new FakeMetadataConnection();
      const op = await makeSession(conn).getCrossReference({
        parentCatalogName: 'pc',
        parentSchemaName: 'ps',
        parentTableName: 'pt',
        foreignCatalogName: 'fc',
        foreignSchemaName: 'fs',
        foreignTableName: 'ft',
      });
      expect(op).to.be.instanceOf(SeaOperationBackend);
    });

    it('throws HiveDriverError when foreignCatalogName is absent', async () => {
      const conn = new FakeMetadataConnection();
      let thrown: unknown;
      try {
        await makeSession(conn).getCrossReference({
          parentCatalogName: 'pc', parentSchemaName: 'ps', parentTableName: 'pt',
          foreignCatalogName: '', foreignSchemaName: 'fs', foreignTableName: 'ft',
        });
      } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
      expect((thrown as Error).message).to.match(/foreignCatalogName is required/);
    });

    it('throws HiveDriverError when foreignSchemaName is absent', async () => {
      const conn = new FakeMetadataConnection();
      let thrown: unknown;
      try {
        await makeSession(conn).getCrossReference({
          parentCatalogName: 'pc', parentSchemaName: 'ps', parentTableName: 'pt',
          foreignCatalogName: 'fc', foreignSchemaName: '', foreignTableName: 'ft',
        });
      } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
      expect((thrown as Error).message).to.match(/foreignSchemaName is required/);
    });

    it('throws HiveDriverError when foreignTableName is absent', async () => {
      const conn = new FakeMetadataConnection();
      let thrown: unknown;
      try {
        await makeSession(conn).getCrossReference({
          parentCatalogName: 'pc', parentSchemaName: 'ps', parentTableName: 'pt',
          foreignCatalogName: 'fc', foreignSchemaName: 'fs', foreignTableName: '',
        });
      } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
      expect((thrown as Error).message).to.match(/foreignTableName is required/);
    });

    it('rejects when session is closed', async () => {
      const conn = new FakeMetadataConnection();
      const session = makeSession(conn);
      await session.close();
      let thrown: unknown;
      try {
        await session.getCrossReference({
          parentCatalogName: 'pc',
          parentSchemaName: 'ps',
          parentTableName: 'pt',
          foreignCatalogName: 'fc',
          foreignSchemaName: 'fs',
          foreignTableName: 'ft',
        });
      } catch (e) { thrown = e; }
      expect(thrown).to.be.instanceOf(HiveDriverError);
    });
  });
});
