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

import { v4 as uuidv4 } from 'uuid';
import ISessionBackend from '../contracts/ISessionBackend';
import IOperationBackend from '../contracts/IOperationBackend';
import IClientContext from '../contracts/IClientContext';
import {
  ExecuteStatementOptions,
  TypeInfoRequest,
  CatalogsRequest,
  SchemasRequest,
  TablesRequest,
  TableTypesRequest,
  ColumnsRequest,
  FunctionsRequest,
  PrimaryKeysRequest,
  CrossReferenceRequest,
} from '../contracts/IDBSQLSession';
import Status from '../dto/Status';
import InfoValue from '../dto/InfoValue';
import HiveDriverError from '../errors/HiveDriverError';
import { SeaNativeConnection, SeaExecuteOptions } from './SeaNativeLoader';
import { decodeNapiKernelError } from './SeaErrorMapping';
import SeaOperationBackend from './SeaOperationBackend';
import SeaTableTypeFilter from './SeaTableTypeFilter';

/**
 * Per-session defaults that apply to every `executeStatement` issued
 * through this backend. Captured at `SeaBackend.openSession()` time from
 * the `OpenSessionRequest` — `initialCatalog` / `initialSchema` /
 * `sessionConfig`.
 *
 * The napi binding routes these to the kernel's `statement_conf` map,
 * which the SEA wire treats as session-scoped parameters. They are
 * forwarded with every `executeStatement` call so the JDBC-style
 * "session config" semantics are preserved even though SEA's wire
 * protocol is statement-scoped.
 */
export interface SeaSessionDefaults {
  initialCatalog?: string;
  initialSchema?: string;
  sessionConfig?: Record<string, string>;
}

export interface SeaSessionBackendOptions {
  /** The opaque napi `Connection` handle returned by `openSession`. */
  connection: SeaNativeConnection;
  context: IClientContext;
  defaults?: SeaSessionDefaults;
  /** Optional override for `id`. Defaults to a fresh UUIDv4. */
  id?: string;
}

/**
 * SEA-backed implementation of `ISessionBackend`.
 *
 * **M1 scope:** `executeStatement`, all 10 metadata methods, and
 * `close`. All metadata methods delegate directly to the corresponding
 * napi `Connection` method — the kernel performs SHOW/information_schema
 * queries and returns JDBC-shaped Arrow batches through a `Statement`
 * handle identical to `executeStatement`. The JS layer wraps each
 * returned `Statement` in a `SeaOperationBackend` so callers consume
 * metadata results through the same `IOperationBackend` interface they
 * use for SQL results.
 *
 * **Session config flow:** the SEA wire protocol is statement-scoped,
 * so "session config" semantics (Spark conf, `initialCatalog`,
 * `initialSchema`) are emulated by forwarding the same defaults with
 * every `executeStatement` call. Per-statement overrides on
 * `ExecuteStatementOptions` are reserved for M1; the `useCloudFetch`
 * boolean is projected onto `sessionConfig.use_cloud_fetch` for the
 * kernel.
 */
export default class SeaSessionBackend implements ISessionBackend {
  private readonly connection: SeaNativeConnection;

  private readonly context: IClientContext;

  private readonly defaults: SeaSessionDefaults;

  private readonly _id: string;

  private closed = false;

  constructor({ connection, context, defaults, id }: SeaSessionBackendOptions) {
    this.connection = connection;
    this.context = context;
    this.defaults = defaults ?? {};
    this._id = id ?? uuidv4();
  }

  public get id(): string {
    return this._id;
  }

  public async getInfo(_infoType: number): Promise<InfoValue> {
    throw new HiveDriverError('SeaSessionBackend.getInfo: not implemented yet (deferred to M1)');
  }

  /**
   * Execute a SQL statement through the napi binding. Merges the
   * session-level defaults (`initialCatalog` / `initialSchema` /
   * `sessionConfig`) with the per-call `useCloudFetch` override.
   *
   * M0 intentionally rejects `queryTimeout`, `namedParameters`, and
   * `ordinalParameters` with explicit deferred-to-M1 errors. The Thrift
   * backend remains the path for consumers that need any of those today.
   */
  public async executeStatement(statement: string, options: ExecuteStatementOptions): Promise<IOperationBackend> {
    this.failIfClosed();

    // M0 surfaces a clear error rather than silently dropping M1-only knobs.
    if (options.namedParameters !== undefined || options.ordinalParameters !== undefined) {
      throw new HiveDriverError(
        'SEA executeStatement: query parameters are not supported in M0 (deferred to M1)',
      );
    }
    if (options.queryTimeout !== undefined) {
      throw new HiveDriverError(
        'SEA executeStatement: queryTimeout is not supported in M0 (deferred to M1)',
      );
    }

    // Merge session-level sessionConfig with per-statement useCloudFetch.
    // The kernel accepts only string-valued conf values; booleans are
    // String()'d to "true"/"false" matching the existing Thrift conf
    // convention.
    const sessionConfig: Record<string, string> = { ...(this.defaults.sessionConfig ?? {}) };
    if (options.useCloudFetch !== undefined) {
      sessionConfig.use_cloud_fetch = String(options.useCloudFetch);
    }

    const executeOptions: SeaExecuteOptions = {
      initialCatalog: this.defaults.initialCatalog,
      initialSchema: this.defaults.initialSchema,
      sessionConfig: Object.keys(sessionConfig).length > 0 ? sessionConfig : undefined,
    };

    let nativeStatement;
    try {
      nativeStatement = await this.connection.executeStatement(statement, executeOptions);
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    return new SeaOperationBackend({
      statement: nativeStatement!,
      context: this.context,
    });
  }

  public async getTypeInfo(_request: TypeInfoRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    let nativeStatement;
    try {
      nativeStatement = await this.connection.listTypeInfo();
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    return new SeaOperationBackend({ statement: nativeStatement, context: this.context });
  }

  public async getCatalogs(_request: CatalogsRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    let nativeStatement;
    try {
      nativeStatement = await this.connection.listCatalogs();
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    return new SeaOperationBackend({ statement: nativeStatement, context: this.context });
  }

  public async getSchemas(request: SchemasRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    let nativeStatement;
    try {
      nativeStatement = await this.connection.listSchemas(
        request.catalogName,
        request.schemaName,
      );
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    return new SeaOperationBackend({ statement: nativeStatement, context: this.context });
  }

  public async getTables(request: TablesRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    let nativeStatement;
    try {
      nativeStatement = await this.connection.listTables(
        request.catalogName,
        request.schemaName,
        request.tableName,
        request.tableTypes,
      );
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    const backend = new SeaOperationBackend({ statement: nativeStatement, context: this.context });
    // The server does not honour tableTypes server-side (advisory only).
    // Apply client-side filter when the caller supplied a non-null list.
    if (request.tableTypes != null) {
      return new SeaTableTypeFilter(backend, new Set(request.tableTypes));
    }
    return backend;
  }

  public async getTableTypes(_request: TableTypesRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    let nativeStatement;
    try {
      nativeStatement = await this.connection.listTableTypes();
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    return new SeaOperationBackend({ statement: nativeStatement, context: this.context });
  }

  public async getColumns(request: ColumnsRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    let nativeStatement;
    try {
      nativeStatement = await this.connection.listColumns(
        request.catalogName,
        request.schemaName,
        request.tableName,
        request.columnName,
      );
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    return new SeaOperationBackend({ statement: nativeStatement, context: this.context });
  }

  public async getFunctions(request: FunctionsRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    let nativeStatement;
    try {
      nativeStatement = await this.connection.listFunctions(
        request.catalogName,
        request.schemaName,
        request.functionName,
      );
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    return new SeaOperationBackend({ statement: nativeStatement, context: this.context });
  }

  public async getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    if (!request.catalogName) {
      throw new HiveDriverError(
        'SeaSessionBackend.getPrimaryKeys: catalogName is required on the SEA path (kernel rejects empty identifiers; Thrift backend silently resolves to session default)',
      );
    }
    let nativeStatement;
    try {
      nativeStatement = await this.connection.getPrimaryKeys(
        request.catalogName,
        request.schemaName,
        request.tableName,
      );
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    return new SeaOperationBackend({ statement: nativeStatement, context: this.context });
  }

  public async getCrossReference(request: CrossReferenceRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    if (!request.foreignCatalogName) {
      throw new HiveDriverError(
        'SeaSessionBackend.getCrossReference: foreignCatalogName is required on the SEA path (kernel rejects empty identifiers)',
      );
    }
    if (!request.foreignSchemaName) {
      throw new HiveDriverError(
        'SeaSessionBackend.getCrossReference: foreignSchemaName is required on the SEA path',
      );
    }
    if (!request.foreignTableName) {
      throw new HiveDriverError(
        'SeaSessionBackend.getCrossReference: foreignTableName is required on the SEA path',
      );
    }
    let nativeStatement;
    try {
      nativeStatement = await this.connection.getCrossReference(
        request.parentCatalogName,
        request.parentSchemaName,
        request.parentTableName,
        request.foreignCatalogName,
        request.foreignSchemaName,
        request.foreignTableName,
      );
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    return new SeaOperationBackend({ statement: nativeStatement, context: this.context });
  }

  public async close(): Promise<Status> {
    if (this.closed) {
      return Status.success();
    }
    try {
      await this.connection.close();
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    this.closed = true;
    return Status.success();
  }

  private failIfClosed(): void {
    if (this.closed) {
      throw new HiveDriverError('SeaSessionBackend: session is closed');
    }
  }
}
