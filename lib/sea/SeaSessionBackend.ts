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
import { SeaNativeConnection, SeaNativeExecuteOptions } from './SeaNativeLoader';
import { decodeNapiKernelError } from './SeaErrorMapping';
import SeaOperationBackend from './SeaOperationBackend';
import SeaTableTypeFilter from './SeaTableTypeFilter';
import { seaServerInfoValue } from './SeaServerInfo';
import { buildSeaPositionalParams, buildSeaNamedParams } from './SeaPositionalParams';
import ParameterError from '../errors/ParameterError';
import { emptyToUndefined, countParameterMarkers } from './SeaInputValidation';
import { serializeQueryTags } from '../utils';

export interface SeaSessionBackendOptions {
  /** The opaque napi `Connection` handle returned by `openSession`. */
  connection: SeaNativeConnection;
  context: IClientContext;
  /** Optional override for `id`. Defaults to a fresh UUIDv4. */
  id?: string;
}

/**
 * SEA-backed implementation of `ISessionBackend`.
 *
 * **M1 scope:** `executeStatement`, 9 of the 10 `IDBSQLSession`
 * metadata methods, and `close`. The implemented nine are:
 * `getTypeInfo`, `getCatalogs`, `getSchemas`, `getTables`,
 * `getTableTypes`, `getColumns`, `getFunctions`, `getPrimaryKeys`,
 * `getCrossReference`. `getInfo` is a stub-throw (deferred — the
 * kernel `Metadata` API does not expose it yet); `getProcedures` is
 * not on `IDBSQLSession` (see `SeaNativeLoader.ts` NOTE comment).
 *
 * All implemented metadata methods delegate directly to the corresponding
 * napi `Connection` method — the kernel performs SHOW/information_schema
 * queries and returns JDBC-shaped Arrow batches through a `Statement`
 * handle identical to `executeStatement`. The JS layer wraps each
 * returned `Statement` in a `SeaOperationBackend` so callers consume
 * metadata results through the same `IOperationBackend` interface they
 * use for SQL results.
 *
 * **Session config flow:** catalog / schema / sessionConf are applied
 * once at session creation (kernel `Session::builder().defaults()` +
 * `.session_conf()` → SEA `CreateSession.catalog` / `.schema` /
 * `.session_confs`) and remain in effect for every statement run on
 * the resulting napi `Connection`. No per-statement forwarding is
 * needed — that pattern was removed when the napi binding moved these
 * onto `openSession` to match pyo3.
 */
export default class SeaSessionBackend implements ISessionBackend {
  private readonly connection: SeaNativeConnection;

  private readonly context: IClientContext;

  private readonly _id: string;

  private closed = false;

  constructor({ connection, context, id }: SeaSessionBackendOptions) {
    this.connection = connection;
    this.context = context;
    this._id = id ?? uuidv4();
  }

  public get id(): string {
    return this._id;
  }

  public async getInfo(infoType: number): Promise<InfoValue> {
    this.failIfClosed();
    // `getInfo` (TGetInfoReq) is a Thrift/JDBC concept with no SEA-protocol or
    // kernel equivalent, so — like JDBC's DatabaseMetaData — we synthesize the
    // values client-side. `seaServerInfoValue` returns matches for the three
    // TGetInfoTypes the Thrift server answers (server name, DBMS name, DBMS
    // version) and `undefined` for the rest, which we surface as an error to
    // mirror the server's reject-unsupported-info-type behaviour.
    const value = seaServerInfoValue(infoType);
    if (value === undefined) {
      throw new HiveDriverError(
        `SEA getInfo: TGetInfoType ${infoType} is not supported. The SEA/kernel protocol ` +
          'has no getInfo RPC; only CLI_SERVER_NAME, CLI_DBMS_NAME and CLI_DBMS_VER are ' +
          'synthesised (matching the Thrift server, which also rejects all other info types).',
      );
    }
    return new InfoValue(value);
  }

  /**
   * Execute a SQL statement through the napi binding.
   *
   * Catalog / schema / sessionConf were applied at session open, so
   * there are no per-statement options to thread through.
   *
   * M0 intentionally rejects `queryTimeout`, `namedParameters`, and
   * `ordinalParameters` with explicit deferred-to-M1 errors. `useCloudFetch`
   * is a no-op on the SEA path — the kernel hardcodes the SEA
   * `disposition` to `INLINE_OR_EXTERNAL_LINKS`, and per-statement
   * conf overrides have no reader on the kernel; cloud-fetch behaviour
   * is governed entirely by the kernel's `ResultConfig` (M1 binding
   * surface).
   *
   * The Thrift backend remains the path for consumers that need any
   * of those today.
   */
  public async executeStatement(statement: string, options: ExecuteStatementOptions): Promise<IOperationBackend> {
    this.failIfClosed();

    if (options.useCloudFetch !== undefined) {
      throw new HiveDriverError(
        'SEA executeStatement: useCloudFetch is controlled by the kernel result configuration and is not a per-statement option on SEA',
      );
    }

    // Reduce `?` / `:name` bindings to the napi inputs the kernel param codec
    // accepts (DECIMAL → DECIMAL(p,s), NULL → value-less), reusing
    // DBSQLParameter's stringification. Positional and named are mutually
    // exclusive at the SQL level (matches the Thrift backend).
    const positionalParams = buildSeaPositionalParams(options.ordinalParameters);
    const namedParams = buildSeaNamedParams(options.namedParameters);
    if (positionalParams !== undefined && namedParams !== undefined) {
      throw new ParameterError('Driver does not support both ordinal and named parameters.');
    }
    // Arity check: positional params must match the `?` marker count, or the
    // server silently binds the prefix and drops the rest (data-correctness
    // footgun). Markers inside string literals / comments are not counted.
    if (positionalParams !== undefined) {
      const markerCount = countParameterMarkers(statement);
      if (positionalParams.length !== markerCount) {
        throw new ParameterError(
          `ordinalParameters length ${positionalParams.length} does not match the ` +
            `${markerCount} '?' placeholder(s) in the SQL`,
        );
      }
    }

    const nativeOptions: SeaNativeExecuteOptions = {};
    if (positionalParams !== undefined) {
      nativeOptions.positionalParams = positionalParams;
    }
    if (namedParams !== undefined) {
      nativeOptions.namedParams = namedParams;
    }
    // JDBC `setQueryTimeout` is whole seconds; the kernel's
    // `query_timeout_secs` (SEA wait timeout, on_wait_timeout = CANCEL) is
    // the native equivalent. The SEA wire caps it at 50s server-side.
    if (options.queryTimeout !== undefined) {
      nativeOptions.queryTimeoutSecs = Number(options.queryTimeout);
    }
    // Query tags: serialise JS-side into the conf overlay's `query_tags` key
    // (the same wire shape the Thrift backend produces via `serializeQueryTags`
    // → `confOverlay`). Not forwarded via the napi `queryTags` field: that's a
    // `HashMap<String,String>` which can't represent a null-valued tag, and the
    // kernel rejects setting both the field and a `query_tags` conf key. A
    // null-valued tag therefore round-trips as a key-only segment.
    const serializedQueryTags = serializeQueryTags(options.queryTags);
    if (serializedQueryTags !== undefined) {
      nativeOptions.statementConf = { query_tags: serializedQueryTags };
    }
    const hasOptions = Object.keys(nativeOptions).length > 0;

    // Submit asynchronously (kernel `wait_timeout=0s`): the server
    // returns a pending `AsyncStatement` handle immediately while the
    // query runs, exactly like the Thrift backend's always-async
    // (`runAsync: true`) path. `SeaOperationBackend` polls `status()`
    // to terminal in `waitUntilReady()` and materialises results via
    // `awaitResult()`, so a long-running query can be cancelled
    // mid-flight and `status()` reports real Pending/Running/Succeeded
    // states — parity the blocking `executeStatement()` path can't offer.
    let asyncStatement;
    try {
      asyncStatement = hasOptions
        ? await this.connection.submitStatement(statement, nativeOptions)
        : await this.connection.submitStatement(statement);
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    return new SeaOperationBackend({
      asyncStatement,
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
        emptyToUndefined(request.catalogName),
        emptyToUndefined(request.schemaName),
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
        emptyToUndefined(request.catalogName),
        emptyToUndefined(request.schemaName),
        emptyToUndefined(request.tableName),
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
        emptyToUndefined(request.catalogName),
        emptyToUndefined(request.schemaName),
        emptyToUndefined(request.tableName),
        emptyToUndefined(request.columnName),
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
        emptyToUndefined(request.catalogName),
        emptyToUndefined(request.schemaName),
        emptyToUndefined(request.functionName),
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
