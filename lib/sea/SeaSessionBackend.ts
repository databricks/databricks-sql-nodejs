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
import ParameterError from '../errors/ParameterError';
import { LogLevel } from '../contracts/IDBSQLLogger';
import { SeaConnection, SeaNativeExecuteOptions, SeaStatement } from './SeaNativeLoader';
import { decodeNapiKernelError } from './SeaErrorMapping';
import SeaOperationBackend from './SeaOperationBackend';
import { buildSeaPositionalParams, buildSeaNamedParams } from './SeaPositionalParams';
import { seaServerInfoValue } from './SeaServerInfo';
import { serializeQueryTags } from '../utils';

export interface SeaSessionBackendOptions {
  /** The opaque napi `Connection` handle returned by `openSession`. */
  connection: SeaConnection;
  context: IClientContext;
  /** Optional override for `id`. Defaults to a fresh UUIDv4. */
  id?: string;
}

/**
 * SEA-backed implementation of `ISessionBackend`.
 *
 * **M0 scope:** `executeStatement` + `close`. Metadata methods
 * (`getCatalogs`, `getSchemas`, etc.) defer to M1 — they throw a clear
 * `HiveDriverError` so consumers using SEA against metadata APIs get an
 * actionable message instead of silently falling back. The Thrift
 * backend continues to handle the metadata path by default (callers
 * opt into SEA via `ConnectionOptions.useSEA`).
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
  private readonly connection: SeaConnection;

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

  /**
   * `getInfo` (JDBC `DatabaseMetaData` / ODBC `SQLGetInfo`) has no SEA/kernel
   * endpoint, so — exactly as JDBC does for `DatabaseMetaData` — we synthesize
   * the answer client-side for the three `TGetInfoType`s the Databricks server
   * answers (server name / DBMS name / DBMS version) and reject the rest.
   *
   * This is NOT a SEA-only contract narrowing: probing the live warehouse over
   * the Thrift path confirms the server itself returns an error for every
   * other `TGetInfoType` (CLI_MAX_DRIVER_CONNECTIONS, CLI_DATA_SOURCE_NAME, …),
   * and the three values it does answer are byte-identical to the constants we
   * synthesize (`"Spark SQL"` / `"Spark SQL"` / `"3.1.1"`, re-verified live).
   * So rejecting an unsupported type matches Thrift's effective behaviour — we
   * just surface a clearer, typed error than the server's opaque one. See
   * {@link seaServerInfoValue}.
   */
  public async getInfo(infoType: number): Promise<InfoValue> {
    this.failIfClosed();
    const value = seaServerInfoValue(infoType);
    if (value === undefined) {
      throw new HiveDriverError(
        `SEA getInfo: unsupported TGetInfoType ${infoType}. Only the info types the Databricks ` +
          `server itself answers are supported: CLI_SERVER_NAME (13), CLI_DBMS_NAME (17), ` +
          `CLI_DBMS_VER (18). The server rejects every other type on the Thrift path too, so this ` +
          `is not a SEA-specific restriction.`,
      );
    }
    return new InfoValue(value);
  }

  /**
   * Execute a SQL statement through the napi binding.
   *
   * Catalog / schema / sessionConf are session-level (applied at open).
   * Per-statement options forwarded to the kernel `ExecuteOptions`:
   *   - `ordinalParameters` / `namedParameters` → bound params (mutually
   *     exclusive — the kernel binds one placeholder style per statement);
   *   - `queryTimeout` → `queryTimeoutSecs` (SEA server wait timeout);
   *   - `rowLimit` → `rowLimit` (SEA-only server-side row cap);
   *   - `queryTags` → serialised into the conf overlay's reserved
   *     `query_tags` key (the same wire shape Thrift's `serializeQueryTags`
   *     produces), merged with any explicit `statementConf`.
   *
   * Still rejected (genuinely unsupported on SEA, rather than silently
   * dropped): `useCloudFetch` (governed by the kernel `ResultConfig`, not a
   * per-statement knob), `useLZ4Compression` (kernel owns result compression),
   * and `stagingAllowedLocalPath` (volume operations). `maxRows` is applied by
   * the facade at fetch time, so it is intentionally not handled here.
   */
  public async executeStatement(statement: string, options: ExecuteStatementOptions): Promise<IOperationBackend> {
    this.failIfClosed();

    if (options.useCloudFetch !== undefined) {
      throw new HiveDriverError(
        'SEA executeStatement: useCloudFetch is controlled by the kernel result configuration and is not a per-statement option on SEA',
      );
    }
    if (options.useLZ4Compression !== undefined) {
      throw new HiveDriverError(
        'SEA executeStatement: useLZ4Compression is not supported on SEA (result compression is governed by the kernel)',
      );
    }
    if (options.stagingAllowedLocalPath !== undefined) {
      throw new HiveDriverError(
        'SEA executeStatement: stagingAllowedLocalPath (volume operations) is not supported on SEA',
      );
    }

    const execOptions = this.buildExecuteOptions(options);

    // Submit asynchronously (kernel `wait_timeout=0s`): the server returns a
    // pending `AsyncStatement` immediately while the query runs, matching the
    // Thrift backend's always-async (`runAsync: true`) path. The operation
    // backend polls `status()` to terminal in `waitUntilReady()` and
    // materialises results via `awaitResult()`, so a long-running query stays
    // cancellable mid-flight and `status()` reports real Pending/Running states.
    let asyncStatement;
    try {
      asyncStatement =
        execOptions === undefined
          ? await this.connection.submitStatement(statement)
          : await this.connection.submitStatement(statement, execOptions);
    } catch (err) {
      throw this.logAndMapError('executeStatement', err);
    }
    // `queryTimeout` is enforced client-side by the operation backend's poll
    // loop: the kernel ignores `queryTimeoutSecs` on the async submit path
    // (`submitStatement` always sends `wait_timeout=0s`), so we do NOT forward
    // it to the napi options — passing it there would be a silent no-op.
    return new SeaOperationBackend({
      asyncStatement: asyncStatement!,
      context: this.context,
      queryTimeoutSecs: options.queryTimeout !== undefined ? Number(options.queryTimeout) : undefined,
    });
  }

  /**
   * Translate the public `ExecuteStatementOptions` into the kernel napi
   * `ExecuteOptions`, returning `undefined` when nothing is set so the
   * no-options call shape (`executeStatement(sql)`) is preserved.
   */
  private buildExecuteOptions(options: ExecuteStatementOptions): SeaNativeExecuteOptions | undefined {
    // Positional (`?`) and named (`:name`) parameters are mutually exclusive —
    // the kernel binds one placeholder style per statement. Use the SAME error
    // type and message as the Thrift backend (`ThriftSessionBackend`) so a
    // caller catching `ParameterError` behaves identically across backends.
    const positionalParams = buildSeaPositionalParams(options.ordinalParameters);
    const namedParams = buildSeaNamedParams(options.namedParameters);
    if (positionalParams !== undefined && namedParams !== undefined) {
      throw new ParameterError('Driver does not support both ordinal and named parameters.');
    }

    const execOptions: SeaNativeExecuteOptions = {};
    if (positionalParams !== undefined) {
      execOptions.positionalParams = positionalParams;
    }
    if (namedParams !== undefined) {
      execOptions.namedParams = namedParams;
    }
    // `queryTimeout` is intentionally NOT forwarded here — the kernel ignores
    // `queryTimeoutSecs` on `submitStatement`, so it is enforced client-side by
    // the operation backend's poll-loop deadline instead (see executeStatement).
    if (options.rowLimit !== undefined) {
      execOptions.rowLimit = Number(options.rowLimit);
    }
    // Per-statement conf overlay plus query tags. Tags are serialised JS-side
    // into the reserved `query_tags` key (the same wire shape the Thrift
    // backend produces via `serializeQueryTags` → `confOverlay`), rather than
    // via the napi `queryTags` field: napi's `HashMap<String,String>` can't
    // represent a null-valued tag, and the kernel rejects setting both the
    // `queryTags` field and a `query_tags` conf key.
    const serializedQueryTags = serializeQueryTags(options.queryTags);
    if (options.statementConf !== undefined || serializedQueryTags !== undefined) {
      const statementConf: Record<string, string> = { ...(options.statementConf ?? {}) };
      if (serializedQueryTags !== undefined) {
        statementConf.query_tags = serializedQueryTags;
      }
      if (Object.keys(statementConf).length > 0) {
        execOptions.statementConf = statementConf;
      }
    }

    return Object.keys(execOptions).length > 0 ? execOptions : undefined;
  }

  /** Wrap a napi metadata `Statement` (already terminal) as an operation backend. */
  private wrapStatement(nativeStatement: SeaStatement): IOperationBackend {
    return new SeaOperationBackend({
      statement: nativeStatement,
      context: this.context,
      id: nativeStatement.statementId,
    });
  }

  /**
   * Metadata calls forward to the kernel's metadata surface (`listCatalogs`,
   * `listTables`, …), each of which returns a napi `Statement` whose result
   * carries the JDBC-shaped columns. We wrap that handle exactly like an
   * executed statement. The kernel owns the SQL synthesis, the column
   * projection, and (for `listTables`) the client-side `TABLE_TYPE` filter —
   * the driver only maps the request fields to positional arguments.
   *
   * The `runAsync` / `maxRows` request fields are not threaded here: `runAsync`
   * is deprecated, and `maxRows` is applied by the facade at fetch time (same
   * as the Thrift path), so the napi call takes only the filter arguments.
   */
  public async getTypeInfo(_request: TypeInfoRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    return this.runMetadata(() => this.connection.listTypeInfo());
  }

  public async getCatalogs(_request: CatalogsRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    return this.runMetadata(() => this.connection.listCatalogs());
  }

  public async getSchemas(request: SchemasRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    return this.runMetadata(() => this.connection.listSchemas(request.catalogName, request.schemaName));
  }

  public async getTables(request: TablesRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    return this.runMetadata(() =>
      this.connection.listTables(request.catalogName, request.schemaName, request.tableName, request.tableTypes),
    );
  }

  public async getTableTypes(_request: TableTypesRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    return this.runMetadata(() => this.connection.listTableTypes());
  }

  public async getColumns(request: ColumnsRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    return this.runMetadata(() =>
      this.connection.listColumns(request.catalogName, request.schemaName, request.tableName, request.columnName),
    );
  }

  public async getFunctions(request: FunctionsRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    return this.runMetadata(() =>
      this.connection.listFunctions(request.catalogName, request.schemaName, request.functionName),
    );
  }

  public async getPrimaryKeys(request: PrimaryKeysRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    // The kernel requires a catalog for primary-key lookup (`Identifier::new`
    // rejects an empty string). The Thrift backend can forward an undefined
    // catalog and let the server resolve a default; the SEA/kernel path cannot,
    // so reject up front with a clear, actionable message rather than passing
    // `''` and surfacing the kernel's opaque "identifier must not be empty".
    if (request.catalogName === undefined || request.catalogName === '') {
      throw new HiveDriverError(
        'SEA getPrimaryKeys requires a catalog — pass `catalogName` explicitly. (The Thrift backend ' +
          'can omit it and let the server resolve a default; the SEA kernel path requires it.)',
      );
    }
    return this.runMetadata(() =>
      this.connection.getPrimaryKeys(request.catalogName as string, request.schemaName, request.tableName),
    );
  }

  public async getCrossReference(request: CrossReferenceRequest): Promise<IOperationBackend> {
    this.failIfClosed();
    return this.runMetadata(() =>
      this.connection.getCrossReference(
        request.parentCatalogName,
        request.parentSchemaName,
        request.parentTableName,
        request.foreignCatalogName,
        request.foreignSchemaName,
        request.foreignTableName,
      ),
    );
  }

  /** Run a napi metadata call, mapping kernel errors and wrapping the result handle. */
  private async runMetadata(call: () => Promise<SeaStatement>): Promise<IOperationBackend> {
    let nativeStatement: SeaStatement;
    try {
      nativeStatement = await call();
    } catch (err) {
      throw this.logAndMapError('metadata', err);
    }
    return this.wrapStatement(nativeStatement);
  }

  /**
   * Map a napi/kernel error to a typed driver error and emit a debug breadcrumb
   * first, matching the rest of the SEA backend's logging convention
   * (`SeaOperationLifecycle` / `SeaOperationBackend`). Metadata and bound-param
   * execute failures otherwise threw with no on-call signal.
   */
  private logAndMapError(op: string, err: unknown): Error {
    const mapped = decodeNapiKernelError(err);
    this.context.getLogger().log(LogLevel.debug, `SEA ${op} failed for session ${this._id}: ${mapped.message}`);
    return mapped;
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
