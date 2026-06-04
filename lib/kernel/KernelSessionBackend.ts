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
import { KernelConnection, KernelNativeExecuteOptions, KernelStatement } from './KernelNativeLoader';
import { decodeNapiKernelError } from './KernelErrorMapping';
import { numberToInt64 } from '../thrift-backend/ThriftSessionBackend';
import KernelOperationBackend from './KernelOperationBackend';
import { buildKernelPositionalParams, buildKernelNamedParams } from './KernelPositionalParams';
import { kernelServerInfoValue } from './KernelServerInfo';
import { serializeQueryTags } from '../utils';

export interface KernelSessionBackendOptions {
  /** The opaque napi `Connection` handle returned by `openSession`. */
  connection: KernelConnection;
  context: IClientContext;
  /** Optional override for `id`. Defaults to a fresh UUIDv4. */
  id?: string;
}

/**
 * SEA-backed implementation of `ISessionBackend`.
 *
 * **Scope:** `executeStatement` (sync + async), `close`, `getInfo`, and the
 * full metadata surface (`getCatalogs`, `getSchemas`, `getTables`,
 * `getColumns`, `getFunctions`, `getTableTypes`, `getTypeInfo`,
 * `getPrimaryKeys`, `getCrossReference`) — each forwards to the kernel's napi
 * metadata calls (see `runMetadata`). The Thrift backend remains the default;
 * callers opt into the kernel path via `ConnectionOptions.useKernel`.
 *
 * **Session config flow:** catalog / schema / sessionConf are applied
 * once at session creation (kernel `Session::builder().defaults()` +
 * `.session_conf()` → SEA `CreateSession.catalog` / `.schema` /
 * `.session_confs`) and remain in effect for every statement run on
 * the resulting napi `Connection`. No per-statement forwarding is
 * needed — that pattern was removed when the napi binding moved these
 * onto `openSession` to match pyo3.
 */
export default class KernelSessionBackend implements ISessionBackend {
  private readonly connection: KernelConnection;

  private readonly context: IClientContext;

  private readonly _id: string;

  private closed = false;

  constructor({ connection, context, id }: KernelSessionBackendOptions) {
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
   * {@link kernelServerInfoValue}.
   */
  public async getInfo(infoType: number): Promise<InfoValue> {
    this.failIfClosed();
    const value = kernelServerInfoValue(infoType);
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
   *   - `queryTimeout` → enforced client-side by the operation backend's poll
   *     deadline (the kernel ignores `queryTimeoutSecs` on the async submit
   *     path), NOT forwarded to the napi options;
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

    // `runAsync` selects the kernel execution path. NOTE: this is a SEA/kernel-
    // specific use of the option — the Thrift backend hardcodes `runAsync: true`
    // on the wire and never reads `options.runAsync`, so the field is a no-op
    // there. The only observable difference between the two SEA paths is WHEN
    // `executeStatement` resolves; the public API, result shape, schema, and
    // error classes are identical on both (and to Thrift). See the option's
    // JSDoc in `IDBSQLSession` for the cross-backend contract.
    //
    //   - DEFAULT (`runAsync` false/undefined) — SYNC. Route through
    //     `executeStatementCancellable`: the kernel blocks on `execute()`
    //     (server-side direct-results / poll-to-terminal), which is faster and,
    //     with the napi sync canceller, fully cancellable mid-COMPUTE. The
    //     blocking drive runs in the operation backend's `result()` (inside
    //     `waitUntilReady`, which the facade invokes lazily at first fetch).
    //     `queryTimeoutSecs` IS honoured on this path (forwarded to the napi
    //     options below) since the kernel `execute()` consults it.
    //
    //   - `runAsync: true` — ASYNC. Submit (`wait_timeout=0s`): the server
    //     returns a pending `AsyncStatement` immediately while the query runs;
    //     the backend polls `status()` to terminal in `waitUntilReady()` and
    //     materialises results via `awaitResult()`. `queryTimeoutSecs` is
    //     ignored by the kernel on submit, so it is enforced client-side by the
    //     operation backend's poll-loop deadline instead.
    const runAsync = options.runAsync ?? false;
    const queryTimeoutSecs =
      options.queryTimeout !== undefined ? numberToInt64(options.queryTimeout).toNumber() : undefined;

    if (!runAsync) {
      // Sync path: forward `queryTimeoutSecs` to the napi options — the kernel
      // `execute()` honours it (server statement timeout).
      const execOptions = this.buildExecuteOptions(options, queryTimeoutSecs);
      let cancellableExecution;
      try {
        cancellableExecution =
          execOptions === undefined
            ? await this.connection.executeStatementCancellable(statement)
            : await this.connection.executeStatementCancellable(statement, execOptions);
      } catch (err) {
        throw this.logAndMapError('executeStatement', err);
      }
      return new KernelOperationBackend({
        cancellableExecution: cancellableExecution!,
        context: this.context,
        // The kernel honours `queryTimeoutSecs` on the sync `execute` path, so
        // it is forwarded via the napi options (see `buildExecuteOptions`); the
        // backend also keeps it as a deadline guard for parity with async.
        queryTimeoutSecs,
      });
    }

    // Async path: do NOT forward `queryTimeoutSecs` (the kernel ignores it on
    // submit — `wait_timeout=0s`); it is enforced client-side by the poll loop.
    const execOptions = this.buildExecuteOptions(options);
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
    return new KernelOperationBackend({
      asyncStatement: asyncStatement!,
      context: this.context,
      // `queryTimeout` is typed `number | bigint | Int64`; `numberToInt64(...).toNumber()`
      // coerces all three (a bare `Number(int64)` yields NaN — node-int64 has no valueOf).
      queryTimeoutSecs,
    });
  }

  /**
   * Translate the public `ExecuteStatementOptions` into the kernel napi
   * `ExecuteOptions`, returning `undefined` when nothing is set so the
   * no-options call shape (`executeStatement(sql)`) is preserved.
   */
  private buildExecuteOptions(
    options: ExecuteStatementOptions,
    queryTimeoutSecs?: number,
  ): KernelNativeExecuteOptions | undefined {
    // Positional (`?`) and named (`:name`) parameters are mutually exclusive —
    // the kernel binds one placeholder style per statement. Use the SAME error
    // type and message as the Thrift backend (`ThriftSessionBackend`) so a
    // caller catching `ParameterError` behaves identically across backends.
    const positionalParams = buildKernelPositionalParams(options.ordinalParameters);
    const namedParams = buildKernelNamedParams(options.namedParameters);
    if (positionalParams !== undefined && namedParams !== undefined) {
      throw new ParameterError('Driver does not support both ordinal and named parameters.');
    }

    const execOptions: KernelNativeExecuteOptions = {};
    if (positionalParams !== undefined) {
      execOptions.positionalParams = positionalParams;
    }
    if (namedParams !== undefined) {
      execOptions.namedParams = namedParams;
    }
    // `queryTimeoutSecs` is forwarded only on the SYNC path (the caller passes
    // it in): the kernel `execute()` consults it as the server statement
    // timeout. On the async submit path the caller omits it (the kernel ignores
    // it under `wait_timeout=0s`), so it is enforced client-side by the
    // operation backend's poll-loop deadline instead (see executeStatement).
    if (queryTimeoutSecs !== undefined) {
      execOptions.queryTimeoutSecs = queryTimeoutSecs;
    }
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
  private wrapStatement(nativeStatement: KernelStatement): IOperationBackend {
    return new KernelOperationBackend({
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
  private async runMetadata(call: () => Promise<KernelStatement>): Promise<IOperationBackend> {
    let nativeStatement: KernelStatement;
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
   * (`KernelOperationLifecycle` / `KernelOperationBackend`). Metadata and bound-param
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
      throw new HiveDriverError('KernelSessionBackend: session is closed');
    }
  }
}
