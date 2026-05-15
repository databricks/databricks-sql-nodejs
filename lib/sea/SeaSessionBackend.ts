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
import { mapKernelErrorToJsError, KernelErrorShape } from './SeaErrorMapping';
import SeaOperationBackend from './SeaOperationBackend';

const KERNEL_ERROR_SENTINEL = '__databricks_error__:';

function rethrowKernelError(err: unknown): never {
  if (err && typeof err === 'object' && 'message' in err) {
    const reason = (err as { reason?: unknown }).reason;
    if (typeof reason === 'string' && reason.startsWith(KERNEL_ERROR_SENTINEL)) {
      try {
        const payload = JSON.parse(reason.slice(KERNEL_ERROR_SENTINEL.length)) as KernelErrorShape;
        throw mapKernelErrorToJsError(payload);
      } catch (parseErr) {
        if (parseErr !== err) {
          throw parseErr;
        }
      }
    }
  }
  throw err;
}

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
 * **M0 scope:** `executeStatement` + `close`. Metadata methods
 * (`getCatalogs`, `getSchemas`, etc.) defer to M1 — they throw a clear
 * `HiveDriverError` so consumers using SEA against metadata APIs get an
 * actionable message instead of silently falling back. The Thrift
 * backend continues to handle the metadata path by default (callers
 * opt into SEA via `ConnectionOptions.useSEA`).
 *
 * **Session config flow:** the SEA wire protocol is statement-scoped,
 * so "session config" semantics (Spark conf, `initialCatalog`,
 * `initialSchema`) are emulated by forwarding the same defaults with
 * every `executeStatement` call. Per-statement overrides on
 * `ExecuteStatementOptions` are reserved for M1; M0 carries only the
 * defaults captured at session-open time plus the `useCloudFetch`
 * boolean projected onto `sessionConfig.use_cloud_fetch` for the
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
      rethrowKernelError(err);
    }
    return new SeaOperationBackend({
      statement: nativeStatement!,
      context: this.context,
    });
  }

  public async getTypeInfo(_request: TypeInfoRequest): Promise<IOperationBackend> {
    throw new HiveDriverError('SeaSessionBackend.getTypeInfo: not implemented yet (deferred to M1)');
  }

  public async getCatalogs(_request: CatalogsRequest): Promise<IOperationBackend> {
    throw new HiveDriverError('SeaSessionBackend.getCatalogs: not implemented yet (deferred to M1)');
  }

  public async getSchemas(_request: SchemasRequest): Promise<IOperationBackend> {
    throw new HiveDriverError('SeaSessionBackend.getSchemas: not implemented yet (deferred to M1)');
  }

  public async getTables(_request: TablesRequest): Promise<IOperationBackend> {
    throw new HiveDriverError('SeaSessionBackend.getTables: not implemented yet (deferred to M1)');
  }

  public async getTableTypes(_request: TableTypesRequest): Promise<IOperationBackend> {
    throw new HiveDriverError('SeaSessionBackend.getTableTypes: not implemented yet (deferred to M1)');
  }

  public async getColumns(_request: ColumnsRequest): Promise<IOperationBackend> {
    throw new HiveDriverError('SeaSessionBackend.getColumns: not implemented yet (deferred to M1)');
  }

  public async getFunctions(_request: FunctionsRequest): Promise<IOperationBackend> {
    throw new HiveDriverError('SeaSessionBackend.getFunctions: not implemented yet (deferred to M1)');
  }

  public async getPrimaryKeys(_request: PrimaryKeysRequest): Promise<IOperationBackend> {
    throw new HiveDriverError('SeaSessionBackend.getPrimaryKeys: not implemented yet (deferred to M1)');
  }

  public async getCrossReference(_request: CrossReferenceRequest): Promise<IOperationBackend> {
    throw new HiveDriverError('SeaSessionBackend.getCrossReference: not implemented yet (deferred to M1)');
  }

  public async close(): Promise<Status> {
    if (this.closed) {
      return Status.success();
    }
    try {
      await this.connection.close();
    } catch (err) {
      rethrowKernelError(err);
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
