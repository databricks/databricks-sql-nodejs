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
import { SeaConnection } from './SeaNativeLoader';
import { decodeNapiKernelError } from './SeaErrorMapping';
import SeaOperationBackend from './SeaOperationBackend';

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

  public async getInfo(_infoType: number): Promise<InfoValue> {
    throw new HiveDriverError('SeaSessionBackend.getInfo: not implemented yet (deferred to M1)');
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

    // M0 surfaces a clear error rather than silently dropping M1-only knobs.
    if (options.namedParameters !== undefined || options.ordinalParameters !== undefined) {
      throw new HiveDriverError('SEA executeStatement: query parameters are not supported in M0 (deferred to M1)');
    }
    if (options.queryTimeout !== undefined) {
      throw new HiveDriverError('SEA executeStatement: queryTimeout is not supported in M0 (deferred to M1)');
    }
    if (options.useCloudFetch !== undefined) {
      throw new HiveDriverError(
        'SEA executeStatement: useCloudFetch is controlled by the kernel result configuration and is not a per-statement option on SEA',
      );
    }

    let nativeStatement;
    try {
      nativeStatement = await this.connection.executeStatement(statement);
    } catch (err) {
      throw decodeNapiKernelError(err);
    }
    return new SeaOperationBackend({
      statement: nativeStatement!,
      context: this.context,
      id: nativeStatement!.statementId,
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
