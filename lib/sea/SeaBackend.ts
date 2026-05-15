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

import IBackend from '../contracts/IBackend';
import ISessionBackend from '../contracts/ISessionBackend';
import IOperationBackend from '../contracts/IOperationBackend';
import { ConnectionOptions, OpenSessionRequest } from '../contracts/IDBSQLClient';
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
import { getSeaNative, SeaNativeBinding } from './SeaNativeLoader';
import { buildSeaConnectionOptions, SeaNativeConnectionOptions } from './SeaAuth';

const NOT_IMPLEMENTED_SESSION =
  'SEA session backend: method not implemented in sea-auth (M0); lands in sea-execution/sea-operation.';

/**
 * Opaque handle to the napi binding's `Connection` class. The exact
 * shape lives in `native/sea/index.d.ts` (auto-generated). We type it as
 * a structural minimum here so the loader's pass-through typing doesn't
 * leak into every call site.
 */
interface NativeConnection {
  close(): Promise<void>;
}

/**
 * Minimal `ISessionBackend` that wraps the napi-binding's `Connection`.
 *
 * For M0 (sea-auth) only `id` and `close()` are functional — they're the
 * subset required to round-trip a connect-open-close cycle. Every other
 * method throws a clear "not implemented in M0" `HiveDriverError`.
 *
 * The `id` field is currently a synthetic counter-based string; the kernel
 * exposes a real session-id through a follow-on getter that
 * `sea-execution` will wire through.
 */
export class SeaSessionBackend implements ISessionBackend {
  private static seq = 0;

  public readonly id: string;

  private readonly connection: NativeConnection;

  constructor(connection: NativeConnection) {
    this.connection = connection;
    SeaSessionBackend.seq += 1;
    this.id = `sea-session-${SeaSessionBackend.seq}`;
  }

  /* eslint-disable @typescript-eslint/no-unused-vars */
  public async getInfo(_infoType: number): Promise<InfoValue> {
    throw new HiveDriverError(NOT_IMPLEMENTED_SESSION);
  }

  public async executeStatement(
    _statement: string,
    _options: ExecuteStatementOptions,
  ): Promise<IOperationBackend> {
    throw new HiveDriverError(NOT_IMPLEMENTED_SESSION);
  }

  public async getTypeInfo(_request: TypeInfoRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(NOT_IMPLEMENTED_SESSION);
  }

  public async getCatalogs(_request: CatalogsRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(NOT_IMPLEMENTED_SESSION);
  }

  public async getSchemas(_request: SchemasRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(NOT_IMPLEMENTED_SESSION);
  }

  public async getTables(_request: TablesRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(NOT_IMPLEMENTED_SESSION);
  }

  public async getTableTypes(_request: TableTypesRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(NOT_IMPLEMENTED_SESSION);
  }

  public async getColumns(_request: ColumnsRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(NOT_IMPLEMENTED_SESSION);
  }

  public async getFunctions(_request: FunctionsRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(NOT_IMPLEMENTED_SESSION);
  }

  public async getPrimaryKeys(_request: PrimaryKeysRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(NOT_IMPLEMENTED_SESSION);
  }

  public async getCrossReference(_request: CrossReferenceRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(NOT_IMPLEMENTED_SESSION);
  }
  /* eslint-enable @typescript-eslint/no-unused-vars */

  public async close(): Promise<Status> {
    await this.connection.close();
    return Status.success();
  }
}

/**
 * M0 SeaBackend — wires PAT auth + napi `openSession` end-to-end.
 *
 * Connect is a no-op at this layer (the napi binding has no notion of a
 * standalone "connect"; a session is opened directly). We capture the
 * validated PAT options and hand them to `openSession()` on demand.
 *
 * Subsequent milestones (`sea-execution`, `sea-operation`) replace the
 * stubbed `ISessionBackend` / `IOperationBackend` methods with real
 * napi-binding calls.
 */
export default class SeaBackend implements IBackend {
  private nativeOptions?: SeaNativeConnectionOptions;

  private readonly native: SeaNativeBinding;

  constructor(native: SeaNativeBinding = getSeaNative()) {
    this.native = native;
  }

  public async connect(options: ConnectionOptions): Promise<void> {
    // Validate PAT auth + capture the napi-binding option shape.
    // Any non-PAT mode (or a missing token) throws here, before we ever
    // touch the native binding.
    this.nativeOptions = buildSeaConnectionOptions(options);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async openSession(_request: OpenSessionRequest): Promise<ISessionBackend> {
    if (!this.nativeOptions) {
      throw new HiveDriverError('SeaBackend: connect() must be called before openSession().');
    }
    const connection = (await this.native.openSession(this.nativeOptions)) as NativeConnection;
    return new SeaSessionBackend(connection);
  }

  public async close(): Promise<void> {
    // Connection-level resources are owned by the session wrapper. No-op here.
    this.nativeOptions = undefined;
  }
}
