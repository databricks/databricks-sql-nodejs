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
import SeaOperationBackend, { SeaStatementNative } from './SeaOperationBackend';

/**
 * The minimal slice of the napi-binding `Connection` class that we
 * consume from JS. Other methods on the binding's `Connection` (added
 * in later rounds) can be appended here without affecting M0.
 */
export interface SeaConnectionNative {
  executeStatement(
    sql: string,
    options: {
      initialCatalog?: string;
      initialSchema?: string;
      sessionConfig?: Record<string, string>;
    },
  ): Promise<SeaStatementNative>;
  close(): Promise<void>;
}

interface SeaSessionBackendOptions {
  connection: SeaConnectionNative;
  context: IClientContext;
  initialCatalog?: string;
  initialSchema?: string;
  configuration?: Record<string, string>;
}

const M0_NOT_IMPLEMENTED =
  'SEA backend: metadata operations (getInfo, getCatalogs, getSchemas, …) are not implemented for M0';

/**
 * `ISessionBackend` over the napi-bound kernel `Session`. M0 only wires
 * `executeStatement`; the metadata calls are tracked by separate features
 * and throw a clear "not implemented" error until they land.
 *
 * The session has no first-class identifier at the napi surface today
 * (the kernel exposes `Session` only as an opaque handle), so we mint
 * a client-side UUID for logging/telemetry.
 */
export default class SeaSessionBackend implements ISessionBackend {
  public readonly id: string;

  private readonly connection: SeaConnectionNative;

  private readonly context: IClientContext;

  private readonly initialCatalog?: string;

  private readonly initialSchema?: string;

  private readonly configuration: Record<string, string>;

  constructor({ connection, context, initialCatalog, initialSchema, configuration }: SeaSessionBackendOptions) {
    this.connection = connection;
    this.context = context;
    this.initialCatalog = initialCatalog;
    this.initialSchema = initialSchema;
    this.configuration = configuration ?? {};
    this.id = uuidv4();
  }

  public async executeStatement(statement: string, options: ExecuteStatementOptions): Promise<IOperationBackend> {
    // Build sessionConfig overlay from session-level configuration and
    // per-statement options. The napi binding currently accepts only
    // string-valued configs; non-string options (queryTimeout, params)
    // are not yet mapped (M1 work).
    const sessionConfig: Record<string, string> = { ...this.configuration };
    if (options.useCloudFetch !== undefined) {
      sessionConfig.use_cloud_fetch = String(options.useCloudFetch);
    }
    const native = await this.connection.executeStatement(statement, {
      initialCatalog: this.initialCatalog,
      initialSchema: this.initialSchema,
      sessionConfig,
    });
    return new SeaOperationBackend({
      statement: native,
      context: this.context,
      operationId: uuidv4(),
    });
  }

  public async close(): Promise<Status> {
    await this.connection.close();
    return Status.success();
  }

  // Metadata operations are out of scope for M0-results. They are
  // tracked by sibling features (sea-operation / sea-execution). Throw
  // clearly so callers can opt out until those land.
  public async getInfo(_infoType: number): Promise<InfoValue> {
    throw new HiveDriverError(M0_NOT_IMPLEMENTED);
  }

  public async getTypeInfo(_request: TypeInfoRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(M0_NOT_IMPLEMENTED);
  }

  public async getCatalogs(_request: CatalogsRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(M0_NOT_IMPLEMENTED);
  }

  public async getSchemas(_request: SchemasRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(M0_NOT_IMPLEMENTED);
  }

  public async getTables(_request: TablesRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(M0_NOT_IMPLEMENTED);
  }

  public async getTableTypes(_request: TableTypesRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(M0_NOT_IMPLEMENTED);
  }

  public async getColumns(_request: ColumnsRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(M0_NOT_IMPLEMENTED);
  }

  public async getFunctions(_request: FunctionsRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(M0_NOT_IMPLEMENTED);
  }

  public async getPrimaryKeys(_request: PrimaryKeysRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(M0_NOT_IMPLEMENTED);
  }

  public async getCrossReference(_request: CrossReferenceRequest): Promise<IOperationBackend> {
    throw new HiveDriverError(M0_NOT_IMPLEMENTED);
  }
}
