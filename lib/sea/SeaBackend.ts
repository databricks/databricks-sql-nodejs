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
import IClientContext from '../contracts/IClientContext';
import { ConnectionOptions, OpenSessionRequest } from '../contracts/IDBSQLClient';
import HiveDriverError from '../errors/HiveDriverError';
import {
  getSeaNative,
  SeaNativeBinding,
  SeaNativeConnection,
} from './SeaNativeLoader';
import { decodeNapiKernelError } from './SeaErrorMapping';
import { buildSeaConnectionOptions, SeaNativeConnectionOptions } from './SeaAuth';
import SeaSessionBackend from './SeaSessionBackend';

export interface SeaBackendOptions {
  /**
   * Optional in the type so unit tests that only exercise the auth-
   * routing surface (which doesn't touch context) can pass
   * `{ nativeBinding }`. The constructor downcasts undefined to
   * `IClientContext` because runtime callers from `DBSQLClient` always
   * supply one — see `lib/DBSQLClient.ts` SEA seam.
   */
  context?: IClientContext;
  /**
   * Optional injection seam for unit tests. When provided, replaces the
   * default `getSeaNative()` call so tests can swap in a mock napi
   * binding without loading the `.node` artifact.
   */
  nativeBinding?: SeaNativeBinding;
}

/**
 * SEA-backed implementation of `IBackend`.
 *
 * **M0 dispatch model:** the napi binding's `openSession()` already
 * builds a kernel `Session` from PAT + hostname + httpPath, so there is
 * no "connect" round-trip before `openSession` — `connect()` only
 * captures the `ConnectionOptions` and validates that PAT auth is in
 * use. The actual session open happens inside `openSession()`.
 *
 * **Auth validation:** delegates to `buildSeaConnectionOptions` from
 * `SeaAuth`, which mirrors the existing DBSQLClient validation pattern
 * (slash-prepended httpPath, AuthenticationError on missing token or
 * blank OAuth credentials, HiveDriverError on unsupported authType /
 * Azure-direct / ambiguous credential combinations). M2M and U2M
 * routing key off `oauthClientId` presence; see SeaAuth.ts.
 *
 * **Why we don't use IClientContext's connectionProvider here:** that
 * provider is the Thrift HTTP transport. The kernel owns its own
 * reqwest+rustls stack inside the native binding, so there is no
 * NodeJS-level connection state to manage on the SEA path. The
 * `IClientContext` is still useful for logger + config access.
 */
export default class SeaBackend implements IBackend {
  private readonly context: IClientContext;

  private readonly binding: SeaNativeBinding;

  private nativeOptions?: SeaNativeConnectionOptions;

  constructor(options?: SeaBackendOptions) {
    this.context = options?.context as IClientContext;
    this.binding = options?.nativeBinding ?? getSeaNative();
  }

  public async connect(options: ConnectionOptions): Promise<void> {
    // Validate PAT auth + capture the napi-binding option shape.
    // Any non-PAT mode (or a missing/empty token) throws here, before
    // we ever touch the native binding.
    this.nativeOptions = buildSeaConnectionOptions(options);
  }

  public async openSession(request: OpenSessionRequest): Promise<ISessionBackend> {
    if (!this.nativeOptions) {
      throw new HiveDriverError('SeaBackend: not connected. Call connect() first.');
    }

    // Fold session-level defaults from the OpenSessionRequest into the
    // napi `ConnectionOptions`. The kernel routes these through
    // `Session::builder().defaults(DefaultOpts)` + `.session_conf(...)`
    // so they land on the SEA `CreateSession` wire fields, not on each
    // per-statement request. Matches pyo3's `Session.__new__` shape.
    //
    // Only set the optional keys when present so the napi call shape
    // stays minimal — keeps wire snapshots / test assertions stable
    // for callers who pass no defaults.
    const sessionOptions: SeaNativeConnectionOptions = { ...this.nativeOptions };
    if (request.initialCatalog !== undefined) {
      sessionOptions.catalog = request.initialCatalog;
    }
    if (request.initialSchema !== undefined) {
      sessionOptions.schema = request.initialSchema;
    }
    if (request.configuration !== undefined) {
      sessionOptions.sessionConf = { ...request.configuration };
    }

    let nativeConnection: SeaNativeConnection;
    try {
      nativeConnection = (await this.binding.openSession(sessionOptions)) as SeaNativeConnection;
    } catch (err) {
      throw decodeNapiKernelError(err);
    }

    return new SeaSessionBackend({
      connection: nativeConnection!,
      context: this.context,
      id: nativeConnection!.sessionId,
    });
  }

  public async close(): Promise<void> {
    // No backend-level resources to release — each `SeaSessionBackend`
    // owns its own napi `Connection` lifecycle.
    this.nativeOptions = undefined;
  }
}
