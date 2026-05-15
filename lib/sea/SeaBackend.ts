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
import { mapKernelErrorToJsError, KernelErrorShape } from './SeaErrorMapping';
import SeaSessionBackend from './SeaSessionBackend';

/**
 * Sentinel string the napi binding uses on `Error.reason` JSON envelopes.
 * Keep in sync with `native/sea/src/error.rs` (`SENTINEL`).
 */
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

export interface SeaBackendOptions {
  context: IClientContext;
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
 * **Why we don't use IClientContext's connectionProvider here:** that
 * provider is the Thrift HTTP transport. The kernel owns its own
 * reqwest+rustls stack inside the native binding, so there is no
 * NodeJS-level connection state to manage on the SEA path. The
 * `IClientContext` is still useful for logger + config access.
 */
export default class SeaBackend implements IBackend {
  private readonly context: IClientContext;

  private readonly binding: SeaNativeBinding;

  private connectionOptions?: ConnectionOptions;

  constructor(options?: SeaBackendOptions) {
    this.context = options?.context as IClientContext;
    this.binding = options?.nativeBinding ?? getSeaNative();
  }

  public async connect(options: ConnectionOptions): Promise<void> {
    // M0 supports PAT only. OAuth flows defer to the M1 auth feature.
    // We validate at connect-time so misconfigured callers fail fast
    // rather than at openSession-time.
    if (options.authType !== undefined && options.authType !== 'access-token') {
      throw new HiveDriverError(
        `SEA backend (M0) supports authType: 'access-token' only — received '${options.authType}'. ` +
          `OAuth flows defer to M1.`,
      );
    }
    // The `token` field lives on the `access-token` arm of the
    // ConnectionOptions discriminated union. Narrow via `authType` check.
    const token = (options as { token?: string }).token;
    if (!token) {
      throw new HiveDriverError('SEA backend: token is required for access-token auth');
    }
    this.connectionOptions = options;
  }

  public async openSession(request: OpenSessionRequest): Promise<ISessionBackend> {
    if (!this.connectionOptions) {
      throw new HiveDriverError('SeaBackend: not connected. Call connect() first.');
    }

    const { host, path } = this.connectionOptions;
    const token = (this.connectionOptions as { token?: string }).token as string;

    let nativeConnection: SeaNativeConnection;
    try {
      nativeConnection = await this.binding.openSession({
        hostName: host,
        httpPath: path,
        token,
      });
    } catch (err) {
      rethrowKernelError(err);
    }

    // Merge `request.configuration` (the existing public field for Spark
    // conf) with any backend-specific session config. The SEA wire
    // protocol applies these per-statement, but we capture them at
    // session-open time and forward with every executeStatement to
    // preserve session-config semantics.
    const sessionConfig = request.configuration ? { ...request.configuration } : undefined;

    return new SeaSessionBackend({
      connection: nativeConnection!,
      context: this.context,
      defaults: {
        initialCatalog: request.initialCatalog,
        initialSchema: request.initialSchema,
        sessionConfig,
      },
    });
  }

  public async close(): Promise<void> {
    // No backend-level resources to release — each `SeaSessionBackend`
    // owns its own napi `Connection` lifecycle.
    this.connectionOptions = undefined;
  }
}
