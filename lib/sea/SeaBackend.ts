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
import { getSeaNative } from './SeaNativeLoader';
import SeaSessionBackend, { SeaConnectionNative } from './SeaSessionBackend';

interface SeaBackendOptions {
  context: IClientContext;
  /**
   * Test seam: lets unit tests inject a fake `openSession` without
   * loading the native binding. Defaults to `getSeaNative().openSession`.
   */
  openSession?: (opts: { hostName: string; httpPath: string; token: string }) => Promise<SeaConnectionNative>;
}

/**
 * `IBackend` implementation that dispatches through the napi-bound
 * kernel. For M0 this covers `connect → openSession → executeStatement`
 * and the result-fetching pipeline; other session APIs throw via
 * `SeaSessionBackend`.
 *
 * Auth: M0 supports PAT only. `ConnectionOptions.token` (the
 * existing public field used by the thrift path's
 * `PlainHttpAuthentication`) is forwarded to the kernel.
 */
export default class SeaBackend implements IBackend {
  private readonly context: IClientContext;

  private readonly openSessionImpl: NonNullable<SeaBackendOptions['openSession']>;

  private connection?: SeaConnectionNative;

  private connectOptions?: ConnectionOptions;

  constructor(options: SeaBackendOptions) {
    this.context = options.context;
    this.openSessionImpl =
      options.openSession ??
      (async (opts) => {
        const native = getSeaNative();
        return native.openSession(opts) as Promise<SeaConnectionNative>;
      });
  }

  public async connect(options: ConnectionOptions): Promise<void> {
    // M0: only PAT is wired through the napi binding.
    if (options.authType !== undefined && options.authType !== 'access-token') {
      throw new HiveDriverError(
        `SEA backend (M0): authType '${options.authType}' is not yet supported; use 'access-token' (PAT).`,
      );
    }
    if (!options.host) {
      throw new HiveDriverError('SEA backend: host is required.');
    }
    if (!options.path) {
      throw new HiveDriverError('SEA backend: path is required.');
    }
    // `token` lives on the access-token branch of the discriminated
    // `AuthOptions` union; TS narrows it once we exclude all non-default
    // authTypes above.
    const token = (options as { token?: string }).token;
    if (!token) {
      throw new HiveDriverError('SEA backend: token is required (M0 supports PAT only).');
    }
    this.connectOptions = options;
    // Open the kernel `Session` eagerly so `openSession` is just a
    // wrapper that returns the bound `SeaSessionBackend`. The kernel's
    // `Session` is reusable across statements, so one `Connection`
    // serves the whole client lifetime.
    this.connection = await this.openSessionImpl({
      hostName: options.host,
      httpPath: options.path,
      token,
    });
  }

  public async openSession(request: OpenSessionRequest): Promise<ISessionBackend> {
    if (!this.connection) {
      throw new HiveDriverError('SEA backend: not connected.');
    }
    return new SeaSessionBackend({
      connection: this.connection,
      context: this.context,
      initialCatalog: request.initialCatalog,
      initialSchema: request.initialSchema,
      configuration: request.configuration,
    });
  }

  public async close(): Promise<void> {
    if (this.connection) {
      await this.connection.close();
      this.connection = undefined;
    }
    this.connectOptions = undefined;
  }
}
