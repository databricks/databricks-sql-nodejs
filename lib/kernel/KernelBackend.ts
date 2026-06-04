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
import { InternalConnectionOptions } from '../contracts/InternalConnectionOptions';
import { LogLevel } from '../contracts/IDBSQLLogger';
import HiveDriverError from '../errors/HiveDriverError';
import { getKernelNative, KernelNativeBinding, KernelConnection } from './KernelNativeLoader';
import { decodeNapiKernelError } from './KernelErrorMapping';
import { buildKernelConnectionOptions, KernelNativeConnectionOptions } from './KernelAuth';
import KernelSessionBackend from './KernelSessionBackend';

export interface KernelBackendOptions {
  /**
   * Required. Provides the logger + config the SEA session/operation chain
   * logs through. `DBSQLClient` supplies it via the SEA seam
   * (`new KernelBackend({ context: this })`); unit tests pass a stub. Kept
   * mandatory (rather than an `as IClientContext` downcast of `undefined`)
   * so a missing context is a compile error, not a latent runtime NPE.
   */
  context: IClientContext;
  /**
   * Optional injection seam for unit tests. When provided, replaces the
   * default `getKernelNative()` call so tests can swap in a mock napi
   * binding without loading the `.node` artifact.
   */
  nativeBinding?: KernelNativeBinding;
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
 * **Auth validation:** delegates to `buildKernelConnectionOptions` from
 * `KernelAuth`, which mirrors the existing DBSQLClient validation pattern
 * (slash-prepended httpPath, AuthenticationError on missing token or
 * blank OAuth credentials, HiveDriverError on unsupported authType /
 * Azure-direct / ambiguous credential combinations). M2M and U2M
 * routing key off `oauthClientId` presence; see KernelAuth.ts.
 *
 * **Why we don't use IClientContext's connectionProvider here:** that
 * provider is the Thrift HTTP transport. The kernel owns its own
 * reqwest+rustls stack inside the native binding, so there is no
 * NodeJS-level connection state to manage on the SEA path. The
 * `IClientContext` is still useful for logger + config access.
 */
export default class KernelBackend implements IBackend {
  private readonly context: IClientContext;

  private readonly binding: KernelNativeBinding;

  private nativeOptions?: KernelNativeConnectionOptions;

  constructor(options: KernelBackendOptions) {
    this.context = options.context;
    this.binding = options.nativeBinding ?? getKernelNative();
  }

  public async connect(options: ConnectionOptions): Promise<void> {
    // Validate PAT auth + capture the napi-binding option shape.
    // Any non-PAT mode (or a missing/empty token) throws here, before
    // we ever touch the native binding.
    this.nativeOptions = buildKernelConnectionOptions(options);

    // Warn on the insecure combo: a `customCaCert` paired with
    // `checkServerCertificate: false` is almost always a mistake — verification
    // is fully off, so the custom trust anchor is never used. The combo is
    // still honoured (kernel contract), but a secure-looking `customCaCert`
    // shouldn't silently mask disabled verification.
    const tlsOpts = options as ConnectionOptions & InternalConnectionOptions;
    if (tlsOpts.checkServerCertificate === false && tlsOpts.customCaCert !== undefined) {
      this.context
        .getLogger()
        .log(
          LogLevel.warn,
          'SEA: `customCaCert` is set but `checkServerCertificate: false` disables certificate ' +
            'verification entirely — the custom CA is not used. Set `checkServerCertificate: true` to use it.',
        );
    }
  }

  public async openSession(request: OpenSessionRequest): Promise<ISessionBackend> {
    if (!this.nativeOptions) {
      throw new HiveDriverError('KernelBackend: not connected. Call connect() first.');
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
    const sessionOptions: KernelNativeConnectionOptions = { ...this.nativeOptions };
    if (request.initialCatalog !== undefined) {
      sessionOptions.catalog = request.initialCatalog;
    }
    if (request.initialSchema !== undefined) {
      sessionOptions.schema = request.initialSchema;
    }
    if (request.configuration !== undefined) {
      sessionOptions.sessionConf = { ...request.configuration };
    }

    let nativeConnection: KernelConnection;
    try {
      // `KernelNativeConnectionOptions.authMode` is a string-literal union
      // ('Pat' | 'OAuthM2m' | 'OAuthU2m') — deliberately not the binding's
      // `const enum AuthMode` (see KernelAuth's note on why a const-enum import
      // is avoided). The literal values are byte-identical to the enum's, so
      // the only divergence is TS's const-enum strictness; cast to the
      // binding's parameter type at this single boundary.
      nativeConnection = (await this.binding.openSession(
        sessionOptions as unknown as Parameters<KernelNativeBinding['openSession']>[0],
      )) as KernelConnection;
    } catch (err) {
      throw decodeNapiKernelError(err);
    }

    return new KernelSessionBackend({
      connection: nativeConnection!,
      context: this.context,
      id: nativeConnection!.sessionId,
    });
  }

  public async close(): Promise<void> {
    // No backend-level resources to release — each `KernelSessionBackend`
    // owns its own napi `Connection` lifecycle.
    this.nativeOptions = undefined;
  }
}
