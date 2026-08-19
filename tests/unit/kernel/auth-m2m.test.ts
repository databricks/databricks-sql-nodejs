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

import { expect } from 'chai';
import expectNativeConnectionOptions from './_helpers/nativeOptions';
import KernelBackend from '../../../lib/kernel/KernelBackend';
import { buildKernelConnectionOptions } from '../../../lib/kernel/KernelAuth';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import { makeFakeBinding, makeFakeContext } from './_helpers/fakeBinding';

describe('KernelAuth + KernelBackend — OAuth M2M auth flow', () => {
  describe('buildKernelConnectionOptions', () => {
    it('accepts databricks-oauth + oauthClientId + oauthClientSecret', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
      };

      const native = buildKernelConnectionOptions(opts);
      expectNativeConnectionOptions(native, {
        hostName: 'example.cloud.databricks.com',
        httpPath: '/sql/1.0/warehouses/abc',
        intervalsAsString: true,
        authMode: 'OAuthM2m',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
        oauthScopes: ['all-apis'],
      });
    });

    it('defaults M2M oauthScopes to all-apis (Thrift + kernel parity)', () => {
      const native = buildKernelConnectionOptions({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
      });
      expect(native.authMode).to.equal('OAuthM2m');
      expect((native as { oauthScopes?: string[] }).oauthScopes).to.deep.equal(['all-apis']);
    });

    it('honors a caller-supplied M2M oauthScopes override (parity with pyo3)', () => {
      const native = buildKernelConnectionOptions({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
        oauthScopes: ['sql', 'offline_access'],
      } as ConnectionOptions);
      expect((native as { oauthScopes?: string[] }).oauthScopes).to.deep.equal(['sql', 'offline_access']);
    });

    it('prepends `/` to the path on the M2M branch too', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: 'sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
      };

      const native = buildKernelConnectionOptions(opts);
      expect(native.httpPath).to.equal('/sql/1.0/warehouses/abc');
    });

    it('defaults a missing oauthClientId to the default client on M2M (Thrift `?? default`)', () => {
      const opts = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientSecret: 'dose-fake-secret',
      } as unknown as ConnectionOptions;

      // Secret present ⇒ M2M; `oauthClientId ?? defaultClientId`, exactly like
      // Thrift's getClientId(). (No upfront "id required" throw.)
      const native = buildKernelConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthM2m');
      expect((native as { oauthClientId?: string }).oauthClientId).to.equal('databricks-sql-connector');
    });

    it('forwards an empty oauthClientId verbatim on M2M (Thrift `??` keeps "")', () => {
      const opts = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: '',
        oauthClientSecret: 'dose-fake-secret',
      } as unknown as ConnectionOptions;

      // `'' ?? default` === '' (nullish coalescing only guards null/undefined),
      // so Thrift forwards the empty id; we match it verbatim — no normalization.
      const native = buildKernelConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthM2m');
      expect((native as { oauthClientId?: string }).oauthClientId).to.equal('');
    });

    it('routes id + empty secret to M2M (strict Thrift parity: secret !== undefined ⇒ M2M)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: '',
      };

      // Routing is strict `oauthClientSecret === undefined ? U2M : M2M`, byte-for-
      // byte with Thrift: a present-but-empty secret counts as a secret ⇒ M2M.
      const native = buildKernelConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthM2m');
      expect((native as { oauthClientId?: string }).oauthClientId).to.equal('client-uuid');
    });

    it('rejects azureTenantId with a clear Entra-direct-out-of-scope error', () => {
      const opts: ConnectionOptions = {
        host: 'adb-12345.0.azuredatabricks.net',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
        azureTenantId: 'tenant-uuid',
      };

      expect(() => buildKernelConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /Azure-direct OAuth.*is not supported/,
      );
    });

    it('rejects useDatabricksOAuthInAzure with the same Entra-direct error', () => {
      const opts: ConnectionOptions = {
        host: 'adb-12345.0.azuredatabricks.net',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
        useDatabricksOAuthInAzure: true,
      };

      expect(() => buildKernelConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /Azure-direct OAuth.*is not supported/,
      );
    });

    it('rejects a `persistence` hook on M2M (no cache needed)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
        persistence: {
          read: async () => undefined,
          persist: async () => undefined,
        },
      };

      expect(() => buildKernelConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /`persistence` is not supported on OAuth M2M/,
      );
    });
  });

  describe('KernelBackend.connect + openSession (M2M)', () => {
    it('round-trips M2M options through to the napi binding', async () => {
      const { binding, calls } = makeFakeBinding();
      const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });

      await backend.connect({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
      });

      const session = await backend.openSession({});
      // Post-integration: KernelSessionBackend generates UUIDv4 ids; the
      // earlier auth-only counter-id scheme was superseded.
      expect(session.id).to.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);

      expect(calls).to.have.lengthOf(1);
      expect(calls[0].method).to.equal('openSession');
      expectNativeConnectionOptions(calls[0].args[0], {
        hostName: 'example.cloud.databricks.com',
        httpPath: '/sql/1.0/warehouses/abc',
        intervalsAsString: true,
        authMode: 'OAuthM2m',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
        oauthScopes: ['all-apis'],
      });

      await session.close();
      await backend.close();
    });

    it('connect() with an M2M secret but no oauthClientId proceeds with the default client (Thrift parity)', async () => {
      const { binding, calls } = makeFakeBinding();
      const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });

      // Strict Thrift parity: secret present + no id ⇒ M2M with the default
      // client (`oauthClientId ?? default`), not an upfront rejection.
      await backend.connect({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        oauthClientSecret: 'dose-fake-secret',
      } as any);
      await backend.openSession({});

      expect(calls).to.have.lengthOf(1);
      expect((calls[0].args[0] as { authMode?: string; oauthClientId?: string }).authMode).to.equal('OAuthM2m');
      expect((calls[0].args[0] as { oauthClientId?: string }).oauthClientId).to.equal('databricks-sql-connector');
    });
  });
});
