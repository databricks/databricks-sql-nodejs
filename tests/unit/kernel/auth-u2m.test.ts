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

describe('KernelAuth + KernelBackend — OAuth U2M auth flow', () => {
  describe('buildKernelConnectionOptions', () => {
    it('accepts databricks-oauth with no clientSecret as the U2M happy path (hardcoded port 8030)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
      };

      const native = buildKernelConnectionOptions(opts);
      expectNativeConnectionOptions(native, {
        hostName: 'example.cloud.databricks.com',
        httpPath: '/sql/1.0/warehouses/abc',
        intervalsAsString: true,
        authMode: 'OAuthU2m',
        oauthRedirectPort: 8030,
        oauthScopes: ['sql', 'offline_access'],
      });
    });

    it('defaults U2M oauthScopes to Thrift parity (sql offline_access)', () => {
      const native = buildKernelConnectionOptions({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
      });
      expect(native.authMode).to.equal('OAuthU2m');
      // Matches the standalone Thrift driver's defaultOAuthScopes, NOT the
      // kernel's bare `all-apis offline_access` default.
      expect((native as { oauthScopes?: string[] }).oauthScopes).to.deep.equal(['sql', 'offline_access']);
    });

    it('honors a caller-supplied U2M oauthScopes override', () => {
      const native = buildKernelConnectionOptions({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthScopes: ['all-apis'],
      } as ConnectionOptions);
      expect((native as { oauthScopes?: string[] }).oauthScopes).to.deep.equal(['all-apis']);
    });

    it('falls back to the default U2M scopes when oauthScopes is an empty array', () => {
      const native = buildKernelConnectionOptions({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthScopes: [],
      } as ConnectionOptions);
      expect((native as { oauthScopes?: string[] }).oauthScopes).to.deep.equal(['sql', 'offline_access']);
    });

    it('routes oauthClientId + no secret to U2M with that id as a custom client (secret-based routing, Thrift parity)', () => {
      // Routing keys off the SECRET (matching Thrift): no usable secret ⇒ U2M,
      // regardless of the id. A non-blank `oauthClientId` is then honoured as a
      // custom U2M client (Thrift forwards `options.oauthClientId` to its U2M
      // flow too). (Old id-presence routing rejected this as M2M-missing-secret.)
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'custom-client',
      };

      const native = buildKernelConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthU2m');
      expect((native as { oauthClientId?: string }).oauthClientId).to.equal('custom-client');
    });

    it('prepends `/` to the path on the U2M branch too', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: 'sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
      };

      const native = buildKernelConnectionOptions(opts);
      expect(native.httpPath).to.equal('/sql/1.0/warehouses/abc');
    });

    it('rejects azureTenantId on the U2M path with the Entra-direct error', () => {
      const opts: ConnectionOptions = {
        host: 'adb-12345.0.azuredatabricks.net',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        azureTenantId: 'tenant-uuid',
      };

      expect(() => buildKernelConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /Azure-direct OAuth.*is not supported/,
      );
    });

    it('rejects useDatabricksOAuthInAzure on the U2M path', () => {
      const opts: ConnectionOptions = {
        host: 'adb-12345.0.azuredatabricks.net',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        useDatabricksOAuthInAzure: true,
      };

      expect(() => buildKernelConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /Azure-direct OAuth.*is not supported/,
      );
    });

    it('rejects a `persistence` hook on U2M citing the AuthConfig::External kernel-plumbing gap', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        persistence: {
          read: async () => undefined,
          persist: async () => undefined,
        },
      };

      expect(() => buildKernelConnectionOptions(opts)).to.throw(HiveDriverError, /AuthConfig::External.*plumbing/);
    });
  });

  describe('KernelBackend.connect + openSession (U2M)', () => {
    it('round-trips U2M options through to the napi binding', async () => {
      const { binding, calls } = makeFakeBinding();
      const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });

      await backend.connect({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
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
        authMode: 'OAuthU2m',
        oauthRedirectPort: 8030,
        oauthScopes: ['sql', 'offline_access'],
      });

      await session.close();
      await backend.close();
    });
  });
});
