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
import SeaBackend from '../../../lib/sea/SeaBackend';
import { buildSeaConnectionOptions } from '../../../lib/sea/SeaAuth';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import { makeFakeBinding } from './_helpers/fakeBinding';

describe('SeaAuth + SeaBackend — OAuth U2M auth flow', () => {
  describe('buildSeaConnectionOptions', () => {
    it('accepts databricks-oauth with no clientSecret as the U2M happy path (hardcoded port 8030)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native).to.deep.equal({
        hostName: 'example.cloud.databricks.com',
        httpPath: '/sql/1.0/warehouses/abc',
        authMode: 'OAuthU2m',
        oauthRedirectPort: 8030,
      });
    });

    it('drops the supplied oauthClientId on the U2M path (kernel uses its own default)', () => {
      // The thrift parity story: thrift's getClientId() falls back to
      // `databricks-cli` when undefined. Here we tell the kernel to do
      // the same via `client_id: None`. If a user supplies a clientId
      // alongside no secret, we treat that as U2M and use kernel default
      // — explicitly NOT propagating the supplied id, because the kernel
      // surface for U2M client_id is None-or-Some-with-no-default-rewrite,
      // and exposing the override here is out-of-scope-for-this-task.
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'custom-client',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthU2m');
      // Custom clientId is intentionally not forwarded — see comment above.
      expect(native).to.not.have.property('oauthClientId');
    });

    it('prepends `/` to the path on the U2M branch too', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: 'sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.httpPath).to.equal('/sql/1.0/warehouses/abc');
    });

    it('rejects azureTenantId on the U2M path with the Entra-direct error', () => {
      const opts: ConnectionOptions = {
        host: 'adb-12345.0.azuredatabricks.net',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        azureTenantId: 'tenant-uuid',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
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

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
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

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /AuthConfig::External.*plumbing/,
      );
    });
  });

  describe('SeaBackend.connect + openSession (U2M)', () => {
    it('round-trips U2M options through to the napi binding', async () => {
      const { binding, calls } = makeFakeBinding();
      const backend = new SeaBackend(binding);

      await backend.connect({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
      });

      const session = await backend.openSession({});
      expect(session.id).to.match(/^sea-session-\d+$/);

      expect(calls).to.have.lengthOf(1);
      expect(calls[0].method).to.equal('openSession');
      expect(calls[0].args[0]).to.deep.equal({
        hostName: 'example.cloud.databricks.com',
        httpPath: '/sql/1.0/warehouses/abc',
        authMode: 'OAuthU2m',
        oauthRedirectPort: 8030,
      });

      await session.close();
      await backend.close();
    });
  });
});
