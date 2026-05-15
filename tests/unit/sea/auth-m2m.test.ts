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
import AuthenticationError from '../../../lib/errors/AuthenticationError';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import { makeFakeBinding } from './_helpers/fakeBinding';

describe('SeaAuth + SeaBackend — OAuth M2M auth flow', () => {
  describe('buildSeaConnectionOptions', () => {
    it('accepts databricks-oauth + oauthClientId + oauthClientSecret', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native).to.deep.equal({
        hostName: 'example.cloud.databricks.com',
        httpPath: '/sql/1.0/warehouses/abc',
        authMode: 'OAuthM2m',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
      });
    });

    it('prepends `/` to the path on the M2M branch too', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: 'sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.httpPath).to.equal('/sql/1.0/warehouses/abc');
    });

    it('rejects missing oauthClientId with AuthenticationError', () => {
      const opts = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientSecret: 'dose-fake-secret',
      } as unknown as ConnectionOptions;

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /oauthClientId.*required/,
      );
    });

    it('rejects empty oauthClientId with AuthenticationError', () => {
      const opts = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: '',
        oauthClientSecret: 'dose-fake-secret',
      } as unknown as ConnectionOptions;

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /oauthClientId.*required/,
      );
    });

    it('routes empty oauthClientSecret to the U2M arm (round-3 NF-N3), where oauthClientId being set then rejects', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: '',
      };

      // Blank secret → U2M arm; oauthClientId set on U2M then raises
      // the dedicated "not supported on U2M" error.
      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /oauthClientId.*not supported on the OAuth U2M flow/,
      );
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

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
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

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
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

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /`persistence` is not supported on OAuth M2M/,
      );
    });
  });

  describe('SeaBackend.connect + openSession (M2M)', () => {
    it('round-trips M2M options through to the napi binding', async () => {
      const { binding, calls } = makeFakeBinding();
      const backend = new SeaBackend(binding);

      await backend.connect({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
      });

      const session = await backend.openSession({});
      expect(session.id).to.match(/^sea-session-\d+$/);

      expect(calls).to.have.lengthOf(1);
      expect(calls[0].method).to.equal('openSession');
      expect(calls[0].args[0]).to.deep.equal({
        hostName: 'example.cloud.databricks.com',
        httpPath: '/sql/1.0/warehouses/abc',
        authMode: 'OAuthM2m',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
      });

      await session.close();
      await backend.close();
    });

    it('rejects connect() for missing oauthClientId before touching the binding', async () => {
      const { binding, calls } = makeFakeBinding();
      const backend = new SeaBackend(binding);

      let caught: unknown;
      try {
        await backend.connect({
          host: 'example.cloud.databricks.com',
          path: '/sql/1.0/warehouses/abc',
          authType: 'databricks-oauth',
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          oauthClientSecret: 'dose-fake-secret',
        } as any);
      } catch (e) {
        caught = e;
      }
      expect(caught).to.be.instanceOf(AuthenticationError);
      expect(calls).to.have.lengthOf(0);
    });
  });
});
