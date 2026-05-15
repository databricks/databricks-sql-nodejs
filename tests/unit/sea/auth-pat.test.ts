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
import { buildSeaConnectionOptions } from '../../../lib/sea/SeaAuth';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import AuthenticationError from '../../../lib/errors/AuthenticationError';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

describe('SeaAuth — PAT auth options builder', () => {
  describe('buildSeaConnectionOptions', () => {
    it('accepts a bare access-token PAT (undefined authType)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native).to.deep.equal({
        hostName: 'example.cloud.databricks.com',
        httpPath: '/sql/1.0/warehouses/abc',
        authMode: 'Pat',
        token: 'dapi-fake-pat',
      });
    });

    it('accepts an explicit access-token PAT', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'access-token',
        token: 'dapi-fake-pat',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.authMode).to.equal('Pat');
      if (native.authMode === 'Pat') {
        expect(native.token).to.equal('dapi-fake-pat');
      }
    });

    it('prepends `/` to a path missing the leading slash', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: 'sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.httpPath).to.equal('/sql/1.0/warehouses/abc');
    });

    it('throws AuthenticationError when token is missing', () => {
      const opts = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'access-token',
        // no token
      } as unknown as ConnectionOptions;

      expect(() => buildSeaConnectionOptions(opts)).to.throw(AuthenticationError, /non-empty PAT/);
    });

    it('throws AuthenticationError when token is an empty string', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: '',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(AuthenticationError, /non-empty PAT/);
    });

    it('accepts databricks-oauth without oauthClientSecret as the U2M happy path', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthU2m');
    });

    it('rejects token-provider with a clear unsupported-mode error', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'token-provider',
        tokenProvider: { getToken: async () => 'tok' } as unknown as ConnectionOptions extends infer T
          ? // eslint-disable-next-line @typescript-eslint/no-explicit-any
            any
          : never,
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /unsupported auth mode 'token-provider'/,
      );
    });

    it('rejects external-token, static-token, and custom auth modes', () => {
      const authTypes = ['external-token', 'static-token', 'custom'] as const;
      for (const authType of authTypes) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const opts = {
          host: 'h',
          path: '/p',
          authType,
        } as any;
        expect(() => buildSeaConnectionOptions(opts)).to.throw(
          HiveDriverError,
          /unsupported auth mode/,
        );
      }
    });
  });

  // Note: SeaBackend.connect/openSession round-trip + error-path coverage
  // moved to tests/unit/sea/execution.test.ts during the sea-integration
  // merge (the execution branch's SeaBackend constructor signature
  // {context, nativeBinding} supersedes the auth-only (binding) shape).
  // OAuth-specific flow-dispatch tests live in auth-m2m.test.ts and
  // auth-u2m.test.ts; M2M end-to-end against a live workspace lives in
  // tests/integration/sea/auth-m2m-e2e.test.ts.
});
