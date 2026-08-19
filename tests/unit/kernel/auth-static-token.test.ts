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
import { buildKernelConnectionOptions } from '../../../lib/kernel/KernelAuth';
import AuthenticationError from '../../../lib/errors/AuthenticationError';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

describe('KernelAuth — static-token auth options builder', () => {
  it('maps a static token to the native bearer-token mode', () => {
    const native = buildKernelConnectionOptions({
      host: 'example.cloud.databricks.com',
      path: '/sql/1.0/warehouses/abc',
      authType: 'static-token',
      staticToken: 'header.payload.signature',
    });

    expectNativeConnectionOptions(native, {
      hostName: 'example.cloud.databricks.com',
      httpPath: '/sql/1.0/warehouses/abc',
      intervalsAsString: true,
      authMode: 'Pat',
      token: 'header.payload.signature',
    });
  });

  it('forwards federationClientId when token federation is enabled', () => {
    const native = buildKernelConnectionOptions({
      host: 'example.cloud.databricks.com',
      path: '/sql/1.0/warehouses/abc',
      authType: 'static-token',
      staticToken: 'header.payload.signature',
      enableTokenFederation: true,
      federationClientId: 'federation-client',
    });

    expect(native.identityFederationClientId).to.equal('federation-client');
  });

  it('does not forward federationClientId when token federation is disabled', () => {
    for (const enableTokenFederation of [undefined, false]) {
      const native = buildKernelConnectionOptions({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'static-token',
        staticToken: 'header.payload.signature',
        enableTokenFederation,
        federationClientId: 'federation-client',
      });

      expect(native).not.to.have.property('identityFederationClientId');
    }
  });

  it('omits an empty federationClientId', () => {
    const native = buildKernelConnectionOptions({
      host: 'example.cloud.databricks.com',
      path: '/sql/1.0/warehouses/abc',
      authType: 'static-token',
      staticToken: 'header.payload.signature',
      enableTokenFederation: true,
      federationClientId: '',
    });

    expect(native).not.to.have.property('identityFederationClientId');
  });

  it('rejects a missing or blank static token', () => {
    for (const staticToken of [undefined, '', '   ', 'undefined', 'null']) {
      expect(() =>
        buildKernelConnectionOptions({
          host: 'example.cloud.databricks.com',
          path: '/sql/1.0/warehouses/abc',
          authType: 'static-token',
          staticToken,
        } as any),
      ).to.throw(AuthenticationError, /non-empty token.*`staticToken`/);
    }
  });

  it('rejects conflicting OAuth credentials', () => {
    for (const conflicting of [{ oauthClientId: 'oauth-client' }, { oauthClientSecret: 'oauth-secret' }]) {
      expect(() =>
        buildKernelConnectionOptions({
          host: 'example.cloud.databricks.com',
          path: '/sql/1.0/warehouses/abc',
          authType: 'static-token',
          staticToken: 'header.payload.signature',
          ...conflicting,
        } as any),
      ).to.throw(HiveDriverError, /cannot supply `staticToken` alongside/);
    }
  });
});
