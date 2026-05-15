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

describe('SeaAuth — edge cases (input validation + ambiguity)', () => {
  describe('whitespace-only and reserved-literal credentials are rejected', () => {
    it('rejects whitespace-only PAT', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: '   \t  ',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /non-empty PAT/,
      );
    });

    it('rejects literal "undefined" as PAT (buggy shell-export hazard)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'undefined',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /non-empty PAT/,
      );
    });

    it('rejects literal "null" as PAT', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'null',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /non-empty PAT/,
      );
    });

    it('rejects whitespace-only oauthClientId on M2M', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: '   ',
        oauthClientSecret: 'dose-fake-secret',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /oauthClientId.*required/,
      );
    });

    it('rejects whitespace-only oauthClientSecret on M2M', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: '\n\t',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /oauthClientSecret.*non-empty non-whitespace/,
      );
    });

    it('rejects literal "undefined" as oauthClientId on M2M', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'undefined',
        oauthClientSecret: 'dose-fake-secret',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /oauthClientId.*required/,
      );
    });

    it('rejects literal "undefined" as oauthClientSecret on M2M', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'undefined',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /oauthClientSecret.*non-empty non-whitespace/,
      );
    });
  });

  describe('ambiguous credentials are rejected', () => {
    it('rejects PAT path with stray oauthClientId', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'access-token',
        token: 'dapi-fake-pat',
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        oauthClientId: 'client-uuid',
      } as any;

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /cannot supply both `token` and `oauthClientId/,
      );
    });

    it('rejects PAT path with stray oauthClientSecret', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'access-token',
        token: 'dapi-fake-pat',
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        oauthClientSecret: 'dose-fake-secret',
      } as any;

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /cannot supply both `token` and `oauthClientId/,
      );
    });

    it('rejects M2M path with stray token', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        token: 'dapi-fake-pat',
      } as any;

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /cannot supply `token` alongside `authType: 'databricks-oauth'`/,
      );
    });

    it('rejects U2M path with stray token', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        // no client secret → would be U2M, but token is set → rejected first
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        token: 'dapi-fake-pat',
      } as any;

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /cannot supply `token` alongside `authType: 'databricks-oauth'`/,
      );
    });
  });

  describe('explicit-undefined vs missing for Azure-direct discriminants', () => {
    it('accepts explicit `azureTenantId: undefined` on M2M (treated as not-set)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
        azureTenantId: undefined,
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthM2m');
    });

    it('accepts `useDatabricksOAuthInAzure: false` on M2M (only `=== true` rejects)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'dose-fake-secret',
        useDatabricksOAuthInAzure: false,
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthM2m');
    });

    it('accepts explicit `azureTenantId: undefined` on U2M too', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        azureTenantId: undefined,
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthU2m');
    });
  });
});

describe('SeaBackend — kernel error envelope decoding (DA-F1)', () => {
  /**
   * Build a fake binding whose `openSession` rejects with the verbatim
   * `__databricks_error__:{...}` envelope shape the napi binding's
   * `napi_err_from_kernel` produces. Used to exercise
   * `decodeNapiKernelError` end-to-end without compiling the native
   * module.
   */
  function bindingRejectingWith(envelopeJson: string) {
    const { binding } = makeFakeBinding();
    binding.openSession = (async () => {
      throw new Error(`__databricks_error__:${envelopeJson}`);
    }) as typeof binding.openSession;
    return binding;
  }

  const validConnectArgs: ConnectionOptions = {
    host: 'example.cloud.databricks.com',
    path: '/sql/1.0/warehouses/abc',
    token: 'dapi-fake-pat',
  };

  it('maps Unauthenticated kernel envelope → AuthenticationError with kernel message preserved', async () => {
    const binding = bindingRejectingWith(
      '{"code":"Unauthenticated","message":"OAuth M2M token exchange failed: invalid_client"}',
    );
    const backend = new SeaBackend(binding);
    await backend.connect(validConnectArgs);

    let caught: unknown;
    try {
      await backend.openSession({});
    } catch (e) {
      caught = e;
    }
    expect(caught).to.be.instanceOf(AuthenticationError);
    expect((caught as Error).message).to.match(/invalid_client/);
  });

  it('maps NetworkError kernel envelope → HiveDriverError with kernel message preserved', async () => {
    const binding = bindingRejectingWith(
      '{"code":"NetworkError","message":"OIDC discovery failed: connection refused"}',
    );
    const backend = new SeaBackend(binding);
    await backend.connect(validConnectArgs);

    let caught: unknown;
    try {
      await backend.openSession({});
    } catch (e) {
      caught = e;
    }
    expect(caught).to.be.instanceOf(HiveDriverError);
    expect((caught as Error).message).to.match(/OIDC discovery failed/);
  });

  it('preserves SQLSTATE on the decoded error when present', async () => {
    const binding = bindingRejectingWith(
      '{"code":"Unauthenticated","message":"forbidden","sqlState":"28000"}',
    );
    const backend = new SeaBackend(binding);
    await backend.connect(validConnectArgs);

    let caught: unknown;
    try {
      await backend.openSession({});
    } catch (e) {
      caught = e;
    }
    expect(caught).to.be.instanceOf(AuthenticationError);
    expect((caught as { sqlState?: string }).sqlState).to.equal('28000');
  });

  it('passes through plain napi errors (no sentinel) unchanged', async () => {
    const { binding } = makeFakeBinding();
    binding.openSession = (async () => {
      throw new Error('openSession: `token` is required for the requested auth mode');
    }) as typeof binding.openSession;
    const backend = new SeaBackend(binding);
    await backend.connect(validConnectArgs);

    let caught: unknown;
    try {
      await backend.openSession({});
    } catch (e) {
      caught = e;
    }
    expect(caught).to.be.instanceOf(Error);
    expect((caught as Error).message).to.match(/`token` is required/);
  });

  it('falls back to original Error for a corrupted envelope', async () => {
    const binding = bindingRejectingWith('not valid json');
    const backend = new SeaBackend(binding);
    await backend.connect(validConnectArgs);

    let caught: unknown;
    try {
      await backend.openSession({});
    } catch (e) {
      caught = e;
    }
    // Corrupted envelopes should NOT silently disappear — we return
    // the original Error so the operator sees the raw payload.
    expect(caught).to.be.instanceOf(Error);
    expect((caught as Error).message).to.contain('not valid json');
  });
});
