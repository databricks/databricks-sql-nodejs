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

      expect(() => buildSeaConnectionOptions(opts)).to.throw(AuthenticationError, /non-empty PAT/);
    });

    it('rejects literal "undefined" as PAT (buggy shell-export hazard)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'undefined',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(AuthenticationError, /non-empty PAT/);
    });

    it('rejects literal "null" as PAT', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'null',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(AuthenticationError, /non-empty PAT/);
    });

    it('rejects mixed-case "UNDEFINED" / "Null" / "NULL" as PAT (case-insensitive)', () => {
      for (const reserved of ['UNDEFINED', 'Undefined', 'Null', 'NULL', 'nUlL']) {
        const opts: ConnectionOptions = {
          host: 'example.cloud.databricks.com',
          path: '/sql/1.0/warehouses/abc',
          token: reserved,
        };

        expect(() => buildSeaConnectionOptions(opts), `for token=${reserved}`).to.throw(
          AuthenticationError,
          /non-empty PAT/,
        );
      }
    });

    // Round-4 NF3-2: presence of `oauthClientId` signals M2M intent.
    // A blank/reserved-literal `oauthClientSecret` is then a missing-secret
    // typo, not a request to fall back to U2M. Surface the M2M "secret
    // required" AuthenticationError so the user fixes the real problem
    // rather than swap class to a HiveDriverError pointing at a flow
    // they didn't intend to use.
    it('rejects mixed-case reserved-literal oauthClientSecret with AuthenticationError when id is set', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'NULL',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /oauthClientSecret.*non-empty.*OAuth M2M/,
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

      expect(() => buildSeaConnectionOptions(opts)).to.throw(AuthenticationError, /oauthClientId.*required/);
    });

    it('rejects whitespace-only oauthClientSecret with AuthenticationError when oauthClientId is set (M2M intent)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: '\n\t',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /oauthClientSecret.*non-empty.*OAuth M2M/,
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

      expect(() => buildSeaConnectionOptions(opts)).to.throw(AuthenticationError, /oauthClientId.*required/);
    });

    it('rejects literal "undefined" as oauthClientSecret with AuthenticationError when id is set (M2M intent)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-uuid',
        oauthClientSecret: 'undefined',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /oauthClientSecret.*non-empty.*OAuth M2M/,
      );
    });

    // Round-4 NF3-2: pin the exact class against the round-3 NF-N3
    // regression where M2M-with-empty-secret was routed through the U2M
    // arm and raised a bare `HiveDriverError`. `instanceof
    // AuthenticationError` correctly returns `false` for a bare
    // `HiveDriverError` instance (instanceof is a one-way subclass
    // check), so the subclass check IS sufficient to catch the
    // regression. We don't add an `error.name` or `constructor.name`
    // belt — the former requires `this.name` on the subclass (LE4-1
    // handles that separately for downstream-consumer benefit, not for
    // this test), and the latter is bundler-fragile (terser/esbuild
    // strip class names without `keep_classnames`).
    it('M2M-with-empty-secret throws AuthenticationError, not bare HiveDriverError (class pin)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'x',
        oauthClientSecret: '',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        AuthenticationError,
        /oauthClientSecret.*non-empty.*OAuth M2M/,
      );
    });

    // Round-5 DA4-2: the round-3 → round-4 test flips left the U2M-arm
    // defense-in-depth U2M+id rejection without coverage. It's still
    // reachable: when `oauthClientId` is a blank-reserved literal
    // (whitespace, `"null"`, `"undefined"`) AND `oauthClientSecret` is
    // absent/blank, BOTH `idIsBlank` and `secretIsBlank` are true so
    // U2M wins routing — but a non-undefined id signals ambiguity that
    // U2M cannot honor (the kernel hardcodes `databricks-cli`).
    it('routes a whitespace oauthClientId with no oauthClientSecret to the U2M defense-in-depth rejection', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: '   ',
      } as unknown as ConnectionOptions;

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /oauthClientId.*not supported on the OAuth U2M flow/,
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

    // NF-N3: a blank `oauthClientSecret` (the
    // `process.env.MY_SECRET || ''` shape) should route to U2M, not
    // to the M2M arm with an "empty secret" rejection. M2M's error
    // message would never mention U2M, leaving the user stuck.
    it('routes blank oauthClientSecret to U2M (not to an M2M-blank-secret rejection)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientSecret: '',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthU2m');
    });

    it('routes whitespace-only oauthClientSecret to U2M too', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientSecret: '   \t  ',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthU2m');
    });

    it('routes literal-"undefined" oauthClientSecret to U2M too', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientSecret: 'undefined',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthU2m');
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
    const backend = new SeaBackend({ nativeBinding: binding });
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
    const backend = new SeaBackend({ nativeBinding: binding });
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
    const binding = bindingRejectingWith('{"code":"Unauthenticated","message":"forbidden","sqlState":"28000"}');
    const backend = new SeaBackend({ nativeBinding: binding });
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
    const backend = new SeaBackend({ nativeBinding: binding });
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

  it('falls back to original Error for a corrupted envelope, stripping the internal sentinel', async () => {
    const binding = bindingRejectingWith('not valid json');
    const backend = new SeaBackend({ nativeBinding: binding });
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
    // Round-4 NF3-3: the `__databricks_error__:` prefix is an internal
    // JS<->binding framing marker; it must not leak to the user-facing
    // message even on the corrupted-envelope fallback path.
    expect((caught as Error).message).to.not.match(/^__databricks_error__:/);
    expect((caught as Error).message).to.equal('not valid json');
  });

  // NF-4 / NF-N1: preserve the 5 optional kernel envelope fields on the
  // decoded JS error under a single `kernelMetadata` namespace.
  // Namespaced to avoid the collision with `OperationStateError.errorCode`
  // and `RetryError.errorCode` (both pre-existing enum fields switched
  // on at `DBSQLOperation.ts:209`).
  it('preserves errorCode + vendorCode + httpStatus + retryable + queryId under kernelMetadata namespace', async () => {
    const binding = bindingRejectingWith(
      '{"code":"Unavailable","message":"upstream timed out",' +
        '"sqlState":"08006","errorCode":"UPSTREAM_TIMEOUT","vendorCode":1234,' +
        '"httpStatus":503,"retryable":true,"queryId":"query-abc-123"}',
    );
    const backend = new SeaBackend({ nativeBinding: binding });
    await backend.connect(validConnectArgs);

    let caught: unknown;
    try {
      await backend.openSession({});
    } catch (e) {
      caught = e;
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const err = caught as any;
    expect(err.sqlState).to.equal('08006');
    expect(err.kernelMetadata).to.deep.equal({
      errorCode: 'UPSTREAM_TIMEOUT',
      vendorCode: 1234,
      httpStatus: 503,
      retryable: true,
      queryId: 'query-abc-123',
    });
  });

  it('keeps sqlState and kernelMetadata non-enumerable (matches Node `.code` pattern)', async () => {
    const binding = bindingRejectingWith('{"code":"NetworkError","message":"x","sqlState":"08000","httpStatus":502}');
    const backend = new SeaBackend({ nativeBinding: binding });
    await backend.connect(validConnectArgs);

    let caught: unknown;
    try {
      await backend.openSession({});
    } catch (e) {
      caught = e;
    }
    expect(Object.keys(caught as object)).to.not.include('sqlState');
    expect(Object.keys(caught as object)).to.not.include('kernelMetadata');
    // But direct access still works.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const err = caught as any;
    expect(err.sqlState).to.equal('08000');
    expect(err.kernelMetadata?.httpStatus).to.equal(502);
  });

  // NF-N1: namespace must NOT clobber a pre-existing `errorCode` enum
  // field on OperationStateError / RetryError. Cancelled envelopes map
  // to OperationStateError(Canceled), and DBSQLOperation.ts:209 switches
  // on `err.errorCode === OperationStateErrorCode.Canceled` — that must
  // continue to read the enum 'CANCELED', not the kernel's textual
  // errorCode.
  it('does not clobber OperationStateError.errorCode enum when kernel envelope sends a textual errorCode', async () => {
    const binding = bindingRejectingWith(
      '{"code":"Cancelled","message":"user-cancel","errorCode":"USER_REQUESTED_CANCEL"}',
    );
    const backend = new SeaBackend({ nativeBinding: binding });
    await backend.connect(validConnectArgs);

    let caught: unknown;
    try {
      await backend.openSession({});
    } catch (e) {
      caught = e;
    }
    // The enum-typed top-level errorCode is untouched (still the
    // CANCELED enum string from OperationStateError's constructor).
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const err = caught as any;
    expect(err.errorCode).to.equal('CANCELED');
    // The kernel's textual errorCode survives under the namespace.
    expect(err.kernelMetadata?.errorCode).to.equal('USER_REQUESTED_CANCEL');
  });

  // NF-N4: per-field type guards. If the kernel sends a wrong-typed
  // field (e.g. `retryable: "true"` string instead of `true` boolean),
  // the decoder should drop that field rather than propagate the
  // wrong type.
  it('drops envelope fields with the wrong runtime type instead of passing them through', async () => {
    // errorCode wrong-type (number instead of string), vendorCode
    // wrong-type (string instead of number), httpStatus correct,
    // retryable wrong-type (string instead of boolean), queryId null.
    // Only httpStatus should survive the type-guard.
    const binding = bindingRejectingWith(
      '{"code":"NetworkError","message":"x","errorCode":42,"vendorCode":"not-a-number","httpStatus":502,"retryable":"true","queryId":null}',
    );
    const backend = new SeaBackend({ nativeBinding: binding });
    await backend.connect(validConnectArgs);

    let caught: unknown;
    try {
      await backend.openSession({});
    } catch (e) {
      caught = e;
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const err = caught as any;
    // Only the well-typed httpStatus survives.
    expect(err.kernelMetadata).to.deep.equal({ httpStatus: 502 });
  });

  it('omits the kernelMetadata namespace entirely when no envelope fields survive validation', async () => {
    // A minimal envelope (just code + message + sqlState) yields an
    // empty metadata object — and we should NOT attach a `{}`-shaped
    // namespace because that's pure noise. The sqlState top-level
    // field is unaffected.
    const binding = bindingRejectingWith('{"code":"Internal","message":"x","sqlState":"08001"}');
    const backend = new SeaBackend({ nativeBinding: binding });
    await backend.connect(validConnectArgs);

    let caught: unknown;
    try {
      await backend.openSession({});
    } catch (e) {
      caught = e;
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const err = caught as any;
    expect(err.sqlState).to.equal('08001');
    expect(err.kernelMetadata).to.equal(undefined);
  });

  // NF-1: SeaSessionBackend.close() must wrap the napi call too.
  it('SeaSessionBackend.close() decodes kernel-error envelopes from native.close()', async () => {
    const { binding } = makeFakeBinding();
    // Make openSession return a fake Connection whose close() throws
    // a kernel-shaped envelope.
    const failingClose = {
      async executeStatement() {
        throw new Error('unused');
      },
      async close() {
        throw new Error('__databricks_error__:{"code":"Internal","message":"server-side close failed"}');
      },
    };
    binding.openSession = (async () => failingClose as unknown) as typeof binding.openSession;

    const backend = new SeaBackend({ nativeBinding: binding });
    await backend.connect(validConnectArgs);
    const session = await backend.openSession({});

    let caught: unknown;
    try {
      await session.close();
    } catch (e) {
      caught = e;
    }
    // Before the NF-1 fix, this would surface as a raw Error whose
    // message starts with `__databricks_error__:`. After the fix, the
    // sentinel is stripped and the typed class is dispatched.
    expect(caught).to.be.instanceOf(HiveDriverError);
    expect((caught as Error).message).to.equal('server-side close failed');
    expect((caught as Error).message).to.not.contain('__databricks_error__');
  });
});
