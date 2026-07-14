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
import KernelBackend from '../../../lib/kernel/KernelBackend';
import { buildKernelConnectionOptions } from '../../../lib/kernel/KernelAuth';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import AuthenticationError from '../../../lib/errors/AuthenticationError';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import { makeFakeBinding, makeFakeContext } from './_helpers/fakeBinding';

describe('KernelAuth — edge cases (input validation + ambiguity)', () => {
  describe('whitespace-only and reserved-literal credentials are rejected', () => {
    it('rejects whitespace-only PAT', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: '   \t  ',
      };

      expect(() => buildKernelConnectionOptions(opts)).to.throw(AuthenticationError, /non-empty PAT/);
    });

    it('rejects literal "undefined" as PAT (buggy shell-export hazard)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'undefined',
      };

      expect(() => buildKernelConnectionOptions(opts)).to.throw(AuthenticationError, /non-empty PAT/);
    });

    it('rejects literal "null" as PAT', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'null',
      };

      expect(() => buildKernelConnectionOptions(opts)).to.throw(AuthenticationError, /non-empty PAT/);
    });

    it('rejects mixed-case "UNDEFINED" / "Null" / "NULL" as PAT (case-insensitive)', () => {
      for (const reserved of ['UNDEFINED', 'Undefined', 'Null', 'NULL', 'nUlL']) {
        const opts: ConnectionOptions = {
          host: 'example.cloud.databricks.com',
          path: '/sql/1.0/warehouses/abc',
          token: reserved,
        };

        expect(() => buildKernelConnectionOptions(opts), `for token=${reserved}`).to.throw(
          AuthenticationError,
          /non-empty PAT/,
        );
      }
    });

    // Strict Thrift parity: flow = `oauthClientSecret === undefined ? U2M : M2M`,
    // and OAuth fields are forwarded VERBATIM (no blank/reserved normalization),
    // exactly as the Thrift driver does. A present-but-degenerate secret
    // (`""` / whitespace / `"undefined"`) therefore counts as a real secret ⇒ M2M
    // — byte-for-byte with Thrift (which only routes to U2M when the secret is
    // strictly `undefined`). The id rides through as `oauthClientId ?? default`.
    const m2mDegenerateSecret = [
      { label: 'reserved-literal "NULL"', secret: 'NULL' },
      { label: 'whitespace-only', secret: '\n\t' },
      { label: 'literal "undefined"', secret: 'undefined' },
      { label: 'empty string', secret: '' },
    ];
    for (const { label, secret } of m2mDegenerateSecret) {
      it(`routes id + ${label} secret to M2M (secret !== undefined ⇒ M2M, like Thrift)`, () => {
        const native = buildKernelConnectionOptions({
          host: 'example.cloud.databricks.com',
          path: '/sql/1.0/warehouses/abc',
          authType: 'databricks-oauth',
          oauthClientId: 'client-uuid',
          oauthClientSecret: secret,
        } as ConnectionOptions);
        expect(native.authMode).to.equal('OAuthM2m');
        expect((native as { oauthClientId?: string }).oauthClientId).to.equal('client-uuid');
      });
    }

    // Degenerate ids on M2M are forwarded verbatim (`oauthClientId ?? default`
    // keeps a non-nullish value), NOT rejected — matching Thrift's getClientId().
    for (const id of ['   ', 'undefined']) {
      it(`forwards a degenerate oauthClientId (${JSON.stringify(id)}) verbatim on M2M`, () => {
        const native = buildKernelConnectionOptions({
          host: 'example.cloud.databricks.com',
          path: '/sql/1.0/warehouses/abc',
          authType: 'databricks-oauth',
          oauthClientId: id,
          oauthClientSecret: 'dose-fake-secret',
        } as ConnectionOptions);
        expect(native.authMode).to.equal('OAuthM2m');
        expect((native as { oauthClientId?: string }).oauthClientId).to.equal(id);
      });
    }

    // No secret ⇒ U2M, and a degenerate id is forwarded verbatim too (Thrift's
    // `oauthClientId ?? default` keeps a non-nullish whitespace id).
    it('routes a whitespace oauthClientId with no secret to U2M, forwarding the id verbatim', () => {
      const native = buildKernelConnectionOptions({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: '   ',
      } as unknown as ConnectionOptions);
      expect(native.authMode).to.equal('OAuthU2m');
      expect((native as { oauthClientId?: string }).oauthClientId).to.equal('   ');
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

      expect(() => buildKernelConnectionOptions(opts)).to.throw(
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

      expect(() => buildKernelConnectionOptions(opts)).to.throw(
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

      expect(() => buildKernelConnectionOptions(opts)).to.throw(
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

      expect(() => buildKernelConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /cannot supply `token` alongside `authType: 'databricks-oauth'`/,
      );
    });

    // NF-N3: a blank `oauthClientSecret` (the
    // `process.env.MY_SECRET || ''` shape) should route to U2M, not
    // to the M2M arm with an "empty secret" rejection. M2M's error
    // message would never mention U2M, leaving the user stuck.
    // Strict Thrift parity: a present-but-degenerate secret (no id) is still a
    // defined secret ⇒ M2M (only a strictly-`undefined` secret routes to U2M).
    // With no id, the client defaults via `oauthClientId ?? default`.
    for (const secret of ['', '   \t  ', 'undefined']) {
      it(`routes a degenerate-but-present oauthClientSecret (${JSON.stringify(secret)}, no id) to M2M`, () => {
        const native = buildKernelConnectionOptions({
          host: 'example.cloud.databricks.com',
          path: '/sql/1.0/warehouses/abc',
          authType: 'databricks-oauth',
          oauthClientSecret: secret,
        } as ConnectionOptions);
        expect(native.authMode).to.equal('OAuthM2m');
        expect((native as { oauthClientId?: string }).oauthClientId).to.equal('databricks-sql-connector');
      });
    }
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

      const native = buildKernelConnectionOptions(opts);
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

      const native = buildKernelConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthM2m');
    });

    it('accepts explicit `azureTenantId: undefined` on U2M too', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        azureTenantId: undefined,
      };

      const native = buildKernelConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthU2m');
    });
  });
});

describe('KernelBackend — kernel error envelope decoding (DA-F1)', () => {
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
    const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });
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
    const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });
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
    const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });
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
    const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });
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
    const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });
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
    const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });
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
    const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });
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
    const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });
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
    const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });
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
    const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });
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

  // NF-1: KernelSessionBackend.close() must wrap the napi call too.
  it('KernelSessionBackend.close() decodes kernel-error envelopes from native.close()', async () => {
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

    const backend = new KernelBackend({ nativeBinding: binding, context: makeFakeContext() });
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
