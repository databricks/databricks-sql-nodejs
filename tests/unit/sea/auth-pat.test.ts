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
import { SeaNativeBinding } from '../../../lib/sea/SeaNativeLoader';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import AuthenticationError from '../../../lib/errors/AuthenticationError';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

/**
 * Fake napi binding that records the option object handed to `openSession`
 * and returns a fake `Connection` whose `close()` we can observe. No real
 * native code runs in this suite.
 */
function makeFakeBinding() {
  const calls: Array<{ method: string; args: unknown[] }> = [];

  const fakeConnection = {
    // Mirrors the kernel `Connection.sessionId` getter; SeaSessionBackend
    // surfaces this as its `id`.
    sessionId: '01ef-fake-session-id',
    async executeStatement() {
      throw new Error('not used in this test');
    },
    async close() {
      calls.push({ method: 'connection.close', args: [] });
    },
  };

  const binding: SeaNativeBinding = {
    version() {
      return 'fake-binding';
    },
    async openSession(opts: { hostName: string; httpPath: string; token: string }) {
      calls.push({ method: 'openSession', args: [opts] });
      // Cast through the binding's own member types: `SeaNativeBinding` is
      // `typeof import('../../native/sea')`, so `openSession`'s resolved
      // return type is the napi `Connection`. A bare `as unknown` stops
      // short of that and fails to satisfy the annotation.
      return fakeConnection as unknown as Awaited<ReturnType<SeaNativeBinding['openSession']>>;
    },
    // `Connection`/`Statement` are exported as type aliases in
    // SeaNativeLoader, so `typeof Connection` is illegal (TS2693); index
    // the binding type instead to get the napi class constructor type.
    Connection: function FakeConnection() {} as unknown as SeaNativeBinding['Connection'],
    Statement: function FakeStatement() {} as unknown as SeaNativeBinding['Statement'],
  };

  return { binding, calls };
}

describe('SeaAuth + SeaBackend — PAT auth flow', () => {
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
      expect(native.token).to.equal('dapi-fake-pat');
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

    it('rejects OAuth with a clear M0-scope error', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /M0\) supports only PAT.*databricks-oauth.*M1/,
      );
    });

    it('rejects token-provider with a clear M0-scope error', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'token-provider',
        tokenProvider: { getToken: async () => 'tok' } as unknown as ConnectionOptions extends infer T
          ? // eslint-disable-next-line @typescript-eslint/no-explicit-any
            any
          : never,
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(HiveDriverError, /token-provider.*M1/);
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
        expect(() => buildSeaConnectionOptions(opts)).to.throw(HiveDriverError, /M0\) supports only PAT/);
      }
    });
  });

  describe('SeaBackend.connect + openSession', () => {
    it('resolves on a valid PAT options object and round-trips through the napi binding', async () => {
      const { binding, calls } = makeFakeBinding();
      const backend = new SeaBackend(binding);

      await backend.connect({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
      });

      const session = await backend.openSession({});
      expect(session).to.exist;
      // id is the real server-issued session id (kernel `sessionId`).
      expect(session.id).to.equal('01ef-fake-session-id');

      expect(calls).to.have.lengthOf(1);
      expect(calls[0].method).to.equal('openSession');
      expect(calls[0].args[0]).to.deep.equal({
        hostName: 'example.cloud.databricks.com',
        httpPath: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
      });

      // Round-trip close.
      const status = await session.close();
      expect(status.isSuccess).to.equal(true);
      expect(calls[1].method).to.equal('connection.close');

      await backend.close();
    });

    it('rejects connect() when token is missing with AuthenticationError', async () => {
      const { binding, calls } = makeFakeBinding();
      const backend = new SeaBackend(binding);

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const opts = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'access-token',
      } as any;

      let caught: unknown;
      try {
        await backend.connect(opts);
      } catch (e) {
        caught = e;
      }
      expect(caught).to.be.instanceOf(AuthenticationError);
      expect(calls).to.have.lengthOf(0);
    });

    it('rejects connect() for OAuth with the M0-scope error', async () => {
      const { binding, calls } = makeFakeBinding();
      const backend = new SeaBackend(binding);

      let caught: unknown;
      try {
        await backend.connect({
          host: 'example.cloud.databricks.com',
          path: '/sql/1.0/warehouses/abc',
          authType: 'databricks-oauth',
        });
      } catch (e) {
        caught = e;
      }
      expect(caught).to.be.instanceOf(HiveDriverError);
      expect((caught as Error).message).to.match(/M0\) supports only PAT/);
      expect(calls).to.have.lengthOf(0);
    });

    it('throws when openSession() is called before connect()', async () => {
      const { binding } = makeFakeBinding();
      const backend = new SeaBackend(binding);

      let caught: unknown;
      try {
        await backend.openSession({});
      } catch (e) {
        caught = e;
      }
      expect(caught).to.be.instanceOf(HiveDriverError);
      expect((caught as Error).message).to.match(/connect\(\) must be called/);
    });

    it('stubbed session methods reject with a clear M0-scope error', async () => {
      const { binding } = makeFakeBinding();
      const backend = new SeaBackend(binding);

      await backend.connect({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
      });
      const session = await backend.openSession({});

      let caught: unknown;
      try {
        await session.executeStatement('SELECT 1', {});
      } catch (e) {
        caught = e;
      }
      expect(caught).to.be.instanceOf(HiveDriverError);
      expect((caught as Error).message).to.match(/not implemented in sea-auth \(M0\)/);
    });
  });
});
