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
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import SeaBackend from '../../../lib/sea/SeaBackend';
import { makeFakeBinding } from './_helpers/fakeBinding';

describe('SeaAuth — maxConnections plumbing', () => {
  describe('buildSeaConnectionOptions forwards maxConnections', () => {
    it('omits maxConnections when not supplied (kernel default applies)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native).to.not.have.property('maxConnections');
    });

    it('forwards maxConnections as a positive integer', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
        maxConnections: 200,
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.maxConnections).to.equal(200);
    });

    it('accepts the minimum value (1)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
        maxConnections: 1,
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.maxConnections).to.equal(1);
    });

    it('rejects zero', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
        maxConnections: 0,
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(HiveDriverError, /maxConnections.*positive integer/);
    });

    it('rejects negative values', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
        maxConnections: -5,
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(HiveDriverError, /maxConnections.*positive integer/);
    });

    it('rejects non-integer values', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
        maxConnections: 1.5,
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(HiveDriverError, /maxConnections.*positive integer/);
    });

    it('rejects NaN', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
        maxConnections: NaN,
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(HiveDriverError, /maxConnections.*positive integer/);
    });

    it('forwards maxConnections through the OAuth M2M arm', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        oauthClientId: 'client-id',
        oauthClientSecret: 'client-secret',
        maxConnections: 50,
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthM2m');
      expect(native.maxConnections).to.equal(50);
    });

    it('forwards maxConnections through the OAuth U2M arm', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        authType: 'databricks-oauth',
        maxConnections: 25,
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.authMode).to.equal('OAuthU2m');
      expect(native.maxConnections).to.equal(25);
    });

    // ─── DA round-1 M1 fixup — upper-bound (u32 ceiling) ───────────

    it('accepts the maximum u32 value (4_294_967_295)', () => {
      const MAX_U32 = 4_294_967_295;
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
        maxConnections: MAX_U32,
      };

      const native = buildSeaConnectionOptions(opts);
      expect(native.maxConnections).to.equal(MAX_U32);
    });

    it('rejects values exceeding the napi u32 ceiling', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
        maxConnections: 4_294_967_296, // 2^32, one over u32 max
      };

      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /maxConnections.*exceeds.*u32 limit/,
      );
    });

    it('rejects 2^53 - 1 (safe integer ceiling — would silently truncate at FFI)', () => {
      const opts: ConnectionOptions = {
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
        maxConnections: Number.MAX_SAFE_INTEGER,
      };

      // The JS layer rejects before the FFI boundary so the napi
      // binding never sees a value it can't faithfully represent.
      expect(() => buildSeaConnectionOptions(opts)).to.throw(
        HiveDriverError,
        /maxConnections.*exceeds.*u32 limit/,
      );
    });
  });

  // ─── DA round-1 M2 fixup — mock-binding round-trip ──────────────

  describe('SeaBackend forwards maxConnections to the napi openSession call', () => {
    it('passes the user-supplied maxConnections value through', async () => {
      const { binding, calls } = makeFakeBinding();
      const backend = new SeaBackend({ nativeBinding: binding });

      await backend.connect({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
        maxConnections: 250,
      });
      await backend.openSession({});

      const openCall = calls.find((c) => c.method === 'openSession');
      expect(openCall, 'openSession must be called on the binding').to.not.equal(undefined);
      const opts = openCall!.args[0] as { maxConnections?: number; authMode?: string };
      expect(opts.maxConnections).to.equal(250);
      expect(opts.authMode).to.equal('Pat');
    });

    it('omits maxConnections from the openSession call when not supplied', async () => {
      const { binding, calls } = makeFakeBinding();
      const backend = new SeaBackend({ nativeBinding: binding });

      await backend.connect({
        host: 'example.cloud.databricks.com',
        path: '/sql/1.0/warehouses/abc',
        token: 'dapi-fake-pat',
      });
      await backend.openSession({});

      const openCall = calls.find((c) => c.method === 'openSession');
      expect(openCall).to.not.equal(undefined);
      const opts = openCall!.args[0] as { maxConnections?: number };
      // `undefined` (not `0`) — the napi binding's `Option<u32>` reads
      // this as None and applies the kernel default of 100.
      expect(opts.maxConnections).to.equal(undefined);
    });
  });
});
