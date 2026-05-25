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
  });
});
