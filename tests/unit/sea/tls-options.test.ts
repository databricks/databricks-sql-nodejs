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
import { buildSeaConnectionOptions, buildSeaTlsOptions } from '../../../lib/sea/SeaAuth';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

const PEM = '-----BEGIN CERTIFICATE-----\nMIIBfakebase64\n-----END CERTIFICATE-----\n';

function patOpts(extra: Partial<ConnectionOptions> = {}): ConnectionOptions {
  return {
    host: 'example.cloud.databricks.com',
    path: '/sql/1.0/warehouses/abc',
    token: 'dapi-fake-pat',
    ...extra,
  } as ConnectionOptions;
}

describe('SeaAuth — TLS options', () => {
  describe('buildSeaTlsOptions', () => {
    it('returns an empty object when no TLS options are set (⇒ napi secure default)', () => {
      expect(buildSeaTlsOptions(patOpts())).to.deep.equal({});
    });

    it('passes checkServerCertificate: true through', () => {
      expect(buildSeaTlsOptions(patOpts({ checkServerCertificate: true }))).to.deep.equal({
        checkServerCertificate: true,
      });
    });

    it('passes checkServerCertificate: false through explicitly', () => {
      expect(buildSeaTlsOptions(patOpts({ checkServerCertificate: false }))).to.deep.equal({
        checkServerCertificate: false,
      });
    });

    it('converts a PEM string customCaCert to a Buffer', () => {
      const tls = buildSeaTlsOptions(patOpts({ customCaCert: PEM }));
      expect(Buffer.isBuffer(tls.customCaCert)).to.equal(true);
      expect(tls.customCaCert!.toString('utf8')).to.equal(PEM);
    });

    it('passes a Buffer customCaCert through unchanged', () => {
      const buf = Buffer.from(PEM, 'utf8');
      const tls = buildSeaTlsOptions(patOpts({ customCaCert: buf }));
      expect(tls.customCaCert).to.equal(buf);
    });

    it('honours customCaCert regardless of checkServerCertificate', () => {
      const tls = buildSeaTlsOptions(patOpts({ checkServerCertificate: true, customCaCert: PEM }));
      expect(tls.checkServerCertificate).to.equal(true);
      expect(Buffer.isBuffer(tls.customCaCert)).to.equal(true);
    });

    it('throws on a customCaCert string without a PEM header', () => {
      expect(() => buildSeaTlsOptions(patOpts({ customCaCert: 'not-a-pem' }))).to.throw(
        HiveDriverError,
        /does not look like a PEM certificate/,
      );
    });

    it('throws on an empty customCaCert Buffer', () => {
      expect(() => buildSeaTlsOptions(patOpts({ customCaCert: Buffer.alloc(0) }))).to.throw(HiveDriverError, /empty/);
    });
  });

  describe('buildSeaConnectionOptions integration', () => {
    it('omits TLS keys entirely when not supplied', () => {
      const native = buildSeaConnectionOptions(patOpts());
      expect(native).to.not.have.property('checkServerCertificate');
      expect(native).to.not.have.property('customCaCert');
    });

    it('threads TLS options onto the napi shape alongside auth', () => {
      const native = buildSeaConnectionOptions(patOpts({ checkServerCertificate: true, customCaCert: PEM }));
      expect(native.authMode).to.equal('Pat');
      expect(native.checkServerCertificate).to.equal(true);
      expect(native.customCaCert!.toString('utf8')).to.equal(PEM);
    });

    it('propagates customCaCert validation errors', () => {
      expect(() => buildSeaConnectionOptions(patOpts({ customCaCert: 'garbage' }))).to.throw(HiveDriverError);
    });
  });
});
