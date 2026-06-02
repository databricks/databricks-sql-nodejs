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

const PAT = { host: 'h.databricks.com', path: '/sql/1.0/warehouses/abc', token: 'dapi-x' };

// Cast helper: the SEA connection-tuning/TLS options live on the internal
// surface, so tests build untyped option literals.
const opts = (extra: Record<string, unknown>) => ({ ...PAT, ...extra }) as unknown as ConnectionOptions;

describe('SeaAuth connection options — intervalsAsString default', () => {
  it('always sets intervalsAsString:true (thrift-compatible interval rendering)', () => {
    const native = buildSeaConnectionOptions(opts({})) as { intervalsAsString?: boolean };
    expect(native.intervalsAsString).to.equal(true);
  });

  it('does NOT force complexTypesAsJson (native Arrow nested types match Thrift)', () => {
    const native = buildSeaConnectionOptions(opts({})) as { complexTypesAsJson?: boolean };
    expect(native.complexTypesAsJson).to.equal(undefined);
  });
});

describe('SeaAuth connection options — maxConnections', () => {
  it('forwards a valid positive integer', () => {
    const native = buildSeaConnectionOptions(opts({ maxConnections: 10 })) as { maxConnections?: number };
    expect(native.maxConnections).to.equal(10);
  });

  it('omits maxConnections when unset', () => {
    const native = buildSeaConnectionOptions(opts({})) as { maxConnections?: number };
    expect(native.maxConnections).to.equal(undefined);
  });

  for (const bad of [0, -1, 1.5]) {
    it(`rejects non-positive-integer maxConnections (${bad})`, () => {
      expect(() => buildSeaConnectionOptions(opts({ maxConnections: bad }))).to.throw(
        HiveDriverError,
        /positive integer/,
      );
    });
  }

  it('rejects maxConnections beyond the u32 limit', () => {
    expect(() => buildSeaConnectionOptions(opts({ maxConnections: 0x1_0000_0000 }))).to.throw(
      HiveDriverError,
      /u32 limit/,
    );
  });
});

describe('SeaAuth TLS options (buildSeaTlsOptions)', () => {
  it('is empty by default (secure-by-default — kernel default verify-on)', () => {
    expect(buildSeaTlsOptions(opts({}))).to.deep.equal({});
  });

  it('passes checkServerCertificate through verbatim (including false)', () => {
    expect(buildSeaTlsOptions(opts({ checkServerCertificate: false }))).to.deep.equal({
      checkServerCertificate: false,
    });
    expect(buildSeaTlsOptions(opts({ checkServerCertificate: true }))).to.deep.equal({
      checkServerCertificate: true,
    });
  });

  it('normalises a PEM string to a Buffer', () => {
    const pem = '-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n';
    const tls = buildSeaTlsOptions(opts({ customCaCert: pem }));
    expect(Buffer.isBuffer(tls.customCaCert)).to.equal(true);
    expect(tls.customCaCert?.toString('utf8')).to.equal(pem);
  });

  it('passes a Buffer customCaCert through unchanged', () => {
    const buf = Buffer.from('-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----');
    expect(buildSeaTlsOptions(opts({ customCaCert: buf })).customCaCert).to.equal(buf);
  });

  it('rejects a non-PEM string', () => {
    expect(() => buildSeaTlsOptions(opts({ customCaCert: 'not-a-pem' }))).to.throw(
      HiveDriverError,
      /PEM certificate/,
    );
  });

  it('rejects an empty Buffer', () => {
    expect(() => buildSeaTlsOptions(opts({ customCaCert: Buffer.alloc(0) }))).to.throw(HiveDriverError, /empty/);
  });

  it('rejects a non-string, non-Buffer customCaCert', () => {
    expect(() => buildSeaTlsOptions(opts({ customCaCert: 123 }))).to.throw(HiveDriverError, /PEM string or a Buffer/);
  });

  it('folds TLS options into the full connection options', () => {
    const native = buildSeaConnectionOptions(opts({ checkServerCertificate: false })) as {
      checkServerCertificate?: boolean;
    };
    expect(native.checkServerCertificate).to.equal(false);
  });
});
