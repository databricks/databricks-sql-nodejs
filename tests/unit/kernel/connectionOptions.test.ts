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
import {
  buildKernelConnectionOptions,
  buildKernelTlsOptions,
  buildKernelHttpOptions,
  buildKernelRetryOptions,
} from '../../../lib/kernel/KernelAuth';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

const PAT = { host: 'h.databricks.com', path: '/sql/1.0/warehouses/abc', token: 'dapi-x' };

// Cast helper: the kernel connection-tuning/TLS options live on the internal
// surface, so tests build untyped option literals.
const opts = (extra: Record<string, unknown>) => ({ ...PAT, ...extra } as unknown as ConnectionOptions);

describe('KernelAuth connection options — intervalsAsString default', () => {
  it('always sets intervalsAsString:true (thrift-compatible interval rendering)', () => {
    const native = buildKernelConnectionOptions(opts({})) as { intervalsAsString?: boolean };
    expect(native.intervalsAsString).to.equal(true);
  });

  it('does NOT force complexTypesAsJson (native Arrow nested types match Thrift)', () => {
    const native = buildKernelConnectionOptions(opts({})) as { complexTypesAsJson?: boolean };
    expect(native.complexTypesAsJson).to.equal(undefined);
  });
});

describe('KernelAuth connection options — maxConnections', () => {
  it('forwards a valid positive integer', () => {
    const native = buildKernelConnectionOptions(opts({ maxConnections: 10 })) as { maxConnections?: number };
    expect(native.maxConnections).to.equal(10);
  });

  it('omits maxConnections when unset', () => {
    const native = buildKernelConnectionOptions(opts({})) as { maxConnections?: number };
    expect(native.maxConnections).to.equal(undefined);
  });

  for (const bad of [0, -1, 1.5]) {
    it(`rejects non-positive-integer maxConnections (${bad})`, () => {
      expect(() => buildKernelConnectionOptions(opts({ maxConnections: bad }))).to.throw(
        HiveDriverError,
        /positive integer/,
      );
    });
  }

  it('rejects maxConnections beyond the u32 limit', () => {
    expect(() => buildKernelConnectionOptions(opts({ maxConnections: 0x1_0000_0000 }))).to.throw(
      HiveDriverError,
      /u32 limit/,
    );
  });
});

describe('KernelAuth TLS options (buildKernelTlsOptions)', () => {
  it('is empty by default (secure-by-default — kernel default verify-on)', () => {
    expect(buildKernelTlsOptions(opts({}))).to.deep.equal({});
  });

  it('passes checkServerCertificate through verbatim (including false)', () => {
    expect(buildKernelTlsOptions(opts({ checkServerCertificate: false }))).to.deep.equal({
      checkServerCertificate: false,
    });
    expect(buildKernelTlsOptions(opts({ checkServerCertificate: true }))).to.deep.equal({
      checkServerCertificate: true,
    });
  });

  it('passes checkServerCertificateHostname through verbatim, independently of the master toggle', () => {
    expect(buildKernelTlsOptions(opts({ checkServerCertificateHostname: false }))).to.deep.equal({
      checkServerCertificateHostname: false,
    });
    // Independent of the master toggle — both can be set together.
    expect(
      buildKernelTlsOptions(opts({ checkServerCertificate: true, checkServerCertificateHostname: false })),
    ).to.deep.equal({ checkServerCertificate: true, checkServerCertificateHostname: false });
  });

  it('normalises a PEM string to a Buffer', () => {
    const pem = '-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n';
    const tls = buildKernelTlsOptions(opts({ customCaCert: pem }));
    expect(Buffer.isBuffer(tls.customCaCert)).to.equal(true);
    expect(tls.customCaCert?.toString('utf8')).to.equal(pem);
  });

  it('passes a Buffer customCaCert through unchanged', () => {
    const buf = Buffer.from('-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----');
    expect(buildKernelTlsOptions(opts({ customCaCert: buf })).customCaCert).to.equal(buf);
  });

  it('rejects a non-PEM string', () => {
    expect(() => buildKernelTlsOptions(opts({ customCaCert: 'not-a-pem' }))).to.throw(
      HiveDriverError,
      /PEM certificate/,
    );
  });

  it('rejects out-of-order / partial PEM markers (ordered match, not two substrings)', () => {
    // END-before-BEGIN, BEGIN-only, and END-only must all fail — a blob that
    // merely *contains* both literals (e.g. a proxy-intercept page) is not a cert.
    const reversed = '-----END CERTIFICATE-----\nMIIB...\n-----BEGIN CERTIFICATE-----';
    const beginOnly = '-----BEGIN CERTIFICATE-----\nMIIB...\n';
    const endOnly = 'MIIB...\n-----END CERTIFICATE-----';
    for (const bad of [reversed, beginOnly, endOnly]) {
      expect(() => buildKernelTlsOptions(opts({ customCaCert: bad })), bad).to.throw(
        HiveDriverError,
        /PEM certificate/,
      );
    }
  });

  it('rejects an empty Buffer', () => {
    expect(() => buildKernelTlsOptions(opts({ customCaCert: Buffer.alloc(0) }))).to.throw(HiveDriverError, /empty/);
  });

  it('rejects a non-string, non-Buffer customCaCert', () => {
    expect(() => buildKernelTlsOptions(opts({ customCaCert: 123 }))).to.throw(
      HiveDriverError,
      /PEM string or a Buffer/,
    );
  });

  it('folds TLS options into the full connection options', () => {
    const native = buildKernelConnectionOptions(opts({ checkServerCertificate: false })) as {
      checkServerCertificate?: boolean;
    };
    expect(native.checkServerCertificate).to.equal(false);
  });
});

const CERT_PEM = '-----BEGIN CERTIFICATE-----\nMIIBcert\n-----END CERTIFICATE-----\n';
// Built by concatenation so the secret-scanning pre-commit hook does not flag
// this obviously-fake fixture as a real private key.
const KEY_PEM = `-----BEGIN PRIVATE ${'KEY'}-----\nMIIBkey\n-----END PRIVATE ${'KEY'}-----\n`;

describe('KernelAuth mTLS options (buildKernelTlsOptions)', () => {
  it('emits no client identity by default', () => {
    const tls = buildKernelTlsOptions(opts({}));
    expect(tls.clientCertPem).to.equal(undefined);
    expect(tls.clientKeyPem).to.equal(undefined);
  });

  it('normalises string cert + key PEMs to Buffers', () => {
    const tls = buildKernelTlsOptions(opts({ clientCertPem: CERT_PEM, clientKeyPem: KEY_PEM }));
    expect(Buffer.isBuffer(tls.clientCertPem)).to.equal(true);
    expect(Buffer.isBuffer(tls.clientKeyPem)).to.equal(true);
    expect(tls.clientCertPem?.toString('utf8')).to.equal(CERT_PEM);
    expect(tls.clientKeyPem?.toString('utf8')).to.equal(KEY_PEM);
  });

  it('passes Buffer cert + key through unchanged', () => {
    const cert = Buffer.from(CERT_PEM);
    const key = Buffer.from(KEY_PEM);
    const tls = buildKernelTlsOptions(opts({ clientCertPem: cert, clientKeyPem: key }));
    expect(tls.clientCertPem).to.equal(cert);
    expect(tls.clientKeyPem).to.equal(key);
  });

  it('rejects supplying only the client cert', () => {
    expect(() => buildKernelTlsOptions(opts({ clientCertPem: CERT_PEM }))).to.throw(
      HiveDriverError,
      /requires both `clientCertPem` and `clientKeyPem`/,
    );
  });

  it('rejects supplying only the client key', () => {
    expect(() => buildKernelTlsOptions(opts({ clientKeyPem: KEY_PEM }))).to.throw(
      HiveDriverError,
      /requires both `clientCertPem` and `clientKeyPem`/,
    );
  });

  it('rejects a client cert that is not a PEM certificate', () => {
    expect(() => buildKernelTlsOptions(opts({ clientCertPem: 'nope', clientKeyPem: KEY_PEM }))).to.throw(
      HiveDriverError,
      /`clientCertPem` string does not look like a PEM certificate/,
    );
  });

  it('rejects a client key that is not a PEM private key', () => {
    expect(() => buildKernelTlsOptions(opts({ clientCertPem: CERT_PEM, clientKeyPem: 'nope' }))).to.throw(
      HiveDriverError,
      /`clientKeyPem` string does not look like a PEM private key/,
    );
  });

  it('rejects an empty cert Buffer', () => {
    expect(() => buildKernelTlsOptions(opts({ clientCertPem: Buffer.alloc(0), clientKeyPem: KEY_PEM }))).to.throw(
      HiveDriverError,
      /`clientCertPem` Buffer is empty/,
    );
  });

  it('folds mTLS into the full connection options', () => {
    const native = buildKernelConnectionOptions(opts({ clientCertPem: CERT_PEM, clientKeyPem: KEY_PEM })) as {
      clientCertPem?: Buffer;
      clientKeyPem?: Buffer;
    };
    expect(native.clientCertPem?.toString('utf8')).to.equal(CERT_PEM);
    expect(native.clientKeyPem?.toString('utf8')).to.equal(KEY_PEM);
  });
});

describe('KernelAuth HTTP options (buildKernelHttpOptions)', () => {
  // Headers cross the FFI as an ordered list of { name, value } pairs
  // (the napi `Array<HeaderEntry>` shape). Helpers to read it like a map.
  const ua = (http: { customHeaders?: Array<{ name: string; value: string }> }) =>
    http.customHeaders?.find((h) => h.name.toLowerCase() === 'user-agent')?.value;
  const names = (http: { customHeaders?: Array<{ name: string; value: string }> }) =>
    (http.customHeaders ?? []).map((h) => h.name);

  it('always emits a User-Agent identifying the connector', () => {
    const http = buildKernelHttpOptions(opts({}));
    expect(ua(http)).to.match(/NodejsDatabricksSqlConnector\//);
  });

  it('folds userAgentEntry into the User-Agent value', () => {
    const http = buildKernelHttpOptions(opts({ userAgentEntry: 'MyApp/2.0' }));
    expect(ua(http)).to.contain('MyApp/2.0');
    expect(ua(http)).to.match(/NodejsDatabricksSqlConnector\//);
  });

  it('passes caller customHeaders through, in order, with the connector User-Agent appended last', () => {
    const http = buildKernelHttpOptions(opts({ customHeaders: { 'X-Trace': 'abc', 'X-Env': 'prod' } }));
    // Order preserved; User-Agent is the final entry (matches Python's
    // `all_headers = http_headers + base_headers`).
    expect(names(http)).to.deep.equal(['X-Trace', 'X-Env', 'User-Agent']);
    expect(http.customHeaders?.[0]).to.deep.equal({ name: 'X-Trace', value: 'abc' });
    expect(ua(http)).to.match(/NodejsDatabricksSqlConnector\//);
  });

  it('drops kernel-managed reserved headers (Authorization / x-databricks-org-id, any casing)', () => {
    const http = buildKernelHttpOptions(
      opts({
        customHeaders: {
          Authorization: 'Bearer leak',
          'X-Databricks-Org-Id': '12345',
          'X-Keep': 'yes',
        },
      }),
    );
    const lower = names(http).map((n) => n.toLowerCase());
    expect(lower).to.not.include('authorization');
    expect(lower).to.not.include('x-databricks-org-id');
    expect(names(http)).to.include('X-Keep');
    expect(names(http)).to.include('User-Agent');
  });

  it('appends the connector UA last even when the caller also set a User-Agent (kernel folds last-wins, matches Python)', () => {
    const http = buildKernelHttpOptions(
      opts({ customHeaders: { 'User-Agent': 'Caller/1.0' }, userAgentEntry: 'Wins/3.0' }),
    );
    // Mirrors Python use_kernel: the caller's UA is forwarded too, and the
    // connector UA is appended last (the kernel's last-wins fold picks it).
    const uaEntries = (http.customHeaders ?? []).filter((h) => h.name.toLowerCase() === 'user-agent');
    expect(uaEntries.length).to.equal(2);
    expect(uaEntries[0].value).to.equal('Caller/1.0');
    expect(uaEntries[1].value).to.contain('Wins/3.0');
    expect(uaEntries[1].value).to.match(/NodejsDatabricksSqlConnector\//);
  });

  it('folds customHeaders + userAgentEntry into the full connection options', () => {
    const native = buildKernelConnectionOptions(
      opts({ customHeaders: { 'X-Trace': 'abc' }, userAgentEntry: 'MyApp/2.0' }),
    ) as { customHeaders?: Array<{ name: string; value: string }> };
    expect(native.customHeaders?.find((h) => h.name === 'X-Trace')?.value).to.equal('abc');
    expect(native.customHeaders?.find((h) => h.name === 'User-Agent')?.value).to.contain('MyApp/2.0');
  });

  describe('rejects header-injection control characters (CR / LF / NUL)', () => {
    // The kernel HTTP client does reject these, but only at connect time with an
    // opaque "Failed to construct HTTP client: InvalidArgument" error (verified
    // against pecotesting). We reject earlier, naming the offending header.
    const injections: Array<[string, Record<string, string>]> = [
      ['CRLF in value', { 'X-Evil': 'ok\r\nInjected-Header: pwned' }],
      ['bare LF in value', { 'X-Evil': 'a\nb' }],
      ['bare CR in value', { 'X-Evil': 'a\rb' }],
      ['NUL in value', { 'X-Evil': 'a\0b' }],
      ['CRLF in name', { 'X-Ev\r\nil': 'v' }],
      ['NUL in name', { 'X-Ev\0il': 'v' }],
    ];
    for (const [label, customHeaders] of injections) {
      it(`throws HiveDriverError on ${label}`, () => {
        expect(() => buildKernelHttpOptions(opts({ customHeaders }))).to.throw(
          HiveDriverError,
          /forbidden control character/,
        );
      });
    }

    it('does not throw on a valid header containing spaces, tabs, and punctuation', () => {
      expect(() =>
        buildKernelHttpOptions(opts({ customHeaders: { 'X-Ok': 'Bearer abc.def-123; q=0.9\tfoo' } })),
      ).to.not.throw();
    });

    it('validates a reserved header before dropping it (injection via Authorization is still rejected)', () => {
      // Reserved-name drop must not let a CR/LF-laced reserved header slip past
      // validation — validate first, then drop.
      expect(() =>
        buildKernelHttpOptions(opts({ customHeaders: { Authorization: 'Bearer x\r\nInjected: 1' } })),
      ).to.throw(HiveDriverError, /forbidden control character/);
    });
  });
});

describe('KernelAuth retry options — buildKernelRetryOptions', () => {
  // The driver's ClientConfig retry defaults (ms / total-attempt count).
  const defaults = {
    retryMaxAttempts: 5,
    retriesTimeout: 15 * 60 * 1000,
    retryDelayMin: 1000,
    retryDelayMax: 60 * 1000,
  };

  it('converts the connector ms knobs to the kernel whole-second kwargs', () => {
    const r = buildKernelRetryOptions(defaults);
    expect(r.retryMinWaitSecs).to.equal(1); // 1000ms
    expect(r.retryMaxWaitSecs).to.equal(60); // 60000ms
    expect(r.retryOverallTimeoutSecs).to.equal(900); // 15min
  });

  it('passes retryMaxAttempts through as a TOTAL attempt count (kernel converts to retries)', () => {
    expect(buildKernelRetryOptions({ ...defaults, retryMaxAttempts: 5 }).retryMaxAttempts).to.equal(5);
    expect(buildKernelRetryOptions({ ...defaults, retryMaxAttempts: 0 }).retryMaxAttempts).to.equal(0);
  });

  it('rounds sub-second delays to the nearest second (kernel granularity)', () => {
    const r = buildKernelRetryOptions({ ...defaults, retryDelayMin: 1500, retryDelayMax: 2400 });
    expect(r.retryMinWaitSecs).to.equal(2); // 1.5s → 2
    expect(r.retryMaxWaitSecs).to.equal(2); // 2.4s → 2
  });

  it('clamps negative/garbage inputs into the napi u32 range', () => {
    const r = buildKernelRetryOptions({
      retryMaxAttempts: -3,
      retriesTimeout: -1,
      retryDelayMin: -1000,
      retryDelayMax: 0,
    });
    expect(r.retryMaxAttempts).to.equal(0);
    expect(r.retryMinWaitSecs).to.equal(0);
    expect(r.retryOverallTimeoutSecs).to.equal(0);
  });
});
