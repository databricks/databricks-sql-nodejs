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
import { buildKernelConnectionOptions } from '../../../lib/kernel/KernelAuth';
import { ConnectionOptions } from '../../../lib/contracts/IDBSQLClient';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import AuthenticationError from '../../../lib/errors/AuthenticationError';

// A private-key file selects JWT client-assertion M2M (RFC 7523); the kernel
// signs a short-lived assertion with the key instead of sending a secret.
const baseJwt = {
  host: 'example.azuredatabricks.net',
  path: '/sql/1.0/warehouses/abc',
  authType: 'databricks-oauth' as const,
  oauthClientId: 'sp-uuid',
  oauthJwtKeyFile: '/keys/jwt.pem',
  oauthJwtKid: 'kid-1',
};

describe('KernelAuth — OAuth M2M JWT private-key auth flow', () => {
  it('routes oauthJwtKeyFile to authMode OAuthM2mJwt with the required fields', () => {
    const native = buildKernelConnectionOptions(baseJwt as ConnectionOptions);
    expect(native.authMode).to.equal('OAuthM2mJwt');
    const jwt = native as {
      oauthClientId?: string;
      jwtKeyFile?: string;
      jwtKid?: string;
      oauthScopes?: string[];
    };
    expect(jwt.oauthClientId).to.equal('sp-uuid');
    expect(jwt.jwtKeyFile).to.equal('/keys/jwt.pem');
    expect(jwt.jwtKid).to.equal('kid-1');
    // Defaults to the M2M scope (parity with pyo3 / the secret M2M path).
    expect(jwt.oauthScopes).to.deep.equal(['all-apis']);
  });

  it('forwards optional passphrase / algorithm / tokenUrl / scopes when present', () => {
    const native = buildKernelConnectionOptions({
      ...baseJwt,
      oauthJwtPassphrase: 'pw',
      oauthJwtAlgorithm: 'ES256',
      tokenUrl: 'https://login.microsoftonline.com/tenant/oauth2/v2.0/token',
      oauthScopes: ['2ff814a6-.../.default'],
    } as ConnectionOptions);
    const jwt = native as {
      jwtPassphrase?: string;
      jwtAlgorithm?: string;
      tokenUrl?: string;
      oauthScopes?: string[];
    };
    expect(jwt.jwtPassphrase).to.equal('pw');
    expect(jwt.jwtAlgorithm).to.equal('ES256');
    expect(jwt.tokenUrl).to.equal('https://login.microsoftonline.com/tenant/oauth2/v2.0/token');
    expect(jwt.oauthScopes).to.deep.equal(['2ff814a6-.../.default']);
  });

  it('omits optional fields when not supplied', () => {
    const native = buildKernelConnectionOptions(baseJwt as ConnectionOptions);
    expect(native).to.not.have.property('jwtPassphrase');
    expect(native).to.not.have.property('jwtAlgorithm');
    expect(native).to.not.have.property('tokenUrl');
  });

  it('takes precedence over the shared-secret M2M / U2M split', () => {
    // A private key present makes this JWT M2M regardless of anything else
    // (no secret ⇒ would otherwise be U2M).
    const native = buildKernelConnectionOptions(baseJwt as ConnectionOptions);
    expect(native.authMode).to.equal('OAuthM2mJwt');
  });

  it('rejects oauthJwtKeyFile together with oauthClientSecret (ambiguous)', () => {
    expect(() =>
      buildKernelConnectionOptions({
        ...baseJwt,
        oauthClientSecret: 'shh',
      } as ConnectionOptions),
    ).to.throw(HiveDriverError, /both `oauthJwtKeyFile`.*`oauthClientSecret`/);
  });

  it('requires oauthClientId', () => {
    const { oauthClientId, ...noClientId } = baseJwt;
    expect(() => buildKernelConnectionOptions(noClientId as ConnectionOptions)).to.throw(
      AuthenticationError,
      /requires `oauthClientId`/,
    );
  });

  it('requires oauthJwtKid', () => {
    const { oauthJwtKid, ...noKid } = baseJwt;
    expect(() => buildKernelConnectionOptions(noKid as ConnectionOptions)).to.throw(
      AuthenticationError,
      /requires `oauthJwtKid`/,
    );
  });

  it('rejects persistence on the JWT M2M path', () => {
    expect(() =>
      buildKernelConnectionOptions({
        ...baseJwt,
        persistence: {} as never,
      } as ConnectionOptions),
    ).to.throw(HiveDriverError, /persistence/);
  });

  it('prepends `/` to the path on the JWT branch too', () => {
    const native = buildKernelConnectionOptions({
      ...baseJwt,
      path: 'sql/1.0/warehouses/abc',
    } as ConnectionOptions);
    expect((native as { httpPath: string }).httpPath).to.equal('/sql/1.0/warehouses/abc');
  });
});
