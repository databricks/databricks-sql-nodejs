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
import { DBSQLClient } from '../../../lib';

/**
 * A throwaway self-signed CA, embedded so the suite is self-contained
 * (no external file dependency in CI). It is added to the trust store as
 * an *additional* root in the customCaCert tests; the real workspace cert
 * still validates via the system roots, so this cert is never actually in
 * the validation path — its expiry is irrelevant.
 */
const THROWAWAY_CA_PEM = `-----BEGIN CERTIFICATE-----
MIIDGTCCAgGgAwIBAgIUdW39pdaepg+Mfb++pCKm9FxYlkswDQYJKoZIhvcNAQEL
BQAwHDEaMBgGA1UEAwwRdGhyb3dhd2F5LXRlc3QtY2EwHhcNMjYwNTMxMjEzNDI1
WhcNMjYwNjAxMjEzNDI1WjAcMRowGAYDVQQDDBF0aHJvd2F3YXktdGVzdC1jYTCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALWHhz4wQsBNqHJJzqJgT0BL
DO9oqZT2zsAnHOWPchsLoAjeBJ4oXgNjh2ZeH1FH4kThGw/giL5VDpO+lVmMu5TG
jeXXAmPIXk27qi1mGqu/tqstzuSqyxxk3oDnJuxCFxBYyA9LG+rhjH9WsW1XvzGn
gOQLZ0Hjx4FkA+aWvH8AV82OUgHRRHfe1GaU4MUsLMYGU+2bwngcL059pBP/h/BS
Q5brMJnFi8UrVTizuCF/QYP5dtEyvDltMKKU/E8uft/DP/2Q25r0hMZNU+I22v0N
Ya8gCzA47LMPeIaZ3tGT94OEyUaU5mpwsVwL4Y6bfQRtEIph6qwJnAjWwJxQsssC
AwEAAaNTMFEwHQYDVR0OBBYEFCDYsRm4L4QEbzu3Vkm+ji82xZcNMB8GA1UdIwQY
MBaAFCDYsRm4L4QEbzu3Vkm+ji82xZcNMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZI
hvcNAQELBQADggEBAJBXM668YVQ+PcrolpH0MisuUkqyjcrMLLDsjqU4PmBv75BZ
m08hffg1qVRjYnqvFYbYaStuvousuIT5h7cS5y0I5rMEzeRi2+MaOB3gaDBofObH
RYpCsehOjiNoNxoTzIIwZh+aNf/LK/Ti+DcW7dgxAlrctPbzZGQt/2wQLK/OLn6X
0FyAJDMQ7l1MOL5wZ4JTd+EImlbwMIj73sug9elXfYGNu2UzqZ82iQFUxaBEuckJ
O+tldQDDMuvU2m5bFypPXu/3Fe25FDFQggdOE1i+sp9WkoW5zh8GKo1y53yjbNAk
7bq2fNLgT9r2D54uqpuJwCMshIUTi5p60GNNPSo=
-----END CERTIFICATE-----
`;

/**
 * SEA TLS end-to-end against a live workspace. Exercises the
 * `checkServerCertificate` toggle and `customCaCert` passthrough end to
 * end (DBSQLClient → SeaBackend → SeaAuth → napi → kernel rustls →
 * real Databricks TLS handshake).
 *
 * Required env (exported by `~/.zshrc`):
 *   - DATABRICKS_PECOTESTING_SERVER_HOSTNAME
 *   - DATABRICKS_PECOTESTING_HTTP_PATH
 *   - DATABRICKS_PECOTESTING_TOKEN_PERSONAL (or _TOKEN)
 *
 * Skipped when secrets are absent.
 */
describe('sea-tls e2e — checkServerCertificate + customCaCert through a live TLS handshake', function suite() {
  const host = process.env.DATABRICKS_PECOTESTING_SERVER_HOSTNAME;
  const path = process.env.DATABRICKS_PECOTESTING_HTTP_PATH;
  const token = process.env.DATABRICKS_PECOTESTING_TOKEN_PERSONAL || process.env.DATABRICKS_PECOTESTING_TOKEN;

  this.timeout(120_000);

  before(function gate() {
    if (!host || !path || !token) {
      // eslint-disable-next-line no-invalid-this
      this.skip();
    }
  });

  async function connectAndSelectOne(extra: Record<string, unknown>): Promise<Array<Record<string, unknown>>> {
    const client = new DBSQLClient();
    await client.connect({
      host: host as string,
      path: path as string,
      token: token as string,
      useSEA: true,
      ...extra,
    });
    const session = await client.openSession();
    const operation = await session.executeStatement('SELECT 1 AS one', {});
    const rows = (await operation.fetchAll()) as Array<Record<string, unknown>>;
    await operation.close();
    await session.close();
    await client.close();
    return rows;
  }

  it('default (permissive, no flag): connects and runs SELECT 1', async () => {
    const rows = await connectAndSelectOne({});
    expect(rows).to.deep.equal([{ one: 1 }]);
  });

  it('strict (checkServerCertificate: true): real workspace cert validates against system roots', async () => {
    // The important positive case — turning verification ON must NOT
    // break against a real, publicly-signed Databricks cert.
    const rows = await connectAndSelectOne({ checkServerCertificate: true });
    expect(rows).to.deep.equal([{ one: 1 }]);
  });

  it('strict + additive customCaCert (PEM string): extra root does not break system-root validation', async () => {
    // A throwaway self-signed CA is added on top of the system roots.
    // The real workspace cert still validates via the system roots, so
    // the connection succeeds — proving custom_ca_cert is additive.
    const rows = await connectAndSelectOne({
      checkServerCertificate: true,
      customCaCert: THROWAWAY_CA_PEM,
    });
    expect(rows).to.deep.equal([{ one: 1 }]);
  });

  it('strict + additive customCaCert (Buffer): same, passed as bytes', async () => {
    const rows = await connectAndSelectOne({
      checkServerCertificate: true,
      customCaCert: Buffer.from(THROWAWAY_CA_PEM, 'utf8'),
    });
    expect(rows).to.deep.equal([{ one: 1 }]);
  });

  it('malformed customCaCert (PEM header, garbage body): kernel rejects at connect/openSession', async () => {
    const garbage = '-----BEGIN CERTIFICATE-----\nnot-valid-base64-cert-bytes\n-----END CERTIFICATE-----\n';
    const client = new DBSQLClient();
    let threw = false;
    try {
      await client.connect({
        host: host as string,
        path: path as string,
        token: token as string,
        useSEA: true,
        checkServerCertificate: true,
        customCaCert: garbage,
      });
      await client.openSession();
    } catch (err) {
      threw = true;
      expect(String((err as Error).message).length).to.be.greaterThan(0);
    } finally {
      await client.close().catch(() => undefined);
    }
    expect(threw, 'expected a malformed custom CA to be rejected').to.equal(true);
  });
});
