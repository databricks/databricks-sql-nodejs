import { expect, AssertionError } from 'chai';
import http from 'http';
import { AddressInfo } from 'net';
import {
  discoverAzureTenantId,
  tenantFromLoginUrl,
} from '../../../../../lib/connection/auth/DatabricksOAuth/AzureTenantDiscovery';
import AuthenticationError from '../../../../../lib/errors/AuthenticationError';

async function expectAuthError(promise: Promise<unknown>, contains: string): Promise<void> {
  try {
    await promise;
    expect.fail('It should throw an error');
  } catch (error) {
    if (error instanceof AssertionError) throw error;
    expect(error).to.be.instanceOf(AuthenticationError);
    expect((error as Error).message).to.contain(contains);
  }
}

describe('tenantFromLoginUrl', () => {
  it('reads the tenant from the first path segment of the Entra login URL', () => {
    expect(tenantFromLoginUrl('https://login.microsoftonline.com/tenant-123/oauth2/authorize?client_id=x')).to.equal(
      'tenant-123',
    );
  });

  it('accepts the sovereign Microsoft login hosts', () => {
    for (const host of ['login.microsoftonline.us', 'login.chinacloudapi.cn']) {
      expect(tenantFromLoginUrl(`https://${host}/tenant-abc/oauth2/authorize`)).to.equal('tenant-abc');
    }
  });

  it('rejects a redirect to a non-Microsoft or look-alike host', () => {
    for (const location of [
      'https://evil.example.com/tenant-abc/oauth2/authorize',
      'https://login.microsoftonline.com.attacker.example/tenant-abc/oauth2/authorize',
    ]) {
      expect(() => tenantFromLoginUrl(location)).to.throw(AuthenticationError, 'Microsoft login host');
    }
  });

  it('rejects a URL without a tenant segment or an invalid URL', () => {
    expect(() => tenantFromLoginUrl('https://login.microsoftonline.com/')).to.throw(AuthenticationError, 'tenant id');
    expect(() => tenantFromLoginUrl('not a url')).to.throw(AuthenticationError, 'invalid URL');
  });
});

describe('discoverAzureTenantId', () => {
  let server: http.Server;
  let base: string;
  let respond: (req: http.IncomingMessage, res: http.ServerResponse) => void;
  const paths: Array<string | undefined> = [];

  before(async () => {
    server = http.createServer((req, res) => {
      paths.push(req.url);
      respond(req, res);
    });
    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', resolve);
    });
    base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  after(async () => {
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  });

  beforeEach(() => {
    paths.length = 0;
  });

  it('reads the tenant from the first /aad/auth redirect without following it', async () => {
    respond = (req, res) => {
      res.writeHead(302, { Location: 'https://login.microsoftonline.com/tenant-xyz/oauth2/authorize?client_id=x' });
      res.end();
    };
    expect(await discoverAzureTenantId(`${base}/`)).to.equal('tenant-xyz');
    expect(paths).to.deep.equal(['/aad/auth']);
  });

  it('fails when /aad/auth does not redirect', async () => {
    respond = (req, res) => {
      res.writeHead(200);
      res.end();
    };
    await expectAuthError(discoverAzureTenantId(base), 'HTTP 200 without a redirect');
  });

  it('fails when the redirect is not to a Microsoft login host', async () => {
    respond = (req, res) => {
      res.writeHead(302, { Location: 'https://idp.example.com/tenant-xyz/authorize' });
      res.end();
    };
    await expectAuthError(discoverAzureTenantId(base), 'Microsoft login host');
  });
});
