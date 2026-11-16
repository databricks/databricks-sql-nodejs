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
    server.closeAllConnections();
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  });

  beforeEach(() => {
    paths.length = 0;
  });

  // Resolves when the server side of the next connection closes; the responses below never end on
  // their own, so only the client destroying its socket closes it.
  function nextConnectionClosed(): Promise<void> {
    return new Promise((resolve) => {
      server.once('connection', (socket) => socket.once('close', () => resolve()));
    });
  }

  function agentSocketCount(agent: http.Agent): number {
    return [agent.sockets, agent.freeSockets].reduce(
      (count, pool) => count + Object.values(pool).reduce((n, sockets) => n + (sockets?.length ?? 0), 0),
      0,
    );
  }

  async function expectSocketReleased(closed: Promise<void>, agent: http.Agent): Promise<void> {
    await closed;
    await new Promise((resolve) => {
      setImmediate(resolve);
    });
    expect(agentSocketCount(agent)).to.equal(0);
  }

  it('reads the tenant from the first /aad/auth redirect without following it', async () => {
    respond = (req, res) => {
      res.writeHead(302, { Location: 'https://login.microsoftonline.com/tenant-xyz/oauth2/authorize?client_id=x' });
      res.end();
    };
    expect(await discoverAzureTenantId(`${base}/`)).to.equal('tenant-xyz');
    expect(paths).to.deep.equal(['/aad/auth']);
  });

  it('drains the redirect body so a keep-alive agent reuses the socket', async () => {
    respond = (req, res) => {
      res.writeHead(302, { Location: 'https://login.microsoftonline.com/tenant-xyz/oauth2/authorize' });
      // Larger than the stream buffers, so an undrained body would hold the only socket.
      res.end('x'.repeat(1024 * 1024));
    };
    let connections = 0;
    const onConnection = () => {
      connections += 1;
    };
    server.on('connection', onConnection);
    const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
    try {
      expect(await discoverAzureTenantId(base, agent)).to.equal('tenant-xyz');
      expect(await discoverAzureTenantId(base, agent)).to.equal('tenant-xyz');
      expect(connections).to.equal(1);
    } finally {
      server.off('connection', onConnection);
      agent.destroy();
    }
  });

  it('aborts a stalled redirect body and returns the tenant from the headers', async function () {
    // The body drain is bounded at 1 s; without the abort, discovery waits 30 s and keeps the socket.
    this.timeout(10000);
    respond = (req, res) => {
      res.writeHead(302, { Location: 'https://login.microsoftonline.com/tenant-xyz/oauth2/authorize' });
      res.write('x');
    };
    const closed = nextConnectionClosed();
    const agent = new http.Agent({ keepAlive: true });
    try {
      expect(await discoverAzureTenantId(base, agent)).to.equal('tenant-xyz');
      await expectSocketReleased(closed, agent);
    } finally {
      agent.destroy();
    }
  });

  it('returns the tenant and releases the socket when the redirect body fails mid-stream', async () => {
    respond = (req, res) => {
      res.writeHead(302, { Location: 'https://login.microsoftonline.com/tenant-xyz/oauth2/authorize' });
      res.write('x', () => res.socket?.destroy());
    };
    const closed = nextConnectionClosed();
    const agent = new http.Agent({ keepAlive: true });
    try {
      expect(await discoverAzureTenantId(base, agent)).to.equal('tenant-xyz');
      await expectSocketReleased(closed, agent);
    } finally {
      agent.destroy();
    }
  });

  it('aborts when the redirect body cannot be decoded', async () => {
    // node-fetch rejects the body read without closing the request, so the drain must abort it.
    respond = (req, res) => {
      res.writeHead(302, {
        Location: 'https://login.microsoftonline.com/tenant-xyz/oauth2/authorize',
        'Content-Encoding': 'gzip',
      });
      res.write('not gzip');
    };
    const closed = nextConnectionClosed();
    const agent = new http.Agent({ keepAlive: true });
    try {
      expect(await discoverAzureTenantId(base, agent)).to.equal('tenant-xyz');
      await expectSocketReleased(closed, agent);
    } finally {
      agent.destroy();
    }
  });

  it('aborts with the azureTenantId hint when the response headers never arrive', async () => {
    respond = () => {};
    const closed = nextConnectionClosed();
    const agent = new http.Agent({ keepAlive: true });
    try {
      await expectAuthError(
        discoverAzureTenantId(base, agent, { headers: 100, bodyDrain: 100 }),
        'failed (timed out); pass azureTenantId',
      );
      await expectSocketReleased(closed, agent);
    } finally {
      agent.destroy();
    }
  });

  it('fails when /aad/auth does not redirect', async () => {
    respond = (req, res) => {
      res.writeHead(200);
      res.end();
    };
    await expectAuthError(discoverAzureTenantId(base), 'HTTP 200 without a redirect');
  });

  it('fails when a non-redirect response carries a Location header', async () => {
    respond = (req, res) => {
      res.writeHead(200, { Location: 'https://login.microsoftonline.com/tenant-xyz/oauth2/authorize' });
      res.end();
    };
    await expectAuthError(discoverAzureTenantId(base), 'HTTP 200 without a redirect');
  });

  it('wraps a network failure with the azureTenantId hint', async () => {
    const closed = http.createServer();
    await new Promise<void>((resolve) => {
      closed.listen(0, '127.0.0.1', resolve);
    });
    const { port } = closed.address() as AddressInfo;
    await new Promise<void>((resolve) => {
      closed.close(() => resolve());
    });
    await expectAuthError(
      discoverAzureTenantId(`http://127.0.0.1:${port}`),
      'failed (ECONNREFUSED); pass azureTenantId',
    );
  });

  it('fails when the redirect is not to a Microsoft login host', async () => {
    respond = (req, res) => {
      res.writeHead(302, { Location: 'https://idp.example.com/tenant-xyz/authorize' });
      res.end();
    };
    await expectAuthError(discoverAzureTenantId(base), 'Microsoft login host');
  });
});
