import { expect } from 'chai';
import sinon from 'sinon';
import http from 'http';
import https from 'https';
import net, { AddressInfo } from 'net';
import DBSQLClient from '../../../../../lib/DBSQLClient';
import { ConnectionOptions } from '../../../../../lib/contracts/IDBSQLClient';
import ConnectionProviderStub from '../../../.stubs/ConnectionProviderStub';
import { createExpiredAccessToken, createValidAccessToken } from '../../../.stubs/OAuth';

const HOST = 'example.cloud.databricks.com';

// Routes every `https://` request to the local plain-HTTP mock IdP below.
class LoopbackAgent extends https.Agent {
  private readonly port: number;

  constructor(port: number) {
    super({ keepAlive: false });
    this.port = port;
  }

  createConnection(): net.Socket {
    return net.connect(this.port, '127.0.0.1');
  }
}

describe('Databricks OAuth token request scope (AUTH-017)', () => {
  let server: http.Server;
  let agent: LoopbackAgent;
  let tokenRequests: Array<URLSearchParams>;
  let issuedTokens: Array<string>;

  before(async () => {
    server = http.createServer((req, res) => {
      let body = '';
      req.on('data', (chunk) => {
        body += chunk;
      });
      req.on('end', () => {
        const send = (payload: object) => {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify(payload));
        };
        if (req.method === 'GET' && req.url === '/oidc/.well-known/oauth-authorization-server') {
          send({
            issuer: `https://${HOST}/oidc`,
            authorization_endpoint: `https://${HOST}/oidc/v1/authorize`,
            token_endpoint: `https://${HOST}/oidc/v1/token`,
          });
        } else if (req.method === 'POST' && req.url === '/oidc/v1/token') {
          tokenRequests.push(new URLSearchParams(body));
          send({ access_token: issuedTokens.shift() ?? createValidAccessToken(), token_type: 'Bearer' });
        } else {
          res.statusCode = 404;
          res.end();
        }
      });
    });
    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', resolve);
    });
    agent = new LoopbackAgent((server.address() as AddressInfo).port);
  });

  after(async () => {
    agent.destroy();
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  });

  beforeEach(() => {
    tokenRequests = [];
    issuedTokens = [];
  });

  afterEach(() => {
    sinon.restore();
  });

  async function authenticateM2M(extra: Partial<{ oauthScopes: Array<string> }> = {}) {
    const client = new DBSQLClient();
    const connectionProvider = new ConnectionProviderStub();
    sinon.stub(connectionProvider, 'getAgent').resolves(agent);
    sinon.stub(client, 'getConnectionProvider').resolves(connectionProvider);

    const options: ConnectionOptions = {
      host: HOST,
      path: '/sql/1.0/warehouses/abc',
      authType: 'databricks-oauth',
      oauthClientId: 'client-id',
      oauthClientSecret: 'client-secret',
      ...extra,
    };
    const provider = client['createAuthProvider'](options);
    return provider.authenticate();
  }

  function scopeOf(request: URLSearchParams): Array<string> | null {
    expect(request.get('grant_type')).to.equal('client_credentials');
    const scope = request.get('scope');
    return scope === null ? null : scope.split(' ');
  }

  it('sends the configured multi-scope value as that exact scope set', async () => {
    await authenticateM2M({ oauthScopes: [' sql   offline_access '] });
    expect(tokenRequests).to.have.length(1);
    expect(tokenRequests[0].get('scope')).to.equal('sql offline_access');
  });

  it('sends scopes configured as separate entries', async () => {
    await authenticateM2M({ oauthScopes: ['sql', 'offline_access', 'sql'] });
    expect(tokenRequests).to.have.length(1);
    expect(scopeOf(tokenRequests[0])).to.deep.equal(['sql', 'offline_access']);
  });

  it('defaults to all-apis when no scopes are configured', async () => {
    await authenticateM2M();
    expect(tokenRequests).to.have.length(1);
    expect(scopeOf(tokenRequests[0])).to.deep.equal(['all-apis']);
  });

  it('defaults to all-apis when the configured scopes are blank', async () => {
    for (const oauthScopes of [[], ['   '], ['', ' ']]) {
      tokenRequests = [];
      // eslint-disable-next-line no-await-in-loop
      await authenticateM2M({ oauthScopes });
      expect(tokenRequests, JSON.stringify(oauthScopes)).to.have.length(1);
      expect(scopeOf(tokenRequests[0]), JSON.stringify(oauthScopes)).to.deep.equal(['all-apis']);
    }
  });

  it('re-sends the configured scopes when re-acquiring an expired token', async () => {
    issuedTokens = [createExpiredAccessToken()];
    await authenticateM2M({ oauthScopes: ['sql'] });
    expect(tokenRequests).to.have.length(2);
    expect(scopeOf(tokenRequests[0])).to.deep.equal(['sql']);
    expect(scopeOf(tokenRequests[1])).to.deep.equal(['sql']);
  });
});
