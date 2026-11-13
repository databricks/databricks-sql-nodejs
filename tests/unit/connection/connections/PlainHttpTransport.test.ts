import { expect, AssertionError } from 'chai';
import http, { IncomingHttpHeaders } from 'http';
import { AddressInfo, Socket } from 'net';
import thrift from 'thrift';
import DBSQLClient from '../../../../lib/DBSQLClient';
import CloudFetchResultHandler from '../../../../lib/result/CloudFetchResultHandler';
import { DatabricksOAuthManager, OAuthFlow } from '../../../../lib/connection/auth/DatabricksOAuth/OAuthManager';
import ResultsProviderStub from '../../.stubs/ResultsProviderStub';
import TCLIService from '../../../../thrift/TCLIService';
import { TStatusCode, TProtocolVersion } from '../../../../thrift/TCLIService_types';

const warehousePath = '/sql/1.0/warehouses/abc';

// Minimal plain-HTTP Thrift endpoint standing in for a trusted local reverse proxy.
class LocalThriftServer {
  public readonly requests: Array<{ method?: string; url?: string; encrypted: boolean; headers: IncomingHttpHeaders }> =
    [];

  public connectionCount = 0;

  private readonly server: http.Server;

  // The driver's keep-alive agent holds sockets open; `server.close()` waits for them.
  private readonly sockets = new Set<Socket>();

  constructor() {
    const handle = { guid: Buffer.from('6ba7b8109dad11d180b400c04fd430c8', 'hex'), secret: Buffer.alloc(16, 2) };
    this.server = thrift.createWebServer({
      services: {
        [warehousePath]: {
          processor: TCLIService,
          // Callback-style handlers: the generated processor's promise style
          // (single-argument handlers) needs `Q`, which `thrift` no longer provides.
          handler: {
            OpenSession: (_req: unknown, result: (error: unknown, response: unknown) => void) =>
              result(null, {
                status: { statusCode: TStatusCode.SUCCESS_STATUS },
                serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
                sessionHandle: { sessionId: handle },
              }),
            CloseSession: (_req: unknown, result: (error: unknown, response: unknown) => void) =>
              result(null, { status: { statusCode: TStatusCode.SUCCESS_STATUS } }),
          },
        },
      },
    } as any) as unknown as http.Server;
    this.server.prependListener('request', (req: http.IncomingMessage) => {
      this.requests.push({
        method: req.method,
        url: req.url,
        encrypted: Boolean((req.socket as any).encrypted),
        headers: req.headers,
      });
    });
    this.server.on('connection', (socket: Socket) => {
      this.connectionCount += 1;
      this.sockets.add(socket);
      socket.on('close', () => this.sockets.delete(socket));
    });
  }

  public async start(): Promise<number> {
    await new Promise<void>((resolve) => {
      this.server.listen(0, '127.0.0.1', resolve);
    });
    return (this.server.address() as AddressInfo).port;
  }

  public async stop(): Promise<void> {
    const closed = new Promise<void>((resolve) => {
      this.server.close(() => resolve());
    });
    this.sockets.forEach((socket) => socket.destroy());
    await closed;
  }
}

describe('DBSQLClient plain HTTP transport', () => {
  let server: LocalThriftServer;
  let port: number;

  beforeEach(async () => {
    server = new LocalThriftServer();
    port = await server.start();
  });

  afterEach(async () => {
    await server.stop();
  });

  const baseOptions = () => ({
    host: '127.0.0.1',
    port,
    path: warehousePath,
    telemetryEnabled: false,
    retryMaxAttempts: 1,
    authType: 'custom' as const,
    // The reverse proxy injects credentials, so the driver sends none.
    provider: { authenticate: async () => ({}) },
  });

  it('opens and closes a session over plain HTTP with https: false', async () => {
    const client = new DBSQLClient();
    await client.connect({ ...baseOptions(), https: false });

    const session = await client.openSession();
    await session.close();
    await client.close();

    expect(server.requests).to.have.length(2);
    for (const request of server.requests) {
      expect(request.method).to.equal('POST');
      expect(request.url).to.equal(warehousePath);
      expect(request.encrypted).to.be.false;
      expect(request.headers.authorization).to.be.undefined;
    }
  });

  // CloudFetch links and OAuth endpoints stay HTTPS with `https: false`. A
  // plain-HTTP agent would make Node throw ERR_INVALID_PROTOCOL before
  // connecting; the right agent opens a socket and attempts a TLS handshake,
  // which this plain-HTTP server rejects without parsing an HTTP request.
  async function expectTlsAttempt(request: () => Promise<unknown>) {
    const connectionsBefore = server.connectionCount;
    try {
      await request();
      expect.fail('a TLS handshake against a plain-HTTP server should fail');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(String((error as any)?.stack ?? error)).to.not.include('ERR_INVALID_PROTOCOL');
      expect((error as any)?.code).to.not.equal('ERR_INVALID_PROTOCOL');
    }
    expect(server.connectionCount).to.be.greaterThan(connectionsBefore);
  }

  it('runs SQL over HTTP and CloudFetch downloads over HTTPS with https: false', async () => {
    const client = new DBSQLClient();
    await client.connect({ ...baseOptions(), https: false });

    const session = await client.openSession();
    await session.close();
    expect(server.requests).to.have.length(2);

    const handler = new CloudFetchResultHandler(client, new ResultsProviderStub([], undefined), {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
    });
    await expectTlsAttempt(() => handler['fetch'](`https://127.0.0.1:${port}/chunk?X-Amz-Signature=secret`));

    await client.close();
    // The HTTPS attempt never reached the server as an HTTP request.
    expect(server.requests).to.have.length(2);
  });

  it('runs SQL over HTTP and OAuth requests over HTTPS with https: false', async () => {
    const client = new DBSQLClient();
    await client.connect({ ...baseOptions(), https: false });

    const session = await client.openSession();
    await session.close();
    expect(server.requests).to.have.length(2);

    const oauthManager = new DatabricksOAuthManager({
      host: `127.0.0.1:${port}`,
      flow: OAuthFlow.M2M,
      clientId: 'id',
      clientSecret: 'secret',
      context: client,
    });
    // OIDC discovery goes to https://127.0.0.1:<port>/oidc/... .
    await expectTlsAttempt(() => oauthManager['getClient']());

    await client.close();
    expect(server.requests).to.have.length(2);
  });

  it('keeps using HTTPS when https is omitted', async () => {
    const client = new DBSQLClient();
    await client.connect(baseOptions());

    try {
      await client.openSession();
      expect.fail('a TLS handshake against a plain-HTTP server should fail');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
    } finally {
      await client.close();
    }

    // The TLS ClientHello never parses as an HTTP request.
    expect(server.requests).to.have.length(0);
  });
});
