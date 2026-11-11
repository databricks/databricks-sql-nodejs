import { expect, AssertionError } from 'chai';
import http, { IncomingHttpHeaders } from 'http';
import { AddressInfo, Socket } from 'net';
import thrift from 'thrift';
import DBSQLClient from '../../../../lib/DBSQLClient';
import TCLIService from '../../../../thrift/TCLIService';
import { TStatusCode, TProtocolVersion } from '../../../../thrift/TCLIService_types';

const warehousePath = '/sql/1.0/warehouses/abc';

// Minimal plain-HTTP Thrift endpoint standing in for a trusted local reverse proxy.
class LocalThriftServer {
  public readonly requests: Array<{ method?: string; url?: string; encrypted: boolean; headers: IncomingHttpHeaders }> =
    [];

  private readonly server: http.Server;

  // The driver's keep-alive agent holds sockets open; `server.close()` waits for them.
  private readonly sockets = new Set<Socket>();

  constructor() {
    const handle = { guid: Buffer.from('6ba7b8109dad11d180b400c04fd430c8', 'hex'), secret: Buffer.alloc(16, 2) };
    this.server = thrift.createWebServer({
      services: {
        [warehousePath]: {
          processor: TCLIService,
          // Single-argument handlers select the generated processor's promise style.
          handler: {
            OpenSession: async (_req: unknown) => ({
              status: { statusCode: TStatusCode.SUCCESS_STATUS },
              serverProtocolVersion: TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8,
              sessionHandle: { sessionId: handle },
            }),
            CloseSession: async (_req: unknown) => ({ status: { statusCode: TStatusCode.SUCCESS_STATUS } }),
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
