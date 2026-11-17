import { execFileSync } from 'child_process';
import fs from 'fs';
import http from 'http';
import https from 'https';
import net, { AddressInfo } from 'net';
import os from 'os';
import path from 'path';
import { expect } from 'chai';
import { FetchError } from 'node-fetch';
import HttpConnection from '../../../../lib/connection/connections/HttpConnection';
import ThriftHttpConnection, { THTTPException } from '../../../../lib/connection/connections/ThriftHttpConnection';
import { ClientConfig } from '../../../../lib/contracts/IClientContext';
import RetryError, { RetryErrorCode } from '../../../../lib/errors/RetryError';
import ClientContextStub from '../../.stubs/ClientContextStub';

// Header-only Thrift binary messages: enough for method-name detection and for the fake `recv_*` below.
function thriftMessage(name: string, type: number, seqId: number): Buffer {
  const nameBytes = Buffer.from(name);
  const buffer = Buffer.alloc(12 + nameBytes.length);
  buffer.writeUInt32BE((0x80010000 | type) >>> 0, 0);
  buffer.writeInt32BE(nameBytes.length, 4);
  nameBytes.copy(buffer, 8);
  buffer.writeInt32BE(seqId, 8 + nameBytes.length);
  return buffer;
}

const CALL = 1;
const REPLY = 2;

type ServerAction =
  | 'reply'
  | 'close-after-request'
  | 'stall'
  | 'status-099'
  | 'status-408'
  | 'status-429'
  | 'status-501'
  | 'status-503'
  | 'status-503-retry-after';

const ERROR_DETAIL = 'server error detail';

interface TestServer {
  port: number;
  requests: number;
  close(): void;
}

async function startServer(actions: ServerAction[]): Promise<TestServer> {
  const state = { requests: 0 };
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      const action = actions[state.requests] ?? 'reply';
      state.requests += 1;
      if (action === 'close-after-request') {
        // The full request has been received; drop the connection without a response
        req.socket.destroy();
      } else if (action === 'stall') {
        // Never respond; the client's request timeout ends it
      } else if (action === 'status-099') {
        // ServerResponse rejects a status below 100, so write the status line itself
        req.socket.end('HTTP/1.1 099 X\r\nContent-Length: 0\r\n\r\n');
      } else if (action === 'status-503-retry-after') {
        res.writeHead(503, { 'Retry-After': '1', 'x-thriftserver-error-message': ERROR_DETAIL });
        res.end();
      } else if (action.startsWith('status-')) {
        res.writeHead(Number(action.slice('status-'.length)), { 'x-thriftserver-error-message': ERROR_DETAIL });
        res.end();
      } else {
        const body = Buffer.concat(chunks);
        const nameLength = body.readInt32BE(4);
        const name = body.toString('utf8', 8, 8 + nameLength);
        res.writeHead(200, { 'Content-Type': 'application/x-thrift' });
        res.end(thriftMessage(name, REPLY, body.readInt32BE(8 + nameLength)));
      }
    });
  });
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });
  return {
    port: (server.address() as AddressInfo).port,
    get requests() {
      return state.requests;
    },
    close() {
      server.closeAllConnections();
      server.close();
    },
  };
}

async function getClosedPort(): Promise<number> {
  const server = net.createServer();
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });
  const { port } = server.address() as AddressInfo;
  await new Promise<void>((resolve) => {
    server.close(() => resolve());
  });
  return port;
}

// Keep-alive agent whose first `refusals` connection attempts go to a closed port (ECONNREFUSED)
class RefusingAgent extends http.Agent {
  public connectAttempts = 0;

  constructor(private readonly refusals: number, private readonly closedPort: number) {
    super({ keepAlive: true });
  }

  createConnection(options: net.NetConnectOpts): net.Socket {
    this.connectAttempts += 1;
    if (this.connectAttempts <= this.refusals) {
      return net.createConnection({ host: '127.0.0.1', port: this.closedPort });
    }
    return net.createConnection(options);
  }
}

// Keep-alive agent whose first connection tries two closed addresses, as for a host with several addresses.
// Node fails it with an AggregateError that has a code but no message. The code is set to ETIMEDOUT, as when
// every address times out, which a test can't wait for.
class MultiAddressTimeoutAgent extends http.Agent {
  public connectAttempts = 0;

  constructor(private readonly closedPort: number) {
    super({ keepAlive: true });
  }

  createConnection(options: net.NetConnectOpts): net.Socket {
    this.connectAttempts += 1;
    if (this.connectAttempts > 1) {
      return net.createConnection(options);
    }
    const addresses = [
      { address: '127.0.0.1', family: 4 },
      { address: '::1', family: 6 },
    ];
    const connectOptions = {
      host: 'multi-address.test',
      port: this.closedPort,
      autoSelectFamily: true,
      lookup: (hostname: string, lookupOptions: unknown, callback: (error: null, results: typeof addresses) => void) =>
        callback(null, addresses),
    };
    const socket = net.createConnection(connectOptions as unknown as net.NetConnectOpts);
    socket.prependListener('error', (error: NodeJS.ErrnoException) => {
      error.code = 'ETIMEDOUT';
    });
    return socket;
  }
}

async function createThriftConnection(
  port: number,
  agent: http.Agent,
  config: Partial<ClientConfig>,
): Promise<ThriftHttpConnection> {
  const context = new ClientContextStub({ retryDelayMin: 1, retryDelayMax: 2, retryMaxAttempts: 3, ...config });
  // A short socketTimeout, so a stalled response becomes a request timeout quickly
  const httpConnection = new HttpConnection({ host: '127.0.0.1', port, path: '/', socketTimeout: 300 }, context);
  httpConnection['httpAgent'] = agent;
  return httpConnection.getThriftConnection();
}

function send(connection: ThriftHttpConnection, method: string, seqId: number): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const reqs: Record<number, (error: unknown, result?: unknown) => void> = {
      [seqId]: (error, result) => (error ? reject(error) : resolve(result)),
    };
    connection.client = {
      _reqs: reqs,
      [`recv_${method}`](this: { _reqs: typeof reqs }, input: unknown, mtype: unknown, rseqid: number) {
        const callback = this._reqs[rseqid];
        delete this._reqs[rseqid];
        callback(null, `${method} result`);
      },
    } as any;
    connection.write(thriftMessage(method, CALL, seqId), seqId);
  });
}

async function expectRejection(promise: Promise<unknown>): Promise<any> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  return expect.fail('It should throw an error');
}

describe('ThriftHttpConnection retries', () => {
  const cleanup: Array<() => void> = [];

  afterEach(() => {
    cleanup.splice(0).forEach((fn) => fn());
  });

  async function setup(actions: ServerAction[], refusals: number, config: Partial<ClientConfig> = {}) {
    const closedPort = await getClosedPort();
    const server = await startServer(actions);
    const agent = new RefusingAgent(refusals, closedPort);
    cleanup.push(
      () => server.close(),
      () => agent.destroy(),
    );
    const connection = await createThriftConnection(server.port, agent, config);
    return { server, agent, connection };
  }

  it('retries ExecuteStatement when the connection is refused before the request is sent', async () => {
    const { server, agent, connection } = await setup(['reply'], 1);

    const result = await send(connection, 'ExecuteStatement', 1);

    expect(result).to.equal('ExecuteStatement result');
    expect(agent.connectAttempts).to.equal(2);
    expect(server.requests).to.equal(1);
  });

  it('retries ExecuteStatement when connecting to every address of the host times out', async () => {
    const server = await startServer(['reply']);
    const agent = new MultiAddressTimeoutAgent(await getClosedPort());
    cleanup.push(
      () => server.close(),
      () => agent.destroy(),
    );
    const connection = await createThriftConnection(server.port, agent, {});

    const result = await send(connection, 'ExecuteStatement', 1);

    expect(result).to.equal('ExecuteStatement result');
    expect(agent.connectAttempts).to.equal(2);
    expect(server.requests).to.equal(1);
  });

  it('surfaces the original error once retries for a refused ExecuteStatement are exhausted', async () => {
    const { server, agent, connection } = await setup([], Infinity);

    const error = await expectRejection(send(connection, 'ExecuteStatement', 1));

    expect(error).to.be.instanceOf(FetchError);
    expect(error.code).to.equal('ECONNREFUSED');
    expect(agent.connectAttempts).to.equal(3);
    expect(server.requests).to.equal(0);
  });

  it('does not retry ExecuteStatement when the connection closes after the request was sent', async () => {
    const { server, connection } = await setup(['reply', 'close-after-request'], 0);

    await send(connection, 'ExecuteStatement', 1);
    const error = await expectRejection(send(connection, 'ExecuteStatement', 2));

    expect(error).to.be.instanceOf(FetchError);
    expect(error.code).to.equal('ECONNRESET');
    expect(server.requests).to.equal(2);
  });

  for (const status of ['status-503', 'status-408'] as const) {
    it(`does not retry ExecuteStatement on an HTTP error status (${status})`, async () => {
      const { server, connection } = await setup([status], 0);

      const error = await expectRejection(send(connection, 'ExecuteStatement', 1));

      expect(error).to.be.instanceOf(THTTPException);
      expect(error.statusCode).to.equal(Number(status.slice('status-'.length)));
      expect(server.requests).to.equal(1);
    });
  }

  for (const status of ['status-503-retry-after', 'status-429'] as const) {
    it(`retries ExecuteStatement when the server rejects it without running it (${status})`, async () => {
      const { server, connection } = await setup([status, 'reply'], 0);

      const result = await send(connection, 'ExecuteStatement', 1);

      expect(result).to.equal('ExecuteStatement result');
      expect(server.requests).to.equal(2);
    });
  }

  for (const status of ['status-503', 'status-429', 'status-408'] as const) {
    it(`retries OpenSession on a transient HTTP error (${status})`, async () => {
      const { server, connection } = await setup([status, 'reply'], 0);

      const result = await send(connection, 'OpenSession', 1);

      expect(result).to.equal('OpenSession result');
      expect(server.requests).to.equal(2);
    });
  }

  it('retries OpenSession after a request timeout, as the kernel', async () => {
    const { server, connection } = await setup(['stall', 'reply'], 0);

    const result = await send(connection, 'OpenSession', 1);

    expect(result).to.equal('OpenSession result');
    expect(server.requests).to.equal(2);
  });

  it('fails an OpenSession that times out on every attempt with "Request timed out"', async () => {
    const { server, connection } = await setup(['stall', 'stall', 'stall'], 0);

    const error = await expectRejection(send(connection, 'OpenSession', 1));

    expect(error.message).to.equal('Request timed out');
    expect(server.requests).to.equal(3);
  });

  it('fails with "Request timed out" when the first timeout uses up retriesTimeout, as with the defaults', async () => {
    const { server, connection } = await setup(['stall', 'reply'], 0, { retriesTimeout: 300 });

    const error = await expectRejection(send(connection, 'OpenSession', 1));

    expect(error.message).to.equal('Request timed out');
    expect(server.requests).to.equal(1);
  });

  it('does not retry ExecuteStatement after a request timeout', async () => {
    const { server, connection } = await setup(['stall', 'reply'], 0);

    const error = await expectRejection(send(connection, 'ExecuteStatement', 1));

    expect(error.message).to.equal('Request timed out');
    expect(server.requests).to.equal(1);
  });

  it('retries OpenSession when the connection is refused', async () => {
    const { server, agent, connection } = await setup(['reply'], 1);

    const result = await send(connection, 'OpenSession', 1);

    expect(result).to.equal('OpenSession result');
    expect(agent.connectAttempts).to.equal(2);
    expect(server.requests).to.equal(1);
  });

  it('does not retry other non-idempotent operations when the connection is refused', async () => {
    const { server, agent, connection } = await setup(['reply'], 1);

    const error = await expectRejection(send(connection, 'FetchResults', 1));

    expect(error).to.be.instanceOf(FetchError);
    expect(error.code).to.equal('ECONNREFUSED');
    expect(agent.connectAttempts).to.equal(1);
    expect(server.requests).to.equal(0);
  });

  it('still retries idempotent operations when the connection closes after the request was sent', async () => {
    const { server, connection } = await setup(['close-after-request', 'reply'], 0);

    const result = await send(connection, 'GetOperationStatus', 1);

    expect(result).to.equal('GetOperationStatus result');
    expect(server.requests).to.equal(2);
  });

  describe('once retries run out', () => {
    function expectHttpError(error: any, status: number) {
      expect(error).to.be.instanceOf(THTTPException);
      expect(error.statusCode).to.equal(status);
      expect(error.message).to.equal(`Received a response with a bad HTTP status code: ${status}: ${ERROR_DETAIL}`);
    }

    // Requests that weren't retried before fail as they did then
    for (const [method, action, status] of [
      ['OpenSession', 'status-503', 503],
      ['OpenSession', 'status-429', 429],
      ['OpenSession', 'status-501', 501],
      ['GetOperationStatus', 'status-408', 408],
      ['GetOperationStatus', 'status-501', 501],
      ['ExecuteStatement', 'status-429', 429],
      ['ExecuteStatement', 'status-503-retry-after', 503],
    ] as const) {
      it(`fails ${method} with the last HTTP error (${action})`, async () => {
        const { server, connection } = await setup([action, action, action], 0);

        const error = await expectRejection(send(connection, method, 1));

        expectHttpError(error, status);
        expect(server.requests).to.equal(3);
      });
    }

    it('fails OpenSession with the connection error', async () => {
      const { server, agent, connection } = await setup([], Infinity);

      const error = await expectRejection(send(connection, 'OpenSession', 1));

      expect(error).to.be.instanceOf(FetchError);
      expect(error.code).to.equal('ECONNREFUSED');
      expect(agent.connectAttempts).to.equal(3);
      expect(server.requests).to.equal(0);
    });

    it('fails OpenSession with the network error when the connection closes after the request', async () => {
      const action = 'close-after-request';
      const { server, connection } = await setup([action, action, action], 0);

      const error = await expectRejection(send(connection, 'OpenSession', 1));

      expect(error).to.be.instanceOf(FetchError);
      expect(error.code).to.equal('ECONNRESET');
      expect(server.requests).to.equal(3);
    });

    // Retried before, so they keep failing with the RetryError
    for (const [action, status] of [
      ['status-503', 503],
      ['status-099', 99],
    ] as const) {
      it(`keeps the RetryError for an idempotent RPC on a ${status}`, async () => {
        const { server, connection } = await setup([action, action, action], 0);

        const error = await expectRejection(send(connection, 'GetOperationStatus', 1));

        expect(error).to.be.instanceOf(RetryError);
        expect(error.errorCode).to.equal(RetryErrorCode.AttemptsExceeded);
        expect(error.payload.response.status).to.equal(status);
        expect(server.requests).to.equal(3);
      });
    }

    it('keeps the RetryError for an idempotent RPC whose connection is refused', async () => {
      const { agent, connection } = await setup([], Infinity);

      const error = await expectRejection(send(connection, 'GetOperationStatus', 1));

      expect(error).to.be.instanceOf(RetryError);
      expect(error.errorCode).to.equal(RetryErrorCode.AttemptsExceeded);
      expect(error.payload.code).to.equal('ECONNREFUSED');
      expect(agent.connectAttempts).to.equal(3);
    });
  });
});

describe('ThriftHttpConnection with a certificate that fails verification', () => {
  // A trusted self-signed `localhost` certificate with an unknown critical extension, generated per run so no
  // private key is committed. OpenSSL rejects it as "unhandled critical extension", which Node reports as
  // `UNSPECIFIED` because it has no name for that X509 error.
  let cert = '';
  let key = '';

  before(function () {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbsql-cert-'));
    try {
      execFileSync(
        'openssl',
        [
          'req',
          '-x509',
          '-newkey',
          'ec',
          '-pkeyopt',
          'ec_paramgen_curve:prime256v1',
          '-nodes',
          '-days',
          '1',
          '-subj',
          '/CN=localhost',
          '-addext',
          'subjectAltName=DNS:localhost,IP:127.0.0.1',
          '-addext',
          '1.3.6.1.4.1.55555.1=critical,ASN1:UTF8String:x',
          '-keyout',
          path.join(dir, 'key.pem'),
          '-out',
          path.join(dir, 'cert.pem'),
        ],
        { stdio: 'ignore' },
      );
      cert = fs.readFileSync(path.join(dir, 'cert.pem'), 'utf8');
      key = fs.readFileSync(path.join(dir, 'key.pem'), 'utf8');
    } catch {
      this.skip(); // openssl is not available
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('fails OpenSession at once with the certificate error Node reports as UNSPECIFIED', async () => {
    let connections = 0;
    const server = https.createServer({ cert, key }, (req, res) => res.end());
    server.on('connection', () => {
      connections += 1;
    });
    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', resolve);
    });
    const context = new ClientContextStub({ retryDelayMin: 1, retryDelayMax: 2, retryMaxAttempts: 3 });
    const { port } = server.address() as AddressInfo;
    const httpConnection = new HttpConnection({ host: 'localhost', port, path: '/', https: true, ca: cert }, context);
    try {
      const connection = await httpConnection.getThriftConnection();

      const error = await expectRejection(send(connection, 'OpenSession', 1));

      expect(error).to.be.instanceOf(FetchError);
      expect(error.code).to.equal('UNSPECIFIED');
      expect(error.message).to.contain('unhandled critical extension');
      expect(connections).to.equal(1);
    } finally {
      (await httpConnection.getAgent())?.destroy();
      server.closeAllConnections();
      server.close();
    }
  });
});
