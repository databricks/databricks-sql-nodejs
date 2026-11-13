import http from 'http';
import net, { AddressInfo } from 'net';
import { expect } from 'chai';
import { FetchError } from 'node-fetch';
import HttpConnection from '../../../../lib/connection/connections/HttpConnection';
import ThriftHttpConnection, { THTTPException } from '../../../../lib/connection/connections/ThriftHttpConnection';
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

type ServerAction = 'reply' | 'close-after-request' | 'status-503';

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
      } else if (action === 'status-503') {
        res.writeHead(503);
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

async function createThriftConnection(port: number, agent: http.Agent): Promise<ThriftHttpConnection> {
  const context = new ClientContextStub({ retryDelayMin: 1, retryDelayMax: 2, retryMaxAttempts: 3 });
  const httpConnection = new HttpConnection({ host: '127.0.0.1', port, path: '/' }, context);
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

  async function setup(actions: ServerAction[], refusals: number) {
    const closedPort = await getClosedPort();
    const server = await startServer(actions);
    const agent = new RefusingAgent(refusals, closedPort);
    cleanup.push(
      () => server.close(),
      () => agent.destroy(),
    );
    const connection = await createThriftConnection(server.port, agent);
    return { server, agent, connection };
  }

  it('retries ExecuteStatement when the connection is refused before the request is sent', async () => {
    const { server, agent, connection } = await setup(['reply'], 1);

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

  it('does not retry ExecuteStatement on an HTTP error status', async () => {
    const { server, connection } = await setup(['status-503'], 0);

    const error = await expectRejection(send(connection, 'ExecuteStatement', 1));

    expect(error).to.be.instanceOf(THTTPException);
    expect(error.statusCode).to.equal(503);
    expect(server.requests).to.equal(1);
  });

  it('does not retry other non-idempotent operations when the connection is refused', async () => {
    const { server, agent, connection } = await setup(['reply'], 1);

    const error = await expectRejection(send(connection, 'OpenSession', 1));

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
});
