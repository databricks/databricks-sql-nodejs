import http from 'http';
import { AddressInfo } from 'net';
import { PassThrough, Readable } from 'stream';
import { expect } from 'chai';
import thrift, { TBinaryProtocol, TBufferedTransport } from 'thrift';
import { RequestInit, FetchError } from 'node-fetch';
import ThriftHttpConnection, { THTTPException } from '../../../../lib/connection/connections/ThriftHttpConnection';
import NullRetryPolicy from '../../../../lib/connection/connections/NullRetryPolicy';
import TCLIService from '../../../../thrift/TCLIService';
import {
  TGetInfoReq,
  TGetInfoResp,
  TGetInfoType,
  TGetInfoValue,
  THandleIdentifier,
  TSessionHandle,
  TStatus,
  TStatusCode,
} from '../../../../thrift/TCLIService_types';

type Receiver = (
  callback: (trans: TBufferedTransport, seqid: number) => void,
  seqid: number,
  maxLength?: number,
) => (data: Buffer) => void;

interface Reply {
  status: number;
  headers?: Record<string, string>;
  body?: string | Buffer;
}

const PREFIX = 'Received a response with a bad HTTP status code';

function readStream(stream: NodeJS.ReadableStream): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer | string) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
    stream.once('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    stream.once('error', reject);
  });
}

// Sends one Thrift write to a local server that answers with `reply`, and
// returns the error handed to the pending Thrift callback.
async function failWith(
  reply: Reply | ((res: http.ServerResponse) => void),
  requestConfig: RequestInit = {},
  // Runs before the server is shut down, e.g. to read a body that is still streaming
  whileConnected?: (error: THTTPException) => Promise<void>,
): Promise<THTTPException> {
  const server = http.createServer((req, res) => {
    req.resume();
    if (typeof reply === 'function') {
      reply(res);
      return;
    }
    res.writeHead(reply.status, reply.headers);
    res.end(reply.body);
  });
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });
  try {
    const { port } = server.address() as AddressInfo;
    const connection = new ThriftHttpConnection(
      { url: `http://127.0.0.1:${port}/`, getRetryPolicy: async () => new NullRetryPolicy() },
      requestConfig,
    );
    const error = await new Promise<THTTPException>((resolve) => {
      connection.client = { _reqs: { 1: (err: unknown) => resolve(err as THTTPException) } } as any;
      connection.write(Buffer.from('x'), 1);
    });
    await whileConnected?.(error);
    return error;
  } finally {
    server.closeAllConnections();
    server.close();
  }
}

describe('ThriftHttpConnection', () => {
  let server: http.Server;
  let url: string;
  let replyLengths: number[];
  let requestLengths: number[];

  beforeEach(async () => {
    replyLengths = [];
    requestLengths = [];
    const processor = new TCLIService.Processor({
      GetInfo: async (req: TGetInfoReq) =>
        new TGetInfoResp({
          status: new TStatus({ statusCode: TStatusCode.SUCCESS_STATUS }),
          infoValue: new TGetInfoValue({ stringValue: `info-${req.infoType}-${'x'.repeat(4096)}` }),
        }),
    }) as unknown as { process(input: TBinaryProtocol, output: TBinaryProtocol): void };

    server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (chunk: Buffer) => chunks.push(chunk));
      req.on('end', () => {
        requestLengths.push(Buffer.concat(chunks).length);
        const output = new TBufferedTransport(undefined, (reply?: Buffer) => {
          replyLengths.push((reply as Buffer).length);
          res.writeHead(200, { 'Content-Type': 'application/x-thrift' });
          res.end(reply);
        });
        TBufferedTransport.receiver((transport) => {
          processor.process(new TBinaryProtocol(transport), new TBinaryProtocol(output));
        }, 0)(Buffer.concat(chunks));
      });
    });
    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', resolve);
    });
    url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/`;
  });

  afterEach(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => {
      server.close(resolve);
    });
  });

  it('caps the receive buffer at the message size for both the request and the response', async () => {
    const maxLengths: Array<number | undefined> = [];
    class RecordingTransport extends TBufferedTransport {
      static receiver(callback: (trans: TBufferedTransport, seqid: number) => void, seqid: number, maxLength?: number) {
        maxLengths.push(maxLength);
        return (TBufferedTransport.receiver as Receiver)(callback, seqid, maxLength);
      }
    }

    const connection = new ThriftHttpConnection({
      url,
      transport: RecordingTransport,
      protocol: TBinaryProtocol,
      getRetryPolicy: async () => new NullRetryPolicy(),
    });
    const client = thrift.createClient(TCLIService, connection as unknown as Parameters<typeof thrift.createClient>[1]);

    const sessionHandle = new TSessionHandle({
      sessionId: new THandleIdentifier({ guid: Buffer.alloc(16), secret: Buffer.alloc(16) }),
    });
    const response = await new Promise<TGetInfoResp>((resolve, reject) => {
      const request = new TGetInfoReq({ sessionHandle, infoType: TGetInfoType.CLI_SERVER_NAME });
      client.GetInfo(request, (error: unknown, result: TGetInfoResp) => {
        if (error) {
          reject(error);
        } else {
          resolve(result);
        }
      });
    });

    expect(response.infoValue.stringValue).to.equal(`info-${TGetInfoType.CLI_SERVER_NAME}-${'x'.repeat(4096)}`);
    expect(requestLengths).to.have.length(1);
    expect(replyLengths).to.have.length(1);
    // The first receiver call peeks at the request's method name; the second decodes the response.
    expect(maxLengths).to.deep.equal([requestLengths[0], replyLengths[0]]);
  });
});

describe('ThriftHttpConnection', () => {
  describe('non-200 responses', () => {
    it('keeps the bare message when the server sends no detail', async () => {
      const error = await failWith({ status: 401 });
      expect(error).to.be.instanceOf(THTTPException);
      expect(error.message).to.equal(`${PREFIX}: 401`);
      expect(error.statusCode).to.equal(401);
    });

    it('appends the Databricks reason-phrase header without consuming the body', async () => {
      const body = '{"error_code":403,"message":"Invalid access token. [ReqId: abc]"}';
      const error = await failWith({
        status: 403,
        headers: { 'x-databricks-reason-phrase': 'Invalid access token.', 'content-type': 'application/json' },
        body,
      });
      expect(error.message).to.equal(`${PREFIX}: 403: Invalid access token.`);
      expect(error.statusCode).to.equal(403);
      expect(await error.response.text()).to.equal(body);
    });

    it('prefers the Thrift server error header over other detail', async () => {
      const error = await failWith({
        status: 404,
        headers: {
          'x-thriftserver-error-message': 'ENDPOINT_NOT_FOUND: SQL warehouse does not exist',
          'x-databricks-reason-phrase': 'Not Found',
          'content-type': 'application/x-thrift',
        },
        body: Buffer.from([0x80, 0x01, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05]),
      });
      expect(error.message).to.equal(`${PREFIX}: 404: ENDPOINT_NOT_FOUND: SQL warehouse does not exist`);
    });

    it('falls back to the JSON body message and keeps the body readable', async () => {
      const body = '{"error_code":401,"message":"Invalid access token. [ReqId: abc]"}';
      const error = await failWith({ status: 401, headers: { 'content-type': 'application/json' }, body });
      expect(error.message).to.equal(`${PREFIX}: 401: Invalid access token. [ReqId: abc]`);
      expect(error.response.status).to.equal(401);
      expect(error.response.headers.get('content-type')).to.equal('application/json');
      expect(await error.response.text()).to.equal(body);
    });

    it('falls back to a plain-text body', async () => {
      const error = await failWith({
        status: 401,
        headers: { 'content-type': 'application/x-thrift' },
        body: 'Authentication Error [INVALID_AUTHORIZATION_SPECIFICATION]: Invalid access token\r\n',
      });
      expect(error.message).to.equal(
        `${PREFIX}: 401: Authentication Error [INVALID_AUTHORIZATION_SPECIFICATION]: Invalid access token`,
      );
    });

    it('ignores HTML and binary bodies', async () => {
      const html = await failWith({
        status: 502,
        headers: { 'content-type': 'text/html' },
        body: '<html><body>Bad Gateway</body></html>',
      });
      expect(html.message).to.equal(`${PREFIX}: 502`);

      const binary = await failWith({ status: 500, body: Buffer.from([0x80, 0x01, 0x00, 0x03, 0x41, 0x00]) });
      expect(binary.message).to.equal(`${PREFIX}: 500`);
    });

    it('collapses whitespace and bounds the detail length', async () => {
      const error = await failWith({ status: 401, body: `first\n\tline ${'x'.repeat(5000)}` });
      const detail = error.message.slice(`${PREFIX}: 401: `.length);
      expect(detail.startsWith('first line xxx')).to.equal(true);
      expect(detail.length).to.equal(501);
      expect(detail.endsWith('…')).to.equal(true);
    });

    it('reports the status only when a chunked body exceeds the byte budget, keeping the body readable', async () => {
      const chunk = `${'y'.repeat(1023)}\n`;
      const chunkCount = 200; // 200 KiB, beyond the 64 KiB budget
      let body: string | undefined;
      const error = await failWith(
        (res) => {
          res.writeHead(401, { 'content-type': 'text/plain' });
          for (let i = 0; i < chunkCount; i += 1) {
            res.write(chunk);
          }
          res.end();
        },
        {},
        async (err) => {
          body = await err.response.text();
        },
      );
      expect(error.message).to.equal(`${PREFIX}: 401`);
      expect(error.statusCode).to.equal(401);
      expect(error.response.status).to.equal(401);
      expect(error.response.headers.get('content-type')).to.equal('text/plain');
      expect(body).to.equal(chunk.repeat(chunkCount));
    });

    it('does not read a body whose Content-Length exceeds the byte budget', async () => {
      const body = 'z'.repeat(100 * 1024);
      let received: string | undefined;
      const error = await failWith(
        { status: 401, headers: { 'content-length': `${body.length}` }, body },
        {},
        async (err) => {
          received = await err.response.text();
        },
      );
      expect(error.message).to.equal(`${PREFIX}: 401`);
      expect(received).to.equal(body);
    });

    it('reports the status only when the body does not end within the time budget', async function test() {
      this.timeout(10_000);
      const started = Date.now();
      const error = await failWith((res) => {
        res.writeHead(503, { 'content-type': 'text/plain' });
        res.write('Service temporarily unavailable'); // never ended
      });
      expect(error.message).to.equal(`${PREFIX}: 503`);
      expect(error.statusCode).to.equal(503);
      expect(Date.now() - started).to.be.below(5_000);
    });

    it('exposes the read body as a stream that can be consumed with .on() and .pipe()', async () => {
      const body = '{"error_code":401,"message":"Invalid access token."}';
      const viaEvents = await failWith({ status: 401, headers: { 'content-type': 'application/json' }, body });
      expect(viaEvents.message).to.equal(`${PREFIX}: 401: Invalid access token.`);
      expect(viaEvents.response.body).to.be.instanceOf(Readable);
      expect(await readStream(viaEvents.response.body)).to.equal(body);

      const viaPipe = await failWith({ status: 401, headers: { 'content-type': 'application/json' }, body });
      expect(await readStream(viaPipe.response.body.pipe(new PassThrough()))).to.equal(body);
    });

    it('exposes a partially read body as a stream that can be piped', async () => {
      const chunk = `${'y'.repeat(1023)}\n`;
      const chunkCount = 100; // beyond the 64 KiB budget
      let body: string | undefined;
      const error = await failWith(
        (res) => {
          res.writeHead(401, { 'content-type': 'text/plain' });
          for (let i = 0; i < chunkCount; i += 1) {
            res.write(chunk);
          }
          res.end();
        },
        {},
        async (err) => {
          body = await readStream(err.response.body.pipe(new PassThrough()));
        },
      );
      expect(error.message).to.equal(`${PREFIX}: 401`);
      expect(body).to.equal(chunk.repeat(chunkCount));
    });

    it('keeps the request timeout on a replayed body that stalls after the byte budget', async function test() {
      this.timeout(10_000);
      let readError: unknown;
      const error = await failWith(
        (res) => {
          res.writeHead(401, { 'content-type': 'text/plain' });
          res.write('s'.repeat(70 * 1024)); // beyond the 64 KiB budget, never ended
        },
        { timeout: 300 },
        async (err) => {
          readError = await err.response.text().then(
            () => undefined,
            (e) => e,
          );
        },
      );
      expect(error.message).to.equal(`${PREFIX}: 401`);
      expect(error.response.timeout).to.equal(300);
      expect(readError).to.be.instanceOf(FetchError);
      expect((readError as FetchError).type).to.equal('body-timeout');
    });

    it('keeps the response size limit on a replayed body', async () => {
      const error = await failWith({ status: 401, body: 'x'.repeat(100) }, { size: 10 });
      expect(error.response.size).to.equal(10);
      const readError = await error.response.text().then(
        () => undefined,
        (e) => e,
      );
      expect(readError).to.be.instanceOf(FetchError);
      expect((readError as FetchError).type).to.equal('max-size');
    });

    it('never echoes request headers', async () => {
      const error = await failWith({ status: 401 }, { headers: { Authorization: 'Bearer dapi-secret-token' } });
      expect(error.message).to.not.include('dapi-secret-token');
    });
  });
});
