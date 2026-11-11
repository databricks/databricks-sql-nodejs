import http from 'http';
import { AddressInfo } from 'net';
import { expect } from 'chai';
import thrift, { TBinaryProtocol, TBufferedTransport } from 'thrift';
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

// Sends one Thrift write to a local server that answers with `reply`, and
// returns the error handed to the pending Thrift callback.
async function failWith(reply: Reply, requestHeaders: Record<string, string> = {}): Promise<THTTPException> {
  const server = http.createServer((req, res) => {
    req.resume();
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
      { headers: requestHeaders },
    );
    return await new Promise<THTTPException>((resolve) => {
      connection.client = { _reqs: { 1: (error: unknown) => resolve(error as THTTPException) } } as any;
      connection.write(Buffer.from('x'), 1);
    });
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

    it('never echoes request headers', async () => {
      const error = await failWith({ status: 401 }, { Authorization: 'Bearer dapi-secret-token' });
      expect(error.message).to.not.include('dapi-secret-token');
    });
  });
});
