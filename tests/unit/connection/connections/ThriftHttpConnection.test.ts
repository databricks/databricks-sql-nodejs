import http from 'http';
import { AddressInfo } from 'net';
import { PassThrough } from 'stream';
import { expect } from 'chai';
import thrift, { TBinaryProtocol, TBufferedTransport } from 'thrift';
import * as nodeFetch from 'node-fetch';
import sinon from 'sinon';
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

describe('ThriftHttpConnection non-200 responses', () => {
  const sandbox = sinon.createSandbox();
  const prefix = 'Received a response with a bad HTTP status code';

  afterEach(() => sandbox.restore());

  async function failWith(response: nodeFetch.Response): Promise<THTTPException> {
    sandbox.stub(nodeFetch, 'default').resolves(response);
    const connection = new ThriftHttpConnection({
      url: 'https://example.com/sql',
      getRetryPolicy: async () => new NullRetryPolicy(),
    });
    return new Promise((resolve) => {
      connection.client = { _reqs: { 1: resolve } } as any;
      connection.write(Buffer.from('x'), 1);
    });
  }

  it('appends server detail and preserves the original unread response', async () => {
    const body = new PassThrough();
    const json = '{"message":"Invalid access token. [ReqId: abc]"}';
    body.end(json);
    const response = new nodeFetch.Response(body, {
      status: 403,
      headers: { 'x-databricks-reason-phrase': 'Invalid access token.' },
      timeout: 300,
      size: 1024,
    });

    const error = await failWith(response);

    expect(error).to.be.instanceOf(THTTPException);
    expect(error.message).to.equal(`${prefix}: 403: Invalid access token.`);
    expect(error.statusCode).to.equal(403);
    expect(error.response).to.equal(response);
    expect(error.response.body).to.equal(body);
    expect(error.response.bodyUsed).to.equal(false);
    expect(await error.response.text()).to.equal(json);
  });

  it('prefers the Thrift server detail over the other headers', async () => {
    const error = await failWith(
      new nodeFetch.Response(undefined, {
        status: 404,
        headers: {
          'x-thriftserver-error-message': 'ENDPOINT_NOT_FOUND: SQL warehouse does not exist',
          'x-databricks-error-or-redirect-message': 'Redirect detail',
          'x-databricks-reason-phrase': 'Not Found',
        },
      }),
    );

    expect(error.message).to.equal(`${prefix}: 404: ENDPOINT_NOT_FOUND: SQL warehouse does not exist`);
  });

  it('uses the redirect detail before the reason phrase', async () => {
    const error = await failWith(
      new nodeFetch.Response(undefined, {
        status: 403,
        headers: {
          'x-databricks-error-or-redirect-message': 'Access denied',
          'x-databricks-reason-phrase': 'Forbidden',
        },
      }),
    );

    expect(error.message).to.equal(`${prefix}: 403: Access denied`);
  });

  it('skips blank headers and normalizes whitespace', async () => {
    const error = await failWith(
      new nodeFetch.Response(undefined, {
        status: 403,
        headers: {
          'x-thriftserver-error-message': ' \t ',
          'x-databricks-reason-phrase': '  Invalid\t access   token.  ',
        },
      }),
    );

    expect(error.message).to.equal(`${prefix}: 403: Invalid access token.`);
  });

  it('bounds the header detail to 500 characters including the ellipsis', async () => {
    const error = await failWith(
      new nodeFetch.Response(undefined, {
        status: 403,
        headers: { 'x-databricks-reason-phrase': 'x'.repeat(1000) },
      }),
    );

    expect(error.message).to.equal(`${prefix}: 403: ${'x'.repeat(499)}…`);
  });

  it('keeps the status-only message and leaves a headerless body unread', async () => {
    const body = new PassThrough();
    const json = '{"message":"Invalid access token."}';
    body.end(json);
    const response = new nodeFetch.Response(body, { status: 401 });

    const error = await failWith(response);

    expect(error.message).to.equal(`${prefix}: 401`);
    expect(error.statusCode).to.equal(401);
    expect(error.response).to.equal(response);
    expect(error.response.body).to.equal(body);
    expect(error.response.bodyUsed).to.equal(false);
    expect(await error.response.text()).to.equal(json);
  });

  it('reports the HTTP error without waiting for an unfinished body', async () => {
    const body = new PassThrough();
    body.write('Service temporarily unavailable');
    const response = new nodeFetch.Response(body, { status: 503 });
    try {
      const error = await failWith(response);

      expect(error.message).to.equal(`${prefix}: 503`);
      expect(error.response).to.equal(response);
      expect(body.readableFlowing).to.equal(null);
    } finally {
      body.destroy();
    }
  });
});
