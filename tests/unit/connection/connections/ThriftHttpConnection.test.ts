import http from 'http';
import { AddressInfo } from 'net';
import { expect } from 'chai';
import thrift, { TBinaryProtocol, TBufferedTransport } from 'thrift';
import ThriftHttpConnection from '../../../../lib/connection/connections/ThriftHttpConnection';
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
