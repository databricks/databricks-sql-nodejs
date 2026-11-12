import { execFileSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import { expect } from 'chai';
import { TBinaryProtocol, TBufferedTransport } from 'thrift';
import TCLIService from '../../thrift/TCLIService';
import {
  TGetInfoReq,
  TGetInfoResp,
  TGetInfoType,
  TGetInfoValue,
  THandleIdentifier,
  TSessionHandle,
  TStatus,
  TStatusCode,
} from '../../thrift/TCLIService_types';

// uuid@12+ is ESM-only; 11.x is the last major with a CommonJS build.
const LAST_CJS_UUID_MAJOR = 11;

describe('thrift CommonJS compatibility', () => {
  // npm never applies our package.json `overrides` to consumers, so they get whatever uuid thrift declares (#461).
  it("thrift's declared uuid range only admits CommonJS-loadable majors", () => {
    const thriftPackage = JSON.parse(fs.readFileSync(path.resolve('node_modules/thrift/package.json'), 'utf8'));
    const range: string | undefined = thriftPackage.dependencies?.uuid;
    if (range === undefined) {
      return;
    }

    const match = /^[\^~]?(\d+)\.\d+\.\d+$/.exec(range);
    expect(match, `unrecognized thrift uuid range "${range}"; check it only admits uuid <= ${LAST_CJS_UUID_MAJOR}`).to
      .not.be.null;
    expect(Number(match?.[1])).to.be.at.most(
      LAST_CJS_UUID_MAJOR,
      `thrift@${thriftPackage.version} declares uuid "${range}", which is ESM-only and breaks require('@databricks/sql') in Jest`,
    );
  });

  it('loads thrift and the generated client without require(esm)', function () {
    if (!process.allowedNodeEnvironmentFlags.has('--no-experimental-require-module')) {
      this.skip();
    }
    execFileSync(
      process.execPath,
      [
        '--no-experimental-require-module',
        '-e',
        "require('thrift'); require('./thrift/TCLIService'); require('./thrift/TCLIService_types'); require('uuid');",
      ],
      { cwd: process.cwd(), stdio: 'pipe' },
    );
  });

  // thrift@0.25 dropped `thrift.Q`, which the generated client/processor use for promise-style calls.
  it('round-trips a promise-style call through the generated client and processor', async () => {
    type RecvClient = Record<string, (input: TBinaryProtocol, mtype: number, rseqid: number) => void>;
    let client: RecvClient | undefined;

    const handler = {
      GetInfo: async (req: TGetInfoReq) =>
        new TGetInfoResp({
          status: new TStatus({ statusCode: TStatusCode.SUCCESS_STATUS }),
          infoValue: new TGetInfoValue({ stringValue: `info-${req.infoType}` }),
        }),
    };
    const processor = new TCLIService.Processor(handler) as unknown as {
      process(input: TBinaryProtocol, output: TBinaryProtocol): void;
    };

    const serverOutput = new TBufferedTransport(undefined, (reply?: Buffer) => {
      TBufferedTransport.receiver((transport) => {
        const input = new TBinaryProtocol(transport);
        const header = input.readMessageBegin();
        client?.[`recv_${header.fname}`](input, header.mtype, header.rseqid);
      }, 0)(reply as Buffer);
    });
    const clientOutput = new TBufferedTransport(undefined, (request?: Buffer) => {
      TBufferedTransport.receiver((transport) => {
        processor.process(new TBinaryProtocol(transport), new TBinaryProtocol(serverOutput));
      }, 0)(request as Buffer);
    });

    const thriftClient = new TCLIService.Client(clientOutput, TBinaryProtocol);
    client = thriftClient as unknown as RecvClient;

    const sessionHandle = new TSessionHandle({
      sessionId: new THandleIdentifier({ guid: Buffer.alloc(16), secret: Buffer.alloc(16) }),
    });
    const pending = (thriftClient.GetInfo as unknown as (req: TGetInfoReq) => Promise<TGetInfoResp>)(
      new TGetInfoReq({ sessionHandle, infoType: TGetInfoType.CLI_SERVER_NAME }),
    );
    expect(pending?.then).to.be.a('function');

    const response = await pending;
    expect(response.infoValue.stringValue).to.equal(`info-${TGetInfoType.CLI_SERVER_NAME}`);
  });
});
