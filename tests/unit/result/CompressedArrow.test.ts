import { expect } from 'chai';
import sinon from 'sinon';
import Int64 from 'node-int64';
import { Binary, Bool, Float64, Table, Utf8, tableFromIPC, tableToIPC, vectorFromArray } from 'apache-arrow';
import ArrowResultHandler from '../../../lib/result/ArrowResultHandler';
import CloudFetchResultHandler from '../../../lib/result/CloudFetchResultHandler';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import { TStatusCode } from '../../../thrift/TCLIService_types';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';
import ClientContextStub from '../.stubs/ClientContextStub';
import getLZ4 from '../../../lib/utils/lz4';

describe('concatenated LZ4 Arrow results', () => {
  let codec: ReturnType<typeof getLZ4>;

  before(function () {
    codec = getLZ4();
    if (!codec) this.skip();
  });

  const table = new Table({
    number: vectorFromArray([1, null, 3.5], new Float64()),
    text: vectorFromArray(['first', 'second', null], new Utf8()),
    boolean: vectorFromArray([true, null, false], new Bool()),
    binary: vectorFromArray([Buffer.from([0, 255]), null, Buffer.alloc(0)], new Binary()),
  });
  const ipc = Buffer.from(tableToIPC(table));
  const schemaEnd = 8 + ipc.readInt32LE(4);
  const schema = ipc.subarray(0, schemaEnd);
  const payload = ipc.subarray(schemaEnd);
  const compress = (bytes: Buffer) =>
    Buffer.concat([
      codec!.encode(bytes.subarray(0, 16)),
      codec!.encode(Buffer.alloc(0)),
      codec!.encode(bytes.subarray(16)),
    ]);

  it('preserves inline values, types, nulls and bytes across frame boundaries', async () => {
    const handler = new ArrowResultHandler(
      new ClientContextStub(),
      new ResultsProviderStub(
        [
          {
            startRowOffset: new Int64(0),
            rows: [],
            arrowBatches: [{ batch: compress(payload), rowCount: new Int64(3) }],
          },
        ],
        undefined,
      ),
      { arrowSchema: schema, lz4Compressed: true, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
    );
    const result = await handler.fetchNext({ limit: 1 });
    const bytes = Buffer.concat(result.batches);
    expect(bytes).to.deep.equal(ipc);
    expect(result.rowCount).to.equal(3);
    expect(tableFromIPC(bytes).toArray()).to.deep.equal(table.toArray());
    expect(tableFromIPC(bytes).schema).to.deep.equal(table.schema);
  });

  for (const expectedRows of [0, 2, 3, 4]) {
    it(`validates CloudFetch row count ${expectedRows} against all decoded frames`, async () => {
      const link = {
        fileLink: 'http://localhost/result',
        rowCount: new Int64(expectedRows),
        startRowOffset: new Int64(0),
        expiryTime: new Int64(Date.now() + 60000),
        bytesNum: new Int64(0),
      };
      const handler = new CloudFetchResultHandler(
        new ClientContextStub(),
        new ResultsProviderStub([{ startRowOffset: new Int64(0), rows: [], resultLinks: [link] }], undefined),
        { lz4Compressed: true, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
      );
      sinon.stub(handler as any, 'downloadLink').resolves({ batches: [compress(ipc)], rowCount: expectedRows });
      if (expectedRows === 3) {
        const result = await handler.fetchNext({ limit: 1 });
        expect(Buffer.concat(result.batches)).to.deep.equal(ipc);
        expect(result.batches).to.have.length(3);
        expect(result.rowCount).to.equal(3);
      } else {
        let caught: unknown;
        try {
          await handler.fetchNext({ limit: 1 });
        } catch (error) {
          caught = error;
        }
        expect(caught).to.be.instanceOf(HiveDriverError);
        expect((caught as Error).message).to.equal(`CloudFetch row count mismatch: expected ${expectedRows}, got 3`);
      }
    });
  }

  it('preserves complete Arrow batches in separate frames', async () => {
    const parts = [ipc.subarray(0, -8), ipc.subarray(schemaEnd)];
    const link = {
      fileLink: 'http://localhost/result',
      rowCount: new Int64(6),
      startRowOffset: new Int64(0),
      expiryTime: new Int64(Date.now() + 60000),
      bytesNum: new Int64(0),
    };
    const handler = new CloudFetchResultHandler(
      new ClientContextStub(),
      new ResultsProviderStub([{ startRowOffset: new Int64(0), rows: [], resultLinks: [link] }], undefined),
      { lz4Compressed: true, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
    );
    sinon.stub(handler as any, 'downloadLink').resolves({
      batches: [Buffer.concat(parts.map((part) => codec!.encode(part)))],
      rowCount: 6,
    });
    const result = await handler.fetchNext({ limit: 1 });
    expect(result.rowCount).to.equal(6);
    expect(result.batches).to.deep.equal(parts);
    expect(tableFromIPC(result.batches).toArray()).to.deep.equal([...table.toArray(), ...table.toArray()]);
  });
});
