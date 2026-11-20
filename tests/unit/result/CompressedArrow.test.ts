import { expect } from 'chai';
import sinon from 'sinon';
import Int64 from 'node-int64';
import { Response } from 'node-fetch';
import { Binary, Bool, Float64, Table, Utf8, tableFromIPC, tableToIPC, vectorFromArray } from 'apache-arrow';
import ArrowResultHandler from '../../../lib/result/ArrowResultHandler';
import CloudFetchResultHandler from '../../../lib/result/CloudFetchResultHandler';
import ArrowResultConverter from '../../../lib/result/ArrowResultConverter';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import { TStatusCode, TTypeId } from '../../../thrift/TCLIService_types';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';
import ClientContextStub from '../.stubs/ClientContextStub';
import getLZ4 from '../../../lib/utils/lz4';

describe('concatenated LZ4 Arrow results', () => {
  let codec: ReturnType<typeof getLZ4>;

  before(function () {
    codec = getLZ4();
    if (!codec) this.skip();
  });

  afterEach(() => sinon.restore());

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

  const createCloudFetch = (body: Buffer, rowCount: number, compressed = true, linkCount = 1) => {
    const context = new ClientContextStub({ cloudFetchConcurrentDownloads: 2 });
    const resultLinks = Array.from({ length: linkCount }, (_, index) => ({
      fileLink: `http://localhost/result/${index}`,
      rowCount: new Int64(rowCount),
      startRowOffset: new Int64(0),
      expiryTime: new Int64(Date.now() + 60000),
      bytesNum: new Int64(body.length),
    }));
    const handler = new CloudFetchResultHandler(
      context,
      new ResultsProviderStub([{ startRowOffset: new Int64(0), rows: [], resultLinks }], undefined),
      { lz4Compressed: compressed, status: { statusCode: TStatusCode.SUCCESS_STATUS } },
    );
    sinon.stub(handler as any, 'fetch').resolves({ response: new Response(), body });
    return { handler, context };
  };

  const thriftSchema = {
    columns: [
      ['number', TTypeId.DOUBLE_TYPE],
      ['text', TTypeId.STRING_TYPE],
      ['boolean', TTypeId.BOOLEAN_TYPE],
      ['binary', TTypeId.BINARY_TYPE],
    ].map(([columnName, type], position) => ({
      columnName: columnName as string,
      position,
      typeDesc: { types: [{ primitiveEntry: { type: type as TTypeId } }] },
    })),
  };

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

  for (const compressed of [false, true]) {
    for (const expectedRows of [0, 2, 3, 4]) {
      it(`caps or validates CloudFetch rows ${expectedRows}, compressed=${compressed}`, async () => {
        const { handler, context } = createCloudFetch(compressed ? compress(ipc) : ipc, expectedRows, compressed);
        const converter = new ArrowResultConverter(context, handler, { schema: thriftSchema });
        if (expectedRows <= 3) {
          expect(await converter.fetchNext({ limit: 10 })).to.deep.equal(
            [
              { number: 1, text: 'first', boolean: true, binary: Buffer.from([0, 255]) },
              { number: null, text: 'second', boolean: null, binary: null },
              { number: 3.5, text: null, boolean: false, binary: Buffer.alloc(0) },
            ].slice(0, expectedRows),
          );
          expect(await converter.hasMore()).to.equal(false);
        } else {
          let caught: unknown;
          try {
            await converter.fetchNext({ limit: 10 });
          } catch (error) {
            caught = error;
          }
          expect(caught).to.be.instanceOf(HiveDriverError);
          expect((caught as Error).message).to.equal('Arrow row count mismatch: 1 rows missing');
        }
      });
    }

    it(`rejects an empty payload with declared rows, compressed=${compressed}`, async () => {
      const { handler, context } = createCloudFetch(
        compressed ? codec!.encode(Buffer.alloc(0)) : Buffer.alloc(0),
        1,
        compressed,
      );
      const converter = new ArrowResultConverter(context, handler, { schema: thriftSchema });
      let caught: unknown;
      try {
        await converter.fetchNext({ limit: 10 });
      } catch (error) {
        caught = error;
      }
      expect(caught).to.be.instanceOf(HiveDriverError);
      expect((caught as Error).message).to.equal('Arrow row count mismatch: 1 rows missing');
    });
  }

  it('preserves complete Arrow batches in separate frames', async () => {
    const parts = [ipc.subarray(0, -8), ipc.subarray(schemaEnd)];
    const { handler } = createCloudFetch(Buffer.concat(parts.map((part) => codec!.encode(part))), 6);
    const result = await handler.fetchNext({ limit: 1 });
    expect(result.rowCount).to.equal(6);
    expect(result.batches).to.deep.equal(parts);
    expect(tableFromIPC(result.batches).toArray()).to.deep.equal([...table.toArray(), ...table.toArray()]);
  });

  it('starts bounded asynchronous decoding for prefetched files before consuming them in order', async () => {
    const pending: Array<(buffers: Array<Buffer>) => void> = [];
    const decode = sinon
      .stub(codec!, 'decodeFramesAsync')
      .callsFake(() => new Promise((resolve) => pending.push(resolve)));
    const { handler } = createCloudFetch(compress(ipc), 3, true, 3);
    let completed = false;
    const first = handler.fetchNext({ limit: 10 }).then((batch) => {
      completed = true;
      return batch;
    });
    await new Promise(setImmediate);
    expect(decode.callCount).to.equal(2);
    pending[1]([ipc]);
    await new Promise(setImmediate);
    expect(completed).to.equal(false);
    pending[0]([ipc]);
    expect((await first).batches).to.deep.equal([ipc]);
    expect((await handler.fetchNext({ limit: 10 })).batches).to.deep.equal([ipc]);
    await new Promise(setImmediate);
    expect(decode.callCount).to.equal(3);
    pending[2]([ipc]);
    expect((await handler.fetchNext({ limit: 10 })).batches).to.deep.equal([ipc]);
  });

  it('handles an abandoned prefetched decode rejection and surfaces it only when consumed', async () => {
    const unhandled: Array<unknown> = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on('unhandledRejection', onUnhandled);
    try {
      const error = new HiveDriverError('LZ4 frame 1 at offset 10: ContentChecksumError');
      const decode = sinon.stub(codec!, 'decodeFramesAsync');
      decode.onFirstCall().resolves([ipc]);
      decode.onSecondCall().rejects(error);
      const { handler } = createCloudFetch(compress(ipc), 3, true, 2);
      expect((await handler.fetchNext({ limit: 10 })).batches).to.deep.equal([ipc]);
      await new Promise(setImmediate);
      expect(unhandled).to.deep.equal([]);
      let caught: unknown;
      try {
        await handler.fetchNext({ limit: 10 });
      } catch (reason) {
        caught = reason;
      }
      expect(caught).to.equal(error);
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });
});
