import { expect } from 'chai';
import sinon from 'sinon';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import KernelOperationBackend from '../../../lib/kernel/KernelOperationBackend';
import OperationStateError, { OperationStateErrorCode } from '../../../lib/errors/OperationStateError';
import ArrowResultConverter from '../../../lib/result/ArrowResultConverter';
import ResultSlicer from '../../../lib/result/ResultSlicer';
import { TTypeId } from '../../../thrift/TCLIService_types';
import ClientContextStub from '../.stubs/ClientContextStub';
import ResultsProviderStub from '../.stubs/ResultsProviderStub';

function createStatement() {
  const batches = [
    [0, 1, 2],
    [3, 4, 5],
    [6, 7, 8],
  ].map((ids) => ({
    ipcBytes: Buffer.from(tableToIPC(tableFromArrays({ id: Int32Array.from(ids) }))),
  }));
  return {
    statementId: 'concurrent-fetch',
    schema: () => batches[0],
    fetchNextBatch: sinon.stub().callsFake(async () => batches.shift() ?? null),
    close: sinon.stub().resolves(),
    cancel: sinon.stub().resolves(),
  };
}

describe('Kernel concurrent result consumption', () => {
  it('constructs one result pipeline for concurrent first readers', async () => {
    const backend = new KernelOperationBackend({ statement: createStatement(), context: new ClientContextStub() });
    const [first, second] = await Promise.all([backend['getResultSlicer'](), backend['getResultSlicer']()]);
    expect(first).to.equal(second);
  });

  for (const fetch of ['fetchChunk', 'hasMore'] as const) {
    it(`rejects a queued ${fetch} after close without touching the cursor`, async () => {
      const statement = createStatement();
      const backend = new KernelOperationBackend({ statement, context: new ClientContextStub() });
      await backend['getResultSlicer']();
      let release!: () => void;
      backend['resultConsumption'] = new Promise<void>((resolve) => {
        release = resolve;
      });
      const pending = fetch === 'fetchChunk' ? backend.fetchChunk({ limit: 100 }) : backend.hasMore();
      await backend.close();
      release();
      const [outcome] = await Promise.allSettled([pending]);
      expect(outcome.status).to.equal('rejected');
      if (outcome.status === 'rejected') {
        expect(outcome.reason).to.be.instanceOf(OperationStateError);
        expect(outcome.reason.errorCode).to.equal(OperationStateErrorCode.Closed);
      }
      expect(statement.fetchNextBatch.called).to.equal(false);
    });
  }

  it('serializes Arrow converter prefetch on an initialized pipeline', async () => {
    const context = new ClientContextStub();
    const backend = new KernelOperationBackend({ statement: createStatement(), context });
    const batches = [
      [0, 1],
      [2, 3],
      [4, 5],
    ].map((ids) => ({
      batches: [Buffer.from(tableToIPC(tableFromArrays({ id: Int32Array.from(ids) })))],
      rowCount: ids.length,
    }));
    const source = new ResultsProviderStub(batches, { batches: [], rowCount: 0 });
    const converter = new ArrowResultConverter(context, source, {
      schema: {
        columns: [
          { columnName: 'id', position: 1, typeDesc: { types: [{ primitiveEntry: { type: TTypeId.INT_TYPE } }] } },
        ],
      },
    });
    sinon.stub(backend as any, 'getResultSlicer').resolves(new ResultSlicer(context, converter));
    const chunks = await Promise.all([
      backend.fetchChunk({ limit: 100, disableBuffering: true }),
      backend.fetchChunk({ limit: 100, disableBuffering: true }),
    ]);
    while (await backend.hasMore()) {
      chunks.push(await backend.fetchChunk({ limit: 100, disableBuffering: true }));
    }
    expect(chunks.flat().map((row: any) => row.id)).to.deep.equal([0, 1, 2, 3, 4, 5]);
  });

  for (const initialized of [false, true]) {
    for (const limit of [1, 4, 100]) {
      it(`returns an exact union with overlapping chunks (initialized=${initialized}, limit=${limit})`, async () => {
        const backend = new KernelOperationBackend({ statement: createStatement(), context: new ClientContextStub() });
        if (initialized) {
          await backend.hasMore();
        }
        const chunks = await Promise.all([backend.fetchChunk({ limit }), backend.fetchChunk({ limit })]);
        while (await backend.hasMore()) {
          chunks.push(await backend.fetchChunk({ limit }));
        }
        const ids = chunks.flat().map((row: any) => row.id);
        expect(ids.sort((left, right) => left - right)).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, 8]);
      });
    }
  }

  it('does not lose prefetched rows when hasMore overlaps unbuffered reads', async () => {
    const backend = new KernelOperationBackend({ statement: createStatement(), context: new ClientContextStub() });
    const [, first, second] = await Promise.all([
      backend.hasMore(),
      backend.fetchChunk({ limit: 1, disableBuffering: true }),
      backend.fetchChunk({ limit: 1, disableBuffering: true }),
    ]);
    const chunks = [first, second];
    while (await backend.hasMore()) {
      chunks.push(await backend.fetchChunk({ limit: 1, disableBuffering: true }));
    }
    expect(chunks.flat().map((row: any) => row.id)).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, 8]);
  });
});
