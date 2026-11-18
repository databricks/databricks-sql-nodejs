import { expect } from 'chai';
import sinon from 'sinon';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import KernelOperationBackend from '../../../lib/kernel/KernelOperationBackend';
import KernelResultsProvider from '../../../lib/kernel/KernelResultsProvider';
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
  it('shares one provider prime between concurrent hasMore calls', async () => {
    const statement = createStatement();
    const provider = new KernelResultsProvider(statement);
    expect(await Promise.all([provider.hasMore(), provider.hasMore()])).to.deep.equal([true, true]);
    expect(statement.fetchNextBatch.callCount).to.equal(1);
    let rows = 0;
    while (await provider.hasMore()) {
      rows += (await provider.fetchNext({ limit: 100 })).rowCount;
    }
    expect(rows).to.equal(9);
  });

  it('releases the provider prime after a rejection', async () => {
    const statement = createStatement();
    const failure = new Error('fetch failed');
    statement.fetchNextBatch.onFirstCall().rejects(failure);
    const provider = new KernelResultsProvider(statement);
    const outcomes = await Promise.allSettled([provider.hasMore(), provider.hasMore()]);
    expect(outcomes).to.deep.equal([
      { status: 'rejected', reason: failure },
      { status: 'rejected', reason: failure },
    ]);
    expect(await provider.hasMore()).to.equal(true);
    expect((await provider.fetchNext({ limit: 100 })).rowCount).to.equal(3);
  });

  it('constructs one result pipeline for concurrent first readers', async () => {
    const backend = new KernelOperationBackend({ statement: createStatement(), context: new ClientContextStub() });
    const [first, second] = await Promise.all([backend['getResultSlicer'](), backend['getResultSlicer']()]);
    expect(first).to.equal(second);
  });

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
