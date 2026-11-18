import { expect } from 'chai';
import sinon from 'sinon';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import DBSQLOperation from '../../../lib/DBSQLOperation';
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

function createOperation() {
  const context = new ClientContextStub();
  const statement = createStatement();
  const backend = new KernelOperationBackend({ statement, context });
  const operation = new DBSQLOperation({ backend, context });
  return { operation, backend, context, statement };
}

describe('Kernel concurrent result consumption through DBSQLOperation', () => {
  it('constructs one result pipeline for concurrent first readers', async () => {
    const { operation, backend } = createOperation();
    const getResultSlicer = sinon.spy(backend as any, 'getResultSlicer');
    await Promise.all([operation.fetchChunk({ maxRows: 1 }), operation.fetchChunk({ maxRows: 1 })]);
    const [first, second] = await Promise.all(getResultSlicer.returnValues);
    expect(first).to.equal(second);
  });

  it('releases a failed read so the next waiter observes cleanup before touching the cursor', async () => {
    const { operation, statement } = createOperation();
    statement.fetchNextBatch.rejects(new Error('fetch failed'));
    const outcomes = await Promise.allSettled([operation.fetchChunk(), operation.fetchChunk()]);
    expect(outcomes.map((outcome) => outcome.status)).to.deep.equal(['rejected', 'rejected']);
    if (outcomes[0].status === 'rejected' && outcomes[1].status === 'rejected') {
      expect(outcomes[0].reason.message).to.equal('fetch failed');
      expect(outcomes[1].reason).to.be.instanceOf(OperationStateError);
      expect(outcomes[1].reason.errorCode).to.equal(OperationStateErrorCode.Closed);
    }
    expect(statement.fetchNextBatch.callCount).to.equal(1);
    expect(statement.close.callCount).to.equal(1);
  });

  for (const fetch of ['fetchChunk', 'hasMoreRows'] as const) {
    it(`honors close for queued ${fetch} without touching the cursor`, async () => {
      const { operation, backend, statement } = createOperation();
      await backend['getResultSlicer']();
      let release!: () => void;
      (operation as any).resultConsumption = new Promise<void>((resolve) => {
        release = resolve;
      });
      const pending = fetch === 'fetchChunk' ? operation.fetchChunk({ maxRows: 100 }) : operation.hasMoreRows();
      await new Promise<void>((resolve) => setImmediate(resolve));
      await operation.close();
      release();
      const [outcome] = await Promise.allSettled([pending]);
      if (fetch === 'hasMoreRows') {
        expect(outcome).to.deep.equal({ status: 'fulfilled', value: false });
      } else {
        expect(outcome.status).to.equal('rejected');
        if (outcome.status === 'rejected') {
          expect(outcome.reason).to.be.instanceOf(OperationStateError);
          expect(outcome.reason.errorCode).to.equal(OperationStateErrorCode.Closed);
        }
      }
      expect(statement.fetchNextBatch.called).to.equal(false);
    });
  }

  it('preserves unread Arrow rows when consumers await different source batches', async () => {
    const { operation, backend, context } = createOperation();
    const batches = [
      [0, 1],
      [2, 3],
      [4, 5],
    ].map((ids) => ({
      batches: [Buffer.from(tableToIPC(tableFromArrays({ id: Int32Array.from(ids) })))],
      rowCount: ids.length,
    }));
    const source = new ResultsProviderStub(batches, { batches: [], rowCount: 0 });
    const releases: Array<() => void> = [];
    const gates = batches.map(() => new Promise<void>((resolve) => releases.push(resolve)));
    const fetchNext = source.fetchNext.bind(source);
    let next = 0;
    sinon.stub(source, 'fetchNext').callsFake(async () => {
      const gate = gates[next++];
      const batch = await fetchNext();
      await gate;
      return batch;
    });
    const converter = new ArrowResultConverter(context, source, {
      schema: {
        columns: [
          { columnName: 'id', position: 1, typeDesc: { types: [{ primitiveEntry: { type: TTypeId.INT_TYPE } }] } },
        ],
      },
    });
    sinon.stub(backend as any, 'getResultSlicer').resolves(new ResultSlicer(context, converter));
    const pending = Promise.all([
      operation.fetchChunk({ maxRows: 100, disableBuffering: true }),
      operation.fetchChunk({ maxRows: 100, disableBuffering: true }),
    ]);
    await new Promise<void>((resolve) => setImmediate(resolve));
    releases.forEach((release) => release());
    const chunks = await pending;
    while (await operation.hasMoreRows()) {
      chunks.push(await operation.fetchChunk({ maxRows: 100, disableBuffering: true }));
    }
    expect(chunks.flat().map((row: any) => row.id)).to.deep.equal([0, 1, 2, 3, 4, 5]);
  });

  for (const initialized of [false, true]) {
    for (const limit of [1, 4, 100]) {
      it(`returns an exact union with overlapping chunks (initialized=${initialized}, limit=${limit})`, async () => {
        const { operation } = createOperation();
        if (initialized) {
          await operation.hasMoreRows();
        }
        const chunks = await Promise.all([
          operation.fetchChunk({ maxRows: limit }),
          operation.fetchChunk({ maxRows: limit }),
        ]);
        while (await operation.hasMoreRows()) {
          chunks.push(await operation.fetchChunk({ maxRows: limit }));
        }
        const ids = chunks.flat().map((row: any) => row.id);
        expect(ids.sort((left, right) => left - right)).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, 8]);
      });
    }
  }

  it('does not lose prefetched rows when hasMore overlaps unbuffered reads', async () => {
    const { operation } = createOperation();
    const [, first, second] = await Promise.all([
      operation.hasMoreRows(),
      operation.fetchChunk({ maxRows: 1, disableBuffering: true }),
      operation.fetchChunk({ maxRows: 1, disableBuffering: true }),
    ]);
    const chunks = [first, second];
    while (await operation.hasMoreRows()) {
      chunks.push(await operation.fetchChunk({ maxRows: 1, disableBuffering: true }));
    }
    expect(chunks.flat().map((row: any) => row.id)).to.deep.equal([0, 1, 2, 3, 4, 5, 6, 7, 8]);
  });
});
