import { expect } from 'chai';
import sinon from 'sinon';
import Int64 from 'node-int64';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import OperationStateError, { OperationStateErrorCode } from '../../lib/errors/OperationStateError';
import {
  TFetchOrientation,
  TOperationState,
  TOperationType,
  TSparkRowSetType,
  TStatusCode,
  TTypeId,
} from '../../thrift/TCLIService_types';
import ClientContextStub from './.stubs/ClientContextStub';
import { createOperationForTest } from './.stubs/createOperationForTest';

function createFinishedOperation(context: ClientContextStub) {
  return createOperationForTest({
    context,
    handle: {
      operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
      operationType: TOperationType.EXECUTE_STATEMENT,
      hasResultSet: true,
    },
    directResults: {
      operationStatus: {
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
        operationState: TOperationState.FINISHED_STATE,
      },
    },
  });
}

describe('Concurrent Thrift result consumption', () => {
  for (const phase of ['metadata', 'fetch'] as const) {
    it(`rejects a queued fetchChunk after close during an in-flight ${phase} request without further RPCs`, async () => {
      const clock = sinon.useFakeTimers();
      const context = new ClientContextStub();
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const metadata = sinon.stub(context.driver, 'getResultSetMetadata').callsFake(async () => {
        if (phase === 'metadata') {
          await gate;
          throw new Error('metadata request interrupted by close');
        }
        return context.driver.getResultSetMetadataResp;
      });
      const fetch = sinon.stub(context.driver, 'fetchResults').callsFake(async () => {
        await gate;
        return context.driver.fetchResultsResp;
      });
      const operation = createFinishedOperation(context);
      try {
        const pending = Promise.allSettled([
          operation.fetchChunk({ maxRows: 1 }),
          operation.fetchChunk({ maxRows: 1 }),
        ]);
        await clock.tickAsync(0);
        const beforeClose = { metadata: metadata.callCount, fetch: fetch.callCount };
        expect(beforeClose).to.deep.equal({ metadata: 1, fetch: phase === 'fetch' ? 1 : 0 });
        await operation.close();
        release();
        await clock.runAllAsync();
        const outcomes = await pending;
        expect({ metadata: metadata.callCount, fetch: fetch.callCount }).to.deep.equal(beforeClose);
        expect(outcomes.map((outcome) => outcome.status)).to.deep.equal(['rejected', 'rejected']);
        if (outcomes[1].status === 'rejected') {
          expect(outcomes[1].reason).to.be.instanceOf(OperationStateError);
          expect(outcomes[1].reason.errorCode).to.equal(OperationStateErrorCode.Closed);
        }
      } finally {
        release();
        clock.restore();
      }
    });
  }

  it('preserves an exact union through the Thrift Arrow pipeline when concurrent reads succeed', async () => {
    const clock = sinon.useFakeTimers();
    const context = new ClientContextStub();
    let arrowSchema!: Buffer;
    const batches = [
      [0, 1],
      [2, 3],
      [4, 5],
    ].map((ids) => {
      const ipc = Buffer.from(tableToIPC(tableFromArrays({ id: Int32Array.from(ids) })));
      const schemaEnd = 8 + ipc.readInt32LE(4);
      arrowSchema = ipc.subarray(0, schemaEnd);
      return { batch: ipc.subarray(schemaEnd), rowCount: new Int64(ids.length) };
    });
    context.driver.getResultSetMetadataResp = {
      status: { statusCode: TStatusCode.SUCCESS_STATUS },
      resultFormat: TSparkRowSetType.ARROW_BASED_SET,
      arrowSchema,
      schema: {
        columns: [
          { columnName: 'id', position: 1, typeDesc: { types: [{ primitiveEntry: { type: TTypeId.INT_TYPE } }] } },
        ],
      },
    };
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let next = 0;
    sinon.stub(context.driver, 'fetchResults').callsFake(async () => {
      const index = next++;
      await gate;
      return {
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
        hasMoreRows: index < batches.length - 1,
        results: {
          startRowOffset: new Int64(index * 2),
          rows: [],
          arrowBatches: batches[index] ? [batches[index]] : [],
        },
      };
    });
    const operation = createFinishedOperation(context);
    try {
      const pending = Promise.all([operation.fetchAll(), operation.fetchAll()]);
      await clock.tickAsync(0);
      release();
      await clock.runAllAsync();
      const chunks = await pending;
      expect(
        chunks
          .flat()
          .map((row: any) => row.id)
          .sort((left, right) => left - right),
      ).to.deep.equal([0, 1, 2, 3, 4, 5]);
    } finally {
      release();
      clock.restore();
    }
  });

  it('sends FETCH_FIRST once when two first reads start without prefetched results', async () => {
    const clock = sinon.useFakeTimers();
    const context = new ClientContextStub();
    const orientations: Array<TFetchOrientation | undefined> = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    sinon.stub(context.driver, 'fetchResults').callsFake(async ({ orientation }) => {
      orientations.push(orientation);
      await gate;
      return {
        status: { statusCode: TStatusCode.SUCCESS_STATUS },
        hasMoreRows: false,
        results: {
          startRowOffset: new Int64(0),
          rows: [],
          columns: [
            {
              i32Val: {
                values: orientation === TFetchOrientation.FETCH_FIRST ? [0, 1, 2] : [],
                nulls: Buffer.alloc(1),
              },
            },
          ],
        },
      };
    });
    const operation = createOperationForTest({
      context,
      handle: {
        operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
        operationType: TOperationType.EXECUTE_STATEMENT,
        hasResultSet: true,
      },
      directResults: {
        operationStatus: {
          status: { statusCode: TStatusCode.SUCCESS_STATUS },
          operationState: TOperationState.FINISHED_STATE,
        },
        resultSetMetadata: {
          status: { statusCode: TStatusCode.SUCCESS_STATUS },
          resultFormat: TSparkRowSetType.COLUMN_BASED_SET,
          schema: {
            columns: [
              { columnName: 'id', position: 1, typeDesc: { types: [{ primitiveEntry: { type: TTypeId.INT_TYPE } }] } },
            ],
          },
        },
      },
    });
    try {
      const pending = Promise.all([operation.fetchAll(), operation.fetchAll()]);
      await clock.tickAsync(0);
      release();
      await clock.runAllAsync();
      const chunks = await pending;
      expect({
        firstReads: orientations.filter((orientation) => orientation === TFetchOrientation.FETCH_FIRST).length,
        ids: chunks.flat().map((row: any) => row.id),
      }).to.deep.equal({ firstReads: 1, ids: [0, 1, 2] });
    } finally {
      clock.restore();
    }
  });
});
