import { expect } from 'chai';
import sinon from 'sinon';
import Int64 from 'node-int64';
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

describe('Concurrent Thrift result consumption', () => {
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
