import { expect } from 'chai';
import { Table, tableFromArrays, tableToIPC } from 'apache-arrow';
import Int64 from 'node-int64';
import sinon from 'sinon';
import {
  TCloseOperationResp,
  TFetchResultsResp,
  TOperationState,
  TOperationType,
  TSparkRowSetType,
  TStatusCode,
  TTypeId,
} from '../../../thrift/TCLIService_types';
import OperationStateError, { OperationStateErrorCode } from '../../../lib/errors/OperationStateError';
import StatusError from '../../../lib/errors/StatusError';
import CloudFetchResultHandler from '../../../lib/result/CloudFetchResultHandler';
import { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import ClientContextStub from '../.stubs/ClientContextStub';
import { createOperationForTest } from '../.stubs/createOperationForTest';

describe('Thrift fetch failure', () => {
  function setup(directResults = false) {
    const context = new ClientContextStub();
    context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
    const operation = createOperationForTest({
      context,
      handle: {
        operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
        operationType: TOperationType.EXECUTE_STATEMENT,
        hasResultSet: true,
      },
      directResults: directResults
        ? {
            operationStatus: context.driver.getOperationStatusResp,
            resultSetMetadata: context.driver.getResultSetMetadataResp,
            resultSet: context.driver.fetchResultsResp,
            closeOperation: context.driver.closeOperationResp,
          }
        : undefined,
    });
    return { context, operation };
  }

  for (const closeFails of [false, true]) {
    it(`rejects later reads after an exhausted fetch, even when close ${
      closeFails ? 'fails' : 'succeeds'
    }`, async () => {
      const { context, operation } = setup();
      const fetchError = new Error('exhausted fetch retries');
      const fetch = sinon.stub(context.driver, 'fetchResults').callThrough();
      fetch.onFirstCall().rejects(fetchError);
      const close = sinon.stub(context.driver, 'closeOperation').callThrough();
      if (closeFails) {
        close.rejects(new Error('close failed'));
      }

      await operation.fetchChunk({ maxRows: 3 }).then(
        () => expect.fail('fetch should fail'),
        (error) => expect(error).to.equal(fetchError),
      );

      for (const read of [
        () => operation.fetchChunk({ maxRows: 3 }),
        () => operation.fetchAll(),
        () => operation.hasMoreRows(),
      ]) {
        await read().then(
          () => expect.fail('later reads must not recover a suffix'),
          (error) => {
            expect(error).to.be.instanceOf(OperationStateError);
            expect(error.errorCode).to.equal(OperationStateErrorCode.Error);
          },
        );
      }
      expect(fetch.callCount).to.equal(1);
      expect(close.callCount).to.equal(1);
    });

    it(`reports the fetch error without waiting for cleanup and reuses its ${
      closeFails ? 'failure' : 'success'
    } on close`, async () => {
      const clock = sinon.useFakeTimers();
      const cleanup = sinon.promise<TCloseOperationResp>();
      const { context, operation } = setup();
      const fetchError = new Error('fetch failed');
      const cleanupError = new StatusError({ statusCode: TStatusCode.ERROR_STATUS, errorMessage: 'cleanup failed' });
      sinon.stub(context.driver, 'fetchResults').rejects(fetchError);
      const close = sinon.stub(context.driver, 'closeOperation').returns(cleanup);
      const log = sinon.spy(context.logger, 'log');
      let rejectedWith: unknown;
      try {
        const fetching = operation.fetchChunk().catch((error) => {
          rejectedWith = error;
        });
        await clock.runAllAsync();
        expect(rejectedWith).to.equal(fetchError);
        await fetching;
        expect(close.callCount).to.equal(1);

        let closed = false;
        const closing = operation.close().then((status) => {
          closed = true;
          return status;
        });
        await clock.runAllAsync();
        expect(closed).to.be.false;
        expect(close.callCount).to.equal(1);

        if (closeFails) {
          cleanup.reject(cleanupError);
        } else {
          cleanup.resolve(context.driver.closeOperationResp);
        }
        const status = await closing;
        expect(status.isSuccess).to.be.true;
        expect(status.info).to.deep.equal(closeFails ? ['Failed to close operation: cleanup failed'] : []);
        expect(
          log.calledWith(
            LogLevel.warn,
            sinon.match('fetch-error cleanup').and(sinon.match(operation.id)).and(sinon.match('cleanup failed')),
          ),
        ).to.equal(closeFails);
        await operation.close();
        expect(close.callCount).to.equal(1);
      } finally {
        if (cleanup.status === 'pending') {
          cleanup.resolve(context.driver.closeOperationResp);
        }
        clock.restore();
      }
    });
  }

  it('contains a throwing logger after background cleanup fails', async () => {
    const { context, operation } = setup();
    const fetchError = new Error('fetch failed');
    const loggerError = new Error('logger sink closed');
    const unhandled: unknown[] = [];
    const onUnhandled = (error: unknown) => {
      unhandled.push(error);
    };
    sinon.stub(context.driver, 'fetchResults').rejects(fetchError);
    sinon.stub(context.driver, 'closeOperation').rejects(new Error('cleanup failed'));
    const log = sinon.stub(context.logger, 'log').callsFake((level) => {
      if (level === LogLevel.warn) {
        throw loggerError;
      }
    });
    process.on('unhandledRejection', onUnhandled);
    try {
      await operation.fetchChunk().then(
        () => expect.fail('fetch should fail'),
        (error) => expect(error).to.equal(fetchError),
      );
      await new Promise<void>((resolve) => setImmediate(resolve));
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(log.calledWith(LogLevel.warn, sinon.match('fetch-error cleanup'))).to.be.true;
      expect(unhandled).to.deep.equal([]);
      await operation.fetchChunk().then(
        () => expect.fail('later reads must remain terminal'),
        (error) => expect(error).to.be.instanceOf(OperationStateError),
      );
    } finally {
      process.removeListener('unhandledRejection', onUnhandled);
    }
  });

  it('reuses an explicit close already in flight when a fetch fails', async () => {
    const clock = sinon.useFakeTimers();
    const { context, operation } = setup();
    const fetchError = new Error('operation is closing');
    const pendingFetch = sinon.promise<TFetchResultsResp>();
    const pendingClose = sinon.promise<TCloseOperationResp>();
    const fetch = sinon.stub(context.driver, 'fetchResults').returns(pendingFetch);
    const close = sinon.stub(context.driver, 'closeOperation').returns(pendingClose);
    close.onSecondCall().rejects(new Error('operation already closed'));
    const log = sinon.spy(context.logger, 'log');
    let rejectedWith: unknown;
    try {
      const fetching = operation.fetchChunk().catch((error) => {
        rejectedWith = error;
      });
      await clock.runAllAsync();
      expect(fetch.callCount).to.equal(1);
      const closing = operation.close();
      await clock.runAllAsync();
      expect(close.callCount).to.equal(1);

      pendingFetch.reject(fetchError);
      await clock.runAllAsync();
      expect(rejectedWith).to.equal(fetchError);
      await fetching;
      expect(close.callCount).to.equal(1);
      pendingClose.resolve(context.driver.closeOperationResp);
      expect((await closing).isSuccess).to.be.true;
      expect(log.calledWith(LogLevel.warn)).to.be.false;
    } finally {
      if (pendingFetch.status === 'pending') {
        pendingFetch.resolve(context.driver.fetchResultsResp);
      }
      if (pendingClose.status === 'pending') {
        pendingClose.resolve(context.driver.closeOperationResp);
      }
      clock.restore();
    }
  });

  it('allows an unfinished explicit close to be retried after failure', async () => {
    const { context, operation } = setup();
    context.driver.getOperationStatusResp.operationState = TOperationState.RUNNING_STATE;
    const closeError = new Error('close failed');
    const close = sinon.stub(context.driver, 'closeOperation').callThrough();
    close.onFirstCall().rejects(closeError);
    await operation.close().then(
      () => expect.fail('unfinished close failure should reject'),
      (error) => expect(error).to.equal(closeError),
    );
    expect((await operation.close()).isSuccess).to.be.true;
    expect(close.callCount).to.equal(2);
  });

  it('keeps the operation usable after an invalid maxRows', async () => {
    const { context, operation } = setup();
    const close = sinon.spy(context.driver, 'closeOperation');
    const fetch = sinon.spy(context.driver, 'fetchResults');

    await operation.fetchChunk({ maxRows: Number.MAX_VALUE }).then(
      () => expect.fail('maxRows should be outside the Int64 range'),
      (error) => expect(error).to.be.instanceOf(RangeError),
    );
    expect(close.called).to.be.false;
    expect(fetch.called).to.be.false;
    expect(await operation.fetchChunk({ maxRows: 3 })).to.deep.equal([{ test: 'a' }, { test: 'b' }, { test: 'c' }]);
  });

  for (const maxRows of [Infinity, Number.MAX_VALUE]) {
    for (const method of ['fetchChunk', 'fetchAll'] as const) {
      it(`preserves direct-result ${method} with maxRows=${maxRows}`, async () => {
        const { context, operation } = setup(true);
        const fetch = sinon.spy(context.driver, 'fetchResults');
        expect(await operation[method]({ maxRows })).to.deep.equal([{ test: 'a' }, { test: 'b' }, { test: 'c' }]);
        expect(fetch.called).to.be.false;
      });
    }
  }

  it('fails after consuming an Arrow batch when trailing prefetch rejects', async () => {
    const { context, operation } = setup();
    const table = new Table([
      ...tableFromArrays({ id: new Int32Array([0, 1]) }).batches,
      ...tableFromArrays({ id: new Int32Array([2, 3]) }).batches,
    ]);
    const ipc = Buffer.from(tableToIPC(table, 'stream'));
    const schemaLength = 8 + ipc.readInt32LE(4);
    context.driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.ARROW_BASED_SET;
    context.driver.getResultSetMetadataResp.arrowSchema = ipc.subarray(0, schemaLength);
    context.driver.getResultSetMetadataResp.schema = {
      columns: [
        {
          columnName: 'id',
          typeDesc: { types: [{ primitiveEntry: { type: TTypeId.INT_TYPE } }] },
          position: 1,
          comment: '',
        },
      ],
    };
    const fetchError = new Error('trailing prefetch failed');
    const fetch = sinon.stub(context.driver, 'fetchResults').resolves({
      status: context.driver.fetchResultsResp.status,
      hasMoreRows: false,
    });
    fetch.onFirstCall().resolves({
      status: context.driver.fetchResultsResp.status,
      hasMoreRows: true,
      results: {
        startRowOffset: new Int64(0),
        rows: [],
        arrowBatches: [{ batch: ipc.subarray(schemaLength), rowCount: new Int64(4) }],
      },
    });
    fetch.onSecondCall().rejects(fetchError);
    const close = sinon.spy(context.driver, 'closeOperation');

    expect(await operation.fetchChunk({ maxRows: 2 })).to.deep.equal([{ id: 0 }, { id: 1 }]);
    expect(fetch.callCount).to.equal(1);
    await operation.fetchChunk({ maxRows: 2 }).then(
      () => expect.fail('trailing prefetch should fail'),
      (error) => expect(error).to.equal(fetchError),
    );
    for (const read of [() => operation.fetchChunk(), () => operation.fetchAll(), () => operation.hasMoreRows()]) {
      await read().then(
        () => expect.fail('later reads must not resume the consumed Arrow cursor'),
        (error) => {
          expect(error).to.be.instanceOf(OperationStateError);
          expect(error.errorCode).to.equal(OperationStateErrorCode.Error);
        },
      );
    }
    expect(fetch.callCount).to.equal(2);
    expect(close.callCount).to.equal(1);
  });

  it('fails after a CloudFetch provider error during fetchAll', async () => {
    const { context, operation } = setup();
    context.driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.URL_BASED_SET;
    const close = sinon.spy(context.driver, 'closeOperation');
    const downloadError = new RangeError('invalid downloaded Arrow data');
    const download = sinon.stub(CloudFetchResultHandler.prototype, 'fetchNext').rejects(downloadError);
    try {
      await operation.fetchAll().then(
        () => expect.fail('fetchAll should fail'),
        (error) => expect(error).to.equal(downloadError),
      );
      await operation.fetchChunk().then(
        () => expect.fail('later reads must fail'),
        (error) => {
          expect(error).to.be.instanceOf(OperationStateError);
          expect(error.errorCode).to.equal(OperationStateErrorCode.Error);
        },
      );
      expect(close.callCount).to.equal(1);
      expect(download.callCount).to.equal(1);
    } finally {
      download.restore();
    }
  });

  it('keeps the operation usable after a progress callback throws', async () => {
    const { context, operation } = setup();
    context.driver.getOperationStatusResp.operationState = TOperationState.RUNNING_STATE;
    const close = sinon.spy(context.driver, 'closeOperation');
    const callbackError = new Error('user callback failed');

    await operation
      .fetchChunk({
        callback: () => {
          throw callbackError;
        },
      })
      .then(
        () => expect.fail('callback should fail'),
        (error) => expect(error).to.equal(callbackError),
      );
    expect(close.called).to.be.false;
    context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
    expect(await operation.fetchChunk({ maxRows: 3 })).to.have.length(3);
  });
});
