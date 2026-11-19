import { expect } from 'chai';
import sinon from 'sinon';
import { TOperationState, TOperationType, TSparkRowSetType } from '../../../thrift/TCLIService_types';
import OperationStateError, { OperationStateErrorCode } from '../../../lib/errors/OperationStateError';
import CloudFetchResultHandler from '../../../lib/result/CloudFetchResultHandler';
import ClientContextStub from '../.stubs/ClientContextStub';
import { createOperationForTest } from '../.stubs/createOperationForTest';

describe('Thrift fetch failure', () => {
  function setup() {
    const context = new ClientContextStub();
    context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
    const operation = createOperationForTest({
      context,
      handle: {
        operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
        operationType: TOperationType.EXECUTE_STATEMENT,
        hasResultSet: true,
      },
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
  }

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

  it('does not consume buffered rows when maxRows is invalid', async () => {
    const { context, operation } = setup();
    const close = sinon.spy(context.driver, 'closeOperation');
    expect(await operation.fetchChunk({ maxRows: 1 })).to.deep.equal([{ test: 'a' }]);
    await operation.fetchChunk({ maxRows: Number.MAX_VALUE }).then(
      () => expect.fail('maxRows should be outside the Int64 range'),
      (error) => expect(error).to.be.instanceOf(RangeError),
    );
    expect(close.called).to.be.false;
    expect(await operation.fetchChunk({ maxRows: 2 })).to.deep.equal([{ test: 'b' }, { test: 'c' }]);
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
