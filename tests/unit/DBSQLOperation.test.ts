import { AssertionError, expect } from 'chai';
import sinon from 'sinon';
import Int64 from 'node-int64';
import {
  TOperationHandle,
  TOperationState,
  TOperationType,
  TSparkDirectResults,
  TSparkRowSetType,
  TStatusCode,
  TTypeId,
} from '../../thrift/TCLIService_types';
import DBSQLOperation from '../../lib/DBSQLOperation';
import StatusError from '../../lib/errors/StatusError';
import OperationStateError from '../../lib/errors/OperationStateError';
import HiveDriverError from '../../lib/errors/HiveDriverError';
import JsonResultHandler from '../../lib/result/JsonResultHandler';
import ArrowResultConverter from '../../lib/result/ArrowResultConverter';
import ArrowResultHandler from '../../lib/result/ArrowResultHandler';
import CloudFetchResultHandler from '../../lib/result/CloudFetchResultHandler';
import ResultSlicer from '../../lib/result/ResultSlicer';

import ClientContextStub from './.stubs/ClientContextStub';
import { Type } from 'apache-arrow';

function operationHandleStub(overrides: Partial<TOperationHandle>): TOperationHandle {
  return {
    operationId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) },
    operationType: TOperationType.EXECUTE_STATEMENT,
    hasResultSet: true,
    ...overrides,
  };
}

async function expectFailure(fn: () => Promise<unknown>) {
  try {
    await fn();
    expect.fail('It should throw an error');
  } catch (error) {
    if (error instanceof AssertionError) {
      throw error;
    }
  }
}

describe('DBSQLOperation', () => {
  describe('status', () => {
    it('should pick up state from operation handle', async () => {
      const context = new ClientContextStub();
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(operation['state']).to.equal(TOperationState.INITIALIZED_STATE);
      expect(operation['operationHandle'].hasResultSet).to.be.true;
    });

    it('should pick up state from directResults', async () => {
      const context = new ClientContextStub();
      const operation = new DBSQLOperation({
        handle: operationHandleStub({ hasResultSet: true }),
        context,
        directResults: {
          operationStatus: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
            operationState: TOperationState.FINISHED_STATE,
            hasResultSet: true,
          },
        },
      });

      expect(operation['state']).to.equal(TOperationState.FINISHED_STATE);
      expect(operation['operationHandle'].hasResultSet).to.be.true;
    });

    it('should fetch status and update internal state', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: false }), context });

      expect(operation['state']).to.equal(TOperationState.INITIALIZED_STATE);
      expect(operation['operationHandle'].hasResultSet).to.be.false;

      const status = await operation.status();

      expect(driver.getOperationStatus.called).to.be.true;
      expect(status.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation['state']).to.equal(TOperationState.FINISHED_STATE);
      expect(operation['operationHandle'].hasResultSet).to.be.true;
    });

    it('should request progress', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: false }), context });
      await operation.status(true);

      expect(driver.getOperationStatus.called).to.be.true;
      const request = driver.getOperationStatus.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should not fetch status once operation is finished', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: false }), context });

      expect(operation['state']).to.equal(TOperationState.INITIALIZED_STATE);
      expect(operation['operationHandle'].hasResultSet).to.be.false;

      // First call - should fetch data and cache
      driver.getOperationStatusResp = {
        ...driver.getOperationStatusResp,
        operationState: TOperationState.FINISHED_STATE,
      };
      const status1 = await operation.status();

      expect(driver.getOperationStatus.callCount).to.equal(1);
      expect(status1.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation['state']).to.equal(TOperationState.FINISHED_STATE);
      expect(operation['operationHandle'].hasResultSet).to.be.true;

      // Second call - should return cached data
      driver.getOperationStatusResp = {
        ...driver.getOperationStatusResp,
        operationState: TOperationState.RUNNING_STATE,
      };
      const status2 = await operation.status();

      expect(driver.getOperationStatus.callCount).to.equal(1);
      expect(status2.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation['state']).to.equal(TOperationState.FINISHED_STATE);
      expect(operation['operationHandle'].hasResultSet).to.be.true;
    });

    it('should fetch status if directResults status is not finished', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({
        handle: operationHandleStub({ hasResultSet: false }),
        context,
        directResults: {
          operationStatus: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
            operationState: TOperationState.RUNNING_STATE,
            hasResultSet: false,
          },
        },
      });

      expect(operation['state']).to.equal(TOperationState.RUNNING_STATE); // from directResults
      expect(operation['operationHandle'].hasResultSet).to.be.false;

      const status = await operation.status(false);

      expect(driver.getOperationStatus.called).to.be.true;
      expect(status.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation['state']).to.equal(TOperationState.FINISHED_STATE);
      expect(operation['operationHandle'].hasResultSet).to.be.true;
    });

    it('should not fetch status if directResults status is finished', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.operationState = TOperationState.RUNNING_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({
        handle: operationHandleStub({ hasResultSet: false }),
        context,
        directResults: {
          operationStatus: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
            operationState: TOperationState.FINISHED_STATE,
            hasResultSet: false,
          },
        },
      });

      expect(operation['state']).to.equal(TOperationState.FINISHED_STATE); // from directResults
      expect(operation['operationHandle'].hasResultSet).to.be.false;

      const status = await operation.status(false);

      expect(driver.getOperationStatus.called).to.be.false;
      expect(status.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation['state']).to.equal(TOperationState.FINISHED_STATE);
      expect(operation['operationHandle'].hasResultSet).to.be.false;
    });

    it('should throw an error in case of a status error', async () => {
      const context = new ClientContextStub();
      context.driver.getOperationStatusResp.status.statusCode = TStatusCode.ERROR_STATUS;
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      try {
        await operation.status(false);
        expect.fail('It should throw a StatusError');
      } catch (e) {
        if (e instanceof AssertionError) {
          throw e;
        }
        expect(e).to.be.instanceOf(StatusError);
      }
    });
  });

  describe('cancel', () => {
    it('should cancel operation and update state', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.false;

      await operation.cancel();

      expect(driver.cancelOperation.called).to.be.true;
      expect(operation['cancelled']).to.be.true;
      expect(operation['closed']).to.be.false;
    });

    it('should return immediately if already cancelled', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.false;

      await operation.cancel();
      expect(driver.cancelOperation.callCount).to.be.equal(1);
      expect(operation['cancelled']).to.be.true;
      expect(operation['closed']).to.be.false;

      await operation.cancel();
      expect(driver.cancelOperation.callCount).to.be.equal(1);
      expect(operation['cancelled']).to.be.true;
      expect(operation['closed']).to.be.false;
    });

    it('should return immediately if already closed', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.false;

      await operation.close();
      expect(driver.closeOperation.callCount).to.be.equal(1);
      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.true;

      await operation.cancel();
      expect(driver.cancelOperation.callCount).to.be.equal(0);
      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.true;
    });

    it('should throw an error in case of a status error and keep state', async () => {
      const context = new ClientContextStub();
      context.driver.cancelOperationResp.status.statusCode = TStatusCode.ERROR_STATUS;
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.false;

      try {
        await operation.cancel();
        expect.fail('It should throw a StatusError');
      } catch (e) {
        if (e instanceof AssertionError) {
          throw e;
        }
        expect(e).to.be.instanceOf(StatusError);
        expect(operation['cancelled']).to.be.false;
        expect(operation['closed']).to.be.false;
      }
    });

    it('should reject all methods once cancelled', async () => {
      const context = new ClientContextStub();
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      await operation.cancel();
      expect(operation['cancelled']).to.be.true;

      await expectFailure(() => operation.fetchAll());
      await expectFailure(() => operation.fetchChunk({ disableBuffering: true }));
      await expectFailure(() => operation.status());
      await expectFailure(() => operation.finished());
      await expectFailure(() => operation.getSchema());
    });
  });

  describe('close', () => {
    it('should close operation and update state', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.false;

      await operation.close();

      expect(driver.closeOperation.called).to.be.true;
      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.true;
    });

    it('should return immediately if already closed', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.false;

      await operation.close();
      expect(driver.closeOperation.callCount).to.be.equal(1);
      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.true;

      await operation.close();
      expect(driver.closeOperation.callCount).to.be.equal(1);
      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.true;
    });

    it('should return immediately if already cancelled', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.false;

      await operation.cancel();
      expect(driver.cancelOperation.callCount).to.be.equal(1);
      expect(operation['cancelled']).to.be.true;
      expect(operation['closed']).to.be.false;

      await operation.close();
      expect(driver.closeOperation.callCount).to.be.equal(0);
      expect(operation['cancelled']).to.be.true;
      expect(operation['closed']).to.be.false;
    });

    it('should initialize from directResults', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);

      const operation = new DBSQLOperation({
        handle: operationHandleStub({ hasResultSet: true }),
        context,
        directResults: {
          closeOperation: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
          },
        },
      });

      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.false;

      await operation.close();

      expect(driver.closeOperation.called).to.be.false;
      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.true;
      expect(driver.closeOperation.callCount).to.be.equal(0);
    });

    it('should throw an error in case of a status error and keep state', async () => {
      const context = new ClientContextStub();
      context.driver.closeOperationResp.status.statusCode = TStatusCode.ERROR_STATUS;
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(operation['cancelled']).to.be.false;
      expect(operation['closed']).to.be.false;

      try {
        await operation.close();
        expect.fail('It should throw a StatusError');
      } catch (e) {
        if (e instanceof AssertionError) {
          throw e;
        }
        expect(e).to.be.instanceOf(StatusError);
        expect(operation['cancelled']).to.be.false;
        expect(operation['closed']).to.be.false;
      }
    });

    it('should reject all methods once closed', async () => {
      const context = new ClientContextStub();
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      await operation.close();
      expect(operation['closed']).to.be.true;

      await expectFailure(() => operation.fetchAll());
      await expectFailure(() => operation.fetchChunk({ disableBuffering: true }));
      await expectFailure(() => operation.status());
      await expectFailure(() => operation.finished());
      await expectFailure(() => operation.getSchema());
    });
  });

  describe('finished', () => {
    [TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE, TOperationState.PENDING_STATE].forEach(
      (operationState) => {
        it(`should wait for finished state starting from TOperationState.${TOperationState[operationState]}`, async () => {
          const attemptsUntilFinished = 3;

          const context = new ClientContextStub();

          context.driver.getOperationStatusResp.operationState = operationState;
          const getOperationStatusStub = sinon.stub(context.driver, 'getOperationStatus');

          getOperationStatusStub
            .callThrough()
            .onCall(attemptsUntilFinished - 1) // count is zero-based
            .callsFake((...args) => {
              context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
              return getOperationStatusStub.wrappedMethod.apply(context.driver, args);
            });

          const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

          expect(operation['state']).to.equal(TOperationState.INITIALIZED_STATE);

          await operation.finished();

          expect(getOperationStatusStub.callCount).to.be.equal(attemptsUntilFinished);
          expect(operation['state']).to.equal(TOperationState.FINISHED_STATE);
        });
      },
    );

    it('should request progress', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      const getOperationStatusStub = sinon.stub(context.driver, 'getOperationStatus');

      getOperationStatusStub
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return getOperationStatusStub.wrappedMethod.apply(context.driver, args);
        });

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });
      await operation.finished({ progress: true });

      expect(getOperationStatusStub.called).to.be.true;
      const request = getOperationStatusStub.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should invoke progress callback', async () => {
      const attemptsUntilFinished = 3;

      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      const getOperationStatusStub = sinon.stub(context.driver, 'getOperationStatus');

      getOperationStatusStub
        .callThrough()
        .onCall(attemptsUntilFinished - 1) // count is zero-based
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return getOperationStatusStub.wrappedMethod.apply(context.driver, args);
        });

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      const callback = sinon.stub();

      await operation.finished({ callback });

      expect(getOperationStatusStub.called).to.be.true;
      expect(callback.callCount).to.be.equal(attemptsUntilFinished);
    });

    it('should pick up finished state from directResults', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.status.statusCode = TStatusCode.SUCCESS_STATUS;
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;

      const operation = new DBSQLOperation({
        handle: operationHandleStub({ hasResultSet: true }),
        context,
        directResults: {
          operationStatus: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
            operationState: TOperationState.FINISHED_STATE,
            hasResultSet: true,
          },
        },
      });

      await operation.finished();

      // Once operation is finished - no need to fetch status again
      expect(driver.getOperationStatus.called).to.be.false;
    });

    it('should throw an error in case of a status error', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.status.statusCode = TStatusCode.ERROR_STATUS;
      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      try {
        await operation.finished();
        expect.fail('It should throw a StatusError');
      } catch (e) {
        if (e instanceof AssertionError) {
          throw e;
        }
        expect(e).to.be.instanceOf(StatusError);
      }
    });

    [
      TOperationState.CANCELED_STATE,
      TOperationState.CLOSED_STATE,
      TOperationState.ERROR_STATE,
      TOperationState.UKNOWN_STATE,
      TOperationState.TIMEDOUT_STATE,
    ].forEach((operationState) => {
      it(`should throw an error in case of a TOperationState.${TOperationState[operationState]}`, async () => {
        const context = new ClientContextStub();

        context.driver.getOperationStatusResp.status.statusCode = TStatusCode.SUCCESS_STATUS;
        context.driver.getOperationStatusResp.operationState = operationState;
        const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

        try {
          await operation.finished();
          expect.fail('It should throw a OperationStateError');
        } catch (e) {
          if (e instanceof AssertionError) {
            throw e;
          }
          expect(e).to.be.instanceOf(OperationStateError);
        }
      });
    });
  });

  describe('getSchema', () => {
    it('should return immediately if operation has no results', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = false;

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: false }), context });

      const schema = await operation.getSchema();

      expect(schema).to.be.null;
      expect(driver.getResultSetMetadata.called).to.be.false;
    });

    it('should wait for operation to complete', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      const getOperationStatusStub = sinon.stub(context.driver, 'getOperationStatus');

      getOperationStatusStub
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return getOperationStatusStub.wrappedMethod.apply(context.driver, args);
        });

      context.driver.getResultSetMetadataResp.schema = { columns: [] };

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      const schema = await operation.getSchema();

      expect(getOperationStatusStub.called).to.be.true;
      expect(schema).to.deep.equal(context.driver.getResultSetMetadataResp.schema);
      expect(operation['state']).to.equal(TOperationState.FINISHED_STATE);
    });

    it('should request progress', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      const getOperationStatusStub = sinon.stub(context.driver, 'getOperationStatus');

      getOperationStatusStub
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return getOperationStatusStub.wrappedMethod.apply(context.driver, args);
        });

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });
      await operation.getSchema({ progress: true });

      expect(getOperationStatusStub.called).to.be.true;
      const request = getOperationStatusStub.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should invoke progress callback', async () => {
      const attemptsUntilFinished = 3;

      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      const getOperationStatusStub = sinon.stub(context.driver, 'getOperationStatus');

      getOperationStatusStub
        .callThrough()
        .onCall(attemptsUntilFinished - 1) // count is zero-based
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return getOperationStatusStub.wrappedMethod.apply(context.driver, args);
        });

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      const callback = sinon.stub();

      await operation.getSchema({ callback });

      expect(getOperationStatusStub.called).to.be.true;
      expect(callback.callCount).to.be.equal(attemptsUntilFinished);
    });

    it('should fetch schema if operation has data', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      const schema = await operation.getSchema();
      expect(schema).to.deep.equal(driver.getResultSetMetadataResp.schema);
      expect(driver.getResultSetMetadata.called).to.be.true;
    });

    it('should return cached schema on subsequent calls', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      const schema1 = await operation.getSchema();
      expect(schema1).to.deep.equal(context.driver.getResultSetMetadataResp.schema);
      expect(driver.getResultSetMetadata.callCount).to.equal(1);

      const schema2 = await operation.getSchema();
      expect(schema2).to.deep.equal(context.driver.getResultSetMetadataResp.schema);
      expect(driver.getResultSetMetadata.callCount).to.equal(1); // no additional requests
    });

    it('should use schema from directResults', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      const directResults: TSparkDirectResults = {
        resultSetMetadata: {
          status: { statusCode: TStatusCode.SUCCESS_STATUS },
          schema: {
            columns: [
              {
                columnName: 'another',
                position: 0,
                typeDesc: {
                  types: [
                    {
                      primitiveEntry: { type: TTypeId.STRING_TYPE },
                    },
                  ],
                },
              },
            ],
          },
        },
      };
      const operation = new DBSQLOperation({
        handle: operationHandleStub({ hasResultSet: true }),
        context,
        directResults,
      });

      const schema = await operation.getSchema();

      expect(schema).to.deep.equal(directResults.resultSetMetadata?.schema);
      expect(driver.getResultSetMetadata.called).to.be.false;
    });

    it('should throw an error in case of a status error', async () => {
      const context = new ClientContextStub();
      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.getResultSetMetadataResp.status.statusCode = TStatusCode.ERROR_STATUS;

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      try {
        await operation.getSchema();
        expect.fail('It should throw a StatusError');
      } catch (e) {
        if (e instanceof AssertionError) {
          throw e;
        }
        expect(e).to.be.instanceOf(StatusError);
      }
    });

    it('should use appropriate result handler', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      jsonHandler: {
        driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.COLUMN_BASED_SET;
        driver.getResultSetMetadata.resetHistory();

        const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });
        const resultHandler = await operation['getResultHandler']();
        expect(driver.getResultSetMetadata.called).to.be.true;
        expect(resultHandler).to.be.instanceOf(ResultSlicer);
        expect(resultHandler['source']).to.be.instanceOf(JsonResultHandler);
      }

      arrowHandler: {
        driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.ARROW_BASED_SET;
        driver.getResultSetMetadata.resetHistory();

        const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });
        const resultHandler = await operation['getResultHandler']();
        expect(driver.getResultSetMetadata.called).to.be.true;
        expect(resultHandler).to.be.instanceOf(ResultSlicer);
        expect(resultHandler['source']).to.be.instanceOf(ArrowResultConverter);
        if (!(resultHandler['source'] instanceof ArrowResultConverter)) {
          throw new Error('Expected `resultHandler.source` to be `ArrowResultConverter`');
        }
        expect(resultHandler['source']['source']).to.be.instanceOf(ArrowResultHandler);
      }

      cloudFetchHandler: {
        driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.URL_BASED_SET;
        driver.getResultSetMetadata.resetHistory();

        const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });
        const resultHandler = await operation['getResultHandler']();
        expect(driver.getResultSetMetadata.called).to.be.true;
        expect(resultHandler).to.be.instanceOf(ResultSlicer);
        expect(resultHandler['source']).to.be.instanceOf(ArrowResultConverter);
        if (!(resultHandler['source'] instanceof ArrowResultConverter)) {
          throw new Error('Expected `resultHandler.source` to be `ArrowResultConverter`');
        }
        expect(resultHandler['source']['source']).to.be.instanceOf(CloudFetchResultHandler);
      }
    });
  });

  describe('fetchChunk', () => {
    it('should return immediately if operation has no results', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: false }), context });

      const results = await operation.fetchChunk({ disableBuffering: true });

      expect(results).to.deep.equal([]);
      expect(driver.getResultSetMetadata.called).to.be.false;
      expect(driver.fetchResults.called).to.be.false;
    });

    it('should wait for operation to complete', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      const getOperationStatusStub = sinon.stub(context.driver, 'getOperationStatus');

      getOperationStatusStub
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return getOperationStatusStub.wrappedMethod.apply(context.driver, args);
        });

      context.driver.getResultSetMetadataResp.schema = { columns: [] };
      context.driver.fetchResultsResp.hasMoreRows = false;
      context.driver.fetchResultsResp.results!.columns = [];

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      const results = await operation.fetchChunk({ disableBuffering: true });

      expect(getOperationStatusStub.called).to.be.true;
      expect(results).to.deep.equal([]);
      expect(operation['state']).to.equal(TOperationState.FINISHED_STATE);
    });

    it('should request progress', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      const getOperationStatusStub = sinon.stub(context.driver, 'getOperationStatus');

      getOperationStatusStub
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return getOperationStatusStub.wrappedMethod.apply(context.driver, args);
        });

      context.driver.getResultSetMetadataResp.schema = { columns: [] };
      context.driver.fetchResultsResp.hasMoreRows = false;
      context.driver.fetchResultsResp.results!.columns = [];

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });
      await operation.fetchChunk({ progress: true, disableBuffering: true });

      expect(getOperationStatusStub.called).to.be.true;
      const request = getOperationStatusStub.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should invoke progress callback', async () => {
      const attemptsUntilFinished = 3;

      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      const getOperationStatusStub = sinon.stub(context.driver, 'getOperationStatus');

      getOperationStatusStub
        .callThrough()
        .onCall(attemptsUntilFinished - 1) // count is zero-based
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return getOperationStatusStub.wrappedMethod.apply(context.driver, args);
        });

      context.driver.getResultSetMetadataResp.schema = { columns: [] };
      context.driver.fetchResultsResp.hasMoreRows = false;
      context.driver.fetchResultsResp.results!.columns = [];

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      const callback = sinon.stub();

      await operation.fetchChunk({ callback, disableBuffering: true });

      expect(getOperationStatusStub.called).to.be.true;
      expect(callback.callCount).to.be.equal(attemptsUntilFinished);
    });

    it('should fetch schema and data and return array of records', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      const results = await operation.fetchChunk({ disableBuffering: true });

      expect(results).to.deep.equal([{ test: 'a' }, { test: 'b' }, { test: 'c' }]);
      expect(driver.getResultSetMetadata.called).to.be.true;
      expect(driver.fetchResults.called).to.be.true;
    });

    it('should return data from directResults (all the data in directResults)', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;

      const operation = new DBSQLOperation({
        handle: operationHandleStub({ hasResultSet: true }),
        context,
        directResults: {
          resultSet: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
            hasMoreRows: false,
            results: {
              startRowOffset: new Int64(0),
              rows: [],
              columns: [
                {
                  stringVal: {
                    values: ['a', 'b'],
                    nulls: Buffer.from([]),
                  },
                },
              ],
            },
          },
        },
      });

      const results = await operation.fetchChunk({ disableBuffering: true });

      expect(results).to.deep.equal([{ test: 'a' }, { test: 'b' }]);
      expect(driver.getResultSetMetadata.called).to.be.true;
      expect(driver.fetchResults.called).to.be.false;
    });

    it('should return data from directResults (first chunk in directResults, next chunk fetched)', async () => {
      const context = new ClientContextStub();
      const driver = sinon.spy(context.driver);
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({
        handle: operationHandleStub({ hasResultSet: true }),
        context,
        directResults: {
          resultSet: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
            hasMoreRows: true,
            results: {
              startRowOffset: new Int64(0),
              rows: [],
              columns: [
                {
                  stringVal: {
                    values: ['q', 'w'],
                    nulls: Buffer.from([]),
                  },
                },
              ],
            },
          },
        },
      });

      const results1 = await operation.fetchChunk({ disableBuffering: true });

      expect(results1).to.deep.equal([{ test: 'q' }, { test: 'w' }]);
      expect(driver.getResultSetMetadata.callCount).to.be.eq(1);
      expect(driver.fetchResults.callCount).to.be.eq(0);

      const results2 = await operation.fetchChunk({ disableBuffering: true });

      expect(results2).to.deep.equal([{ test: 'a' }, { test: 'b' }, { test: 'c' }]);
      expect(driver.getResultSetMetadata.callCount).to.be.eq(1);
      expect(driver.fetchResults.callCount).to.be.eq(1);
    });

    it('should fail on unsupported result format', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.ROW_BASED_SET;
      context.driver.getResultSetMetadataResp.schema = { columns: [] };

      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      try {
        await operation.fetchChunk({ disableBuffering: true });
        expect.fail('It should throw a HiveDriverError');
      } catch (e) {
        if (e instanceof AssertionError) {
          throw e;
        }
        expect(e).to.be.instanceOf(HiveDriverError);
      }
    });
  });

  describe('fetchAll', () => {
    it('should fetch data while available and return it all', async () => {
      const context = new ClientContextStub();
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      const originalData = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0];

      const tempData = [...originalData];
      const fetchChunkStub = sinon.stub(operation, 'fetchChunk').callsFake(async (): Promise<Array<any>> => {
        return tempData.splice(0, 3);
      });
      const hasMoreRowsStub = sinon.stub(operation, 'hasMoreRows').callsFake(async () => {
        return tempData.length > 0;
      });

      const fetchedData = await operation.fetchAll();

      // Warning: this check is implementation-specific
      // `fetchAll` should wait for operation to complete. In current implementation
      // it does so by calling `fetchChunk` at least once, which internally does
      // all the job. But since here we stub `fetchChunk` it won't really wait,
      // therefore here we ensure it was called at least once
      expect(fetchChunkStub.callCount).to.be.gte(1);

      expect(fetchChunkStub.called).to.be.true;
      expect(hasMoreRowsStub.called).to.be.true;
      expect(fetchedData).to.deep.equal(originalData);
    });
  });

  describe('hasMoreRows', () => {
    it('should return initial value prior to first fetch', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = false;
      context.driver.fetchResultsResp.results = undefined;
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation['_data']['hasMoreRowsFlag']).to.be.undefined;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.false;
      expect(operation['_data']['hasMoreRowsFlag']).to.be.false;
    });

    it('should return False if operation was closed', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = true;
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(await operation.hasMoreRows()).to.be.true;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.true;
      await operation.close();
      expect(await operation.hasMoreRows()).to.be.false;
    });

    it('should return False if operation was cancelled', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = true;
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(await operation.hasMoreRows()).to.be.true;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.true;
      await operation.cancel();
      expect(await operation.hasMoreRows()).to.be.false;
    });

    it('should return True if hasMoreRows flag was set in response', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = true;
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation['_data']['hasMoreRowsFlag']).to.be.undefined;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation['_data']['hasMoreRowsFlag']).to.be.true;
    });

    it('should return True if hasMoreRows flag is False but there is actual data', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = false;
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation['_data']['hasMoreRowsFlag']).to.be.undefined;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation['_data']['hasMoreRowsFlag']).to.be.true;
    });

    it('should return True if hasMoreRows flag is unset but there is actual data', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = undefined;
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation['_data']['hasMoreRowsFlag']).to.be.undefined;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation['_data']['hasMoreRowsFlag']).to.be.true;
    });

    it('should return False if hasMoreRows flag is False and there is no data', async () => {
      const context = new ClientContextStub();

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = false;
      context.driver.fetchResultsResp.results = undefined;
      const operation = new DBSQLOperation({ handle: operationHandleStub({ hasResultSet: true }), context });

      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation['_data']['hasMoreRowsFlag']).to.be.undefined;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.false;
      expect(operation['_data']['hasMoreRowsFlag']).to.be.false;
    });
  });
});
