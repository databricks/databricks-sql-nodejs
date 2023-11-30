const { expect, AssertionError } = require('chai');
const sinon = require('sinon');
const { DBSQLLogger, LogLevel } = require('../../dist');
const { TStatusCode, TOperationState, TTypeId, TSparkRowSetType } = require('../../thrift/TCLIService_types');
const DBSQLOperation = require('../../dist/DBSQLOperation').default;
const StatusError = require('../../dist/errors/StatusError').default;
const OperationStateError = require('../../dist/errors/OperationStateError').default;
const HiveDriverError = require('../../dist/errors/HiveDriverError').default;
const JsonResultHandler = require('../../dist/result/JsonResultHandler').default;
const ArrowResultHandler = require('../../dist/result/ArrowResultHandler').default;
const CloudFetchResultHandler = require('../../dist/result/CloudFetchResultHandler').default;
const ResultSlicer = require('../../dist/result/ResultSlicer').default;

class OperationHandleMock {
  constructor(hasResultSet = true) {
    this.operationId = 1;
    this.hasResultSet = !!hasResultSet;
  }
}

async function expectFailure(fn) {
  try {
    await fn();
    expect.fail('It should throw an error');
  } catch (error) {
    if (error instanceof AssertionError) {
      throw error;
    }
  }
}

class DriverMock {
  getOperationStatusResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    operationState: TOperationState.INITIALIZED_STATE,
    hasResultSet: false,
  };

  getResultSetMetadataResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    resultFormat: TSparkRowSetType.COLUMN_BASED_SET,
    schema: {
      columns: [
        {
          columnName: 'test',
          position: 1,
          typeDesc: {
            types: [
              {
                primitiveEntry: {
                  type: TTypeId.INT_TYPE,
                },
              },
            ],
          },
        },
      ],
    },
  };

  fetchResultsResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
    hasMoreRows: false,
    results: {
      columns: [
        {
          i32Val: {
            values: [1, 2, 3],
          },
        },
      ],
    },
  };

  cancelOperationResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  closeOperationResp = {
    status: { statusCode: TStatusCode.SUCCESS_STATUS },
  };

  getOperationStatus() {
    return Promise.resolve(this.getOperationStatusResp);
  }

  getResultSetMetadata() {
    return Promise.resolve(this.getResultSetMetadataResp);
  }

  fetchResults() {
    return Promise.resolve(this.fetchResultsResp);
  }

  cancelOperation() {
    return Promise.resolve(this.cancelOperationResp);
  }

  closeOperation() {
    return Promise.resolve(this.closeOperationResp);
  }
}

class ClientContextMock {
  constructor(props) {
    // Create logger that won't emit
    this.logger = new DBSQLLogger({ level: LogLevel.error });
    this.driver = new DriverMock();
  }

  getLogger() {
    return this.logger;
  }

  async getDriver() {
    return this.driver;
  }
}

describe('DBSQLOperation', () => {
  describe('status', () => {
    it('should pick up state from operation handle', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const operation = new DBSQLOperation({ handle, context });

      expect(operation.state).to.equal(TOperationState.INITIALIZED_STATE);
      expect(operation.operationHandle.hasResultSet).to.be.true;
    });

    it('should pick up state from directResults', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      const operation = new DBSQLOperation({
        handle,
        context,
        directResults: {
          operationStatus: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
            operationState: TOperationState.FINISHED_STATE,
            hasResultSet: true,
          },
        },
      });

      expect(operation.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation.operationHandle.hasResultSet).to.be.true;
    });

    it('should fetch status and update internal state', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      sinon.spy(context.driver, 'getOperationStatus');
      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({ handle, context });

      expect(operation.state).to.equal(TOperationState.INITIALIZED_STATE);
      expect(operation.operationHandle.hasResultSet).to.be.false;

      const status = await operation.status();

      expect(context.driver.getOperationStatus.called).to.be.true;
      expect(status.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation.operationHandle.hasResultSet).to.be.true;
    });

    it('should request progress', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      sinon.spy(context.driver, 'getOperationStatus');
      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;

      const operation = new DBSQLOperation({ handle, context });
      await operation.status(true);

      expect(context.driver.getOperationStatus.called).to.be.true;
      const request = context.driver.getOperationStatus.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should not fetch status once operation is finished', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      sinon.spy(context.driver, 'getOperationStatus');
      context.driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({ handle, context });

      expect(operation.state).to.equal(TOperationState.INITIALIZED_STATE);
      expect(operation.operationHandle.hasResultSet).to.be.false;

      // First call - should fetch data and cache
      context.driver.getOperationStatusResp = {
        ...context.driver.getOperationStatusResp,
        operationState: TOperationState.FINISHED_STATE,
      };
      const status1 = await operation.status();

      expect(context.driver.getOperationStatus.callCount).to.equal(1);
      expect(status1.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation.operationHandle.hasResultSet).to.be.true;

      // Second call - should return cached data
      context.driver.getOperationStatusResp = {
        ...context.driver.getOperationStatusResp,
        operationState: TOperationState.RUNNING_STATE,
      };
      const status2 = await operation.status();

      expect(context.driver.getOperationStatus.callCount).to.equal(1);
      expect(status2.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation.operationHandle.hasResultSet).to.be.true;
    });

    it('should fetch status if directResults status is not finished', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      sinon.spy(context.driver, 'getOperationStatus');
      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({
        handle,
        context,
        directResults: {
          operationStatus: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
            operationState: TOperationState.RUNNING_STATE,
            hasResultSet: false,
          },
        },
      });

      expect(operation.state).to.equal(TOperationState.RUNNING_STATE); // from directResults
      expect(operation.operationHandle.hasResultSet).to.be.false;

      const status = await operation.status(false);

      expect(context.driver.getOperationStatus.called).to.be.true;
      expect(status.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation.operationHandle.hasResultSet).to.be.true;
    });

    it('should not fetch status if directResults status is finished', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      sinon.spy(context.driver, 'getOperationStatus');
      context.driver.getOperationStatusResp.operationState = TOperationState.RUNNING_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation({
        handle,
        context,
        directResults: {
          operationStatus: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
            operationState: TOperationState.FINISHED_STATE,
            hasResultSet: false,
          },
        },
      });

      expect(operation.state).to.equal(TOperationState.FINISHED_STATE); // from directResults
      expect(operation.operationHandle.hasResultSet).to.be.false;

      const status = await operation.status(false);

      expect(context.driver.getOperationStatus.called).to.be.false;
      expect(status.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation.operationHandle.hasResultSet).to.be.false;
    });

    it('should throw an error in case of a status error', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.getOperationStatusResp.status.statusCode = TStatusCode.ERROR_STATUS;
      const operation = new DBSQLOperation({ handle, context });

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
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      sinon.spy(context.driver, 'cancelOperation');
      const operation = new DBSQLOperation({ handle, context });

      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.false;

      await operation.cancel();

      expect(context.driver.cancelOperation.called).to.be.true;
      expect(operation.cancelled).to.be.true;
      expect(operation.closed).to.be.false;
    });

    it('should return immediately if already cancelled', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      sinon.spy(context.driver, 'cancelOperation');
      const operation = new DBSQLOperation({ handle, context });

      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.false;

      await operation.cancel();
      expect(context.driver.cancelOperation.callCount).to.be.equal(1);
      expect(operation.cancelled).to.be.true;
      expect(operation.closed).to.be.false;

      await operation.cancel();
      expect(context.driver.cancelOperation.callCount).to.be.equal(1);
      expect(operation.cancelled).to.be.true;
      expect(operation.closed).to.be.false;
    });

    it('should return immediately if already closed', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      sinon.spy(context.driver, 'cancelOperation');
      sinon.spy(context.driver, 'closeOperation');
      const operation = new DBSQLOperation({ handle, context });

      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.false;

      await operation.close();
      expect(context.driver.closeOperation.callCount).to.be.equal(1);
      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.true;

      await operation.cancel();
      expect(context.driver.cancelOperation.callCount).to.be.equal(0);
      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.true;
    });

    it('should throw an error in case of a status error and keep state', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.cancelOperationResp.status.statusCode = TStatusCode.ERROR_STATUS;
      const operation = new DBSQLOperation({ handle, context });

      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.false;

      try {
        await operation.cancel();
        expect.fail('It should throw a StatusError');
      } catch (e) {
        if (e instanceof AssertionError) {
          throw e;
        }
        expect(e).to.be.instanceOf(StatusError);
        expect(operation.cancelled).to.be.false;
        expect(operation.closed).to.be.false;
      }
    });

    it('should reject all methods once cancelled', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();
      const operation = new DBSQLOperation({ handle, context });

      await operation.cancel();
      expect(operation.cancelled).to.be.true;

      await expectFailure(() => operation.fetchAll());
      await expectFailure(() => operation.fetchChunk({ disableBuffering: true }));
      await expectFailure(() => operation.status());
      await expectFailure(() => operation.finished());
      await expectFailure(() => operation.getSchema());
    });
  });

  describe('close', () => {
    it('should close operation and update state', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      sinon.spy(context.driver, 'closeOperation');
      const operation = new DBSQLOperation({ handle, context });

      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.false;

      await operation.close();

      expect(context.driver.closeOperation.called).to.be.true;
      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.true;
    });

    it('should return immediately if already closed', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      sinon.spy(context.driver, 'closeOperation');
      const operation = new DBSQLOperation({ handle, context });

      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.false;

      await operation.close();
      expect(context.driver.closeOperation.callCount).to.be.equal(1);
      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.true;

      await operation.close();
      expect(context.driver.closeOperation.callCount).to.be.equal(1);
      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.true;
    });

    it('should return immediately if already cancelled', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      sinon.spy(context.driver, 'closeOperation');
      sinon.spy(context.driver, 'cancelOperation');
      const operation = new DBSQLOperation({ handle, context });

      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.false;

      await operation.cancel();
      expect(context.driver.cancelOperation.callCount).to.be.equal(1);
      expect(operation.cancelled).to.be.true;
      expect(operation.closed).to.be.false;

      await operation.close();
      expect(context.driver.closeOperation.callCount).to.be.equal(0);
      expect(operation.cancelled).to.be.true;
      expect(operation.closed).to.be.false;
    });

    it('should initialize from directResults', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      sinon.spy(context.driver, 'closeOperation');
      const operation = new DBSQLOperation({
        handle,
        context,
        directResults: {
          closeOperation: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
          },
        },
      });

      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.false;

      await operation.close();

      expect(context.driver.closeOperation.called).to.be.false;
      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.true;
      expect(context.driver.closeOperation.callCount).to.be.equal(0);
    });

    it('should throw an error in case of a status error and keep state', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.closeOperationResp.status.statusCode = TStatusCode.ERROR_STATUS;
      const operation = new DBSQLOperation({ handle, context });

      expect(operation.cancelled).to.be.false;
      expect(operation.closed).to.be.false;

      try {
        await operation.close();
        expect.fail('It should throw a StatusError');
      } catch (e) {
        if (e instanceof AssertionError) {
          throw e;
        }
        expect(e).to.be.instanceOf(StatusError);
        expect(operation.cancelled).to.be.false;
        expect(operation.closed).to.be.false;
      }
    });

    it('should reject all methods once closed', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();
      const operation = new DBSQLOperation({ handle, context });

      await operation.close();
      expect(operation.closed).to.be.true;

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

          const context = new ClientContextMock();
          const handle = new OperationHandleMock();

          context.driver.getOperationStatusResp.operationState = operationState;
          sinon
            .stub(context.driver, 'getOperationStatus')
            .callThrough()
            .onCall(attemptsUntilFinished - 1) // count is zero-based
            .callsFake((...args) => {
              context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
              return context.driver.getOperationStatus.wrappedMethod.apply(context.driver, args);
            });

          const operation = new DBSQLOperation({ handle, context });

          expect(operation.state).to.equal(TOperationState.INITIALIZED_STATE);

          await operation.finished();

          expect(context.driver.getOperationStatus.callCount).to.be.equal(attemptsUntilFinished);
          expect(operation.state).to.equal(TOperationState.FINISHED_STATE);
        });
      },
    );

    it('should request progress', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(context.driver, 'getOperationStatus')
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return context.driver.getOperationStatus.wrappedMethod.apply(context.driver, args);
        });

      const operation = new DBSQLOperation({ handle, context });
      await operation.finished({ progress: true });

      expect(context.driver.getOperationStatus.called).to.be.true;
      const request = context.driver.getOperationStatus.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should invoke progress callback', async () => {
      const attemptsUntilFinished = 3;

      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(context.driver, 'getOperationStatus')
        .callThrough()
        .onCall(attemptsUntilFinished - 1) // count is zero-based
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return context.driver.getOperationStatus.wrappedMethod.apply(context.driver, args);
        });

      const operation = new DBSQLOperation({ handle, context });

      const callback = sinon.stub();

      await operation.finished({ callback });

      expect(context.driver.getOperationStatus.called).to.be.true;
      expect(callback.callCount).to.be.equal(attemptsUntilFinished);
    });

    it('should pick up finished state from directResults', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      sinon.spy(context.driver, 'getOperationStatus');
      context.driver.getOperationStatusResp.status.statusCode = TStatusCode.SUCCESS_STATUS;
      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;

      const operation = new DBSQLOperation({
        handle,
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
      expect(context.driver.getOperationStatus.called).to.be.false;
    });

    it('should throw an error in case of a status error', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.getOperationStatusResp.status.statusCode = TStatusCode.ERROR_STATUS;
      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      const operation = new DBSQLOperation({ handle, context });

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
        const context = new ClientContextMock();
        const handle = new OperationHandleMock();

        context.driver.getOperationStatusResp.status.statusCode = TStatusCode.SUCCESS_STATUS;
        context.driver.getOperationStatusResp.operationState = operationState;
        const operation = new DBSQLOperation({ handle, context });

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
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = false;
      sinon.spy(context.driver, 'getResultSetMetadata');
      const operation = new DBSQLOperation({ handle, context });

      const schema = await operation.getSchema();

      expect(schema).to.be.null;
      expect(context.driver.getResultSetMetadata.called).to.be.false;
    });

    it('should wait for operation to complete', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(context.driver, 'getOperationStatus')
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return context.driver.getOperationStatus.wrappedMethod.apply(context.driver, args);
        });

      context.driver.getResultSetMetadataResp.schema = { columns: [] };

      const operation = new DBSQLOperation({ handle, context });

      const schema = await operation.getSchema();

      expect(context.driver.getOperationStatus.called).to.be.true;
      expect(schema).to.deep.equal(context.driver.getResultSetMetadataResp.schema);
      expect(operation.state).to.equal(TOperationState.FINISHED_STATE);
    });

    it('should request progress', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(context.driver, 'getOperationStatus')
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return context.driver.getOperationStatus.wrappedMethod.apply(context.driver, args);
        });

      const operation = new DBSQLOperation({ handle, context });
      await operation.getSchema({ progress: true });

      expect(context.driver.getOperationStatus.called).to.be.true;
      const request = context.driver.getOperationStatus.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should invoke progress callback', async () => {
      const attemptsUntilFinished = 3;

      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(context.driver, 'getOperationStatus')
        .callThrough()
        .onCall(attemptsUntilFinished - 1) // count is zero-based
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return context.driver.getOperationStatus.wrappedMethod.apply(context.driver, args);
        });

      const operation = new DBSQLOperation({ handle, context });

      const callback = sinon.stub();

      await operation.getSchema({ callback });

      expect(context.driver.getOperationStatus.called).to.be.true;
      expect(callback.callCount).to.be.equal(attemptsUntilFinished);
    });

    it('should fetch schema if operation has data', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      sinon.spy(context.driver, 'getResultSetMetadata');
      const operation = new DBSQLOperation({ handle, context });

      const schema = await operation.getSchema();

      expect(schema).to.deep.equal(context.driver.getResultSetMetadataResp.schema);
      expect(context.driver.getResultSetMetadata.called).to.be.true;
    });

    it('should return cached schema on subsequent calls', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      sinon.spy(context.driver, 'getResultSetMetadata');
      const operation = new DBSQLOperation({ handle, context });

      const schema1 = await operation.getSchema();
      expect(schema1).to.deep.equal(context.driver.getResultSetMetadataResp.schema);
      expect(context.driver.getResultSetMetadata.callCount).to.equal(1);

      const schema2 = await operation.getSchema();
      expect(schema2).to.deep.equal(context.driver.getResultSetMetadataResp.schema);
      expect(context.driver.getResultSetMetadata.callCount).to.equal(1); // no additional requests
    });

    it('should use schema from directResults', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      sinon.spy(context.driver, 'getResultSetMetadata');

      const directResults = {
        resultSetMetadata: {
          status: { statusCode: TStatusCode.SUCCESS_STATUS },
          schema: {
            columns: [{ columnName: 'another' }],
          },
        },
      };
      const operation = new DBSQLOperation({ handle, context, directResults });

      const schema = await operation.getSchema();

      expect(schema).to.deep.equal(directResults.resultSetMetadata.schema);
      expect(context.driver.getResultSetMetadata.called).to.be.false;
    });

    it('should throw an error in case of a status error', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.getResultSetMetadataResp.status.statusCode = TStatusCode.ERROR_STATUS;
      const operation = new DBSQLOperation({ handle, context });

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
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      sinon.spy(context.driver, 'getResultSetMetadata');

      jsonHandler: {
        context.driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.COLUMN_BASED_SET;
        context.driver.getResultSetMetadata.resetHistory();

        const operation = new DBSQLOperation({ handle, context });
        const resultHandler = await operation.getResultHandler();
        expect(context.driver.getResultSetMetadata.called).to.be.true;
        expect(resultHandler).to.be.instanceOf(ResultSlicer);
        expect(resultHandler.source).to.be.instanceOf(JsonResultHandler);
      }

      arrowHandler: {
        context.driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.ARROW_BASED_SET;
        context.driver.getResultSetMetadata.resetHistory();

        const operation = new DBSQLOperation({ handle, context });
        const resultHandler = await operation.getResultHandler();
        expect(context.driver.getResultSetMetadata.called).to.be.true;
        expect(resultHandler).to.be.instanceOf(ResultSlicer);
        expect(resultHandler.source).to.be.instanceOf(ArrowResultHandler);
      }

      cloudFetchHandler: {
        context.driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.URL_BASED_SET;
        context.driver.getResultSetMetadata.resetHistory();

        const operation = new DBSQLOperation({ handle, context });
        const resultHandler = await operation.getResultHandler();
        expect(context.driver.getResultSetMetadata.called).to.be.true;
        expect(resultHandler).to.be.instanceOf(ResultSlicer);
        expect(resultHandler.source).to.be.instanceOf(CloudFetchResultHandler);
      }
    });
  });

  describe('fetchChunk', () => {
    it('should return immediately if operation has no results', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      sinon.spy(context.driver, 'getResultSetMetadata');
      sinon.spy(context.driver, 'fetchResults');
      const operation = new DBSQLOperation({ handle, context });

      const results = await operation.fetchChunk({ disableBuffering: true });

      expect(results).to.deep.equal([]);
      expect(context.driver.getResultSetMetadata.called).to.be.false;
      expect(context.driver.fetchResults.called).to.be.false;
    });

    it('should wait for operation to complete', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(context.driver, 'getOperationStatus')
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return context.driver.getOperationStatus.wrappedMethod.apply(context.driver, args);
        });

      context.driver.getResultSetMetadataResp.schema = { columns: [] };
      context.driver.fetchResultsResp.hasMoreRows = false;
      context.driver.fetchResultsResp.results.columns = [];

      const operation = new DBSQLOperation({ handle, context });

      const results = await operation.fetchChunk({ disableBuffering: true });

      expect(context.driver.getOperationStatus.called).to.be.true;
      expect(results).to.deep.equal([]);
      expect(operation.state).to.equal(TOperationState.FINISHED_STATE);
    });

    it('should request progress', async () => {
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(context.driver, 'getOperationStatus')
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return context.driver.getOperationStatus.wrappedMethod.apply(context.driver, args);
        });

      context.driver.getResultSetMetadataResp.schema = { columns: [] };
      context.driver.fetchResultsResp.hasMoreRows = false;
      context.driver.fetchResultsResp.results.columns = [];

      const operation = new DBSQLOperation({ handle, context });
      await operation.fetchChunk({ progress: true, disableBuffering: true });

      expect(context.driver.getOperationStatus.called).to.be.true;
      const request = context.driver.getOperationStatus.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should invoke progress callback', async () => {
      const attemptsUntilFinished = 3;

      const context = new ClientContextMock();
      const handle = new OperationHandleMock();

      context.driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(context.driver, 'getOperationStatus')
        .callThrough()
        .onCall(attemptsUntilFinished - 1) // count is zero-based
        .callsFake((...args) => {
          context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return context.driver.getOperationStatus.wrappedMethod.apply(context.driver, args);
        });

      context.driver.getResultSetMetadataResp.schema = { columns: [] };
      context.driver.fetchResultsResp.hasMoreRows = false;
      context.driver.fetchResultsResp.results.columns = [];

      const operation = new DBSQLOperation({ handle, context });

      const callback = sinon.stub();

      await operation.fetchChunk({ callback, disableBuffering: true });

      expect(context.driver.getOperationStatus.called).to.be.true;
      expect(callback.callCount).to.be.equal(attemptsUntilFinished);
    });

    it('should fetch schema and data and return array of records', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      sinon.spy(context.driver, 'getResultSetMetadata');
      sinon.spy(context.driver, 'fetchResults');

      const operation = new DBSQLOperation({ handle, context });

      const results = await operation.fetchChunk({ disableBuffering: true });

      expect(results).to.deep.equal([{ test: 1 }, { test: 2 }, { test: 3 }]);
      expect(context.driver.getResultSetMetadata.called).to.be.true;
      expect(context.driver.fetchResults.called).to.be.true;
    });

    it('should return data from directResults (all the data in directResults)', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      sinon.spy(context.driver, 'getResultSetMetadata');
      sinon.spy(context.driver, 'fetchResults');

      const operation = new DBSQLOperation({
        handle,
        context,
        directResults: {
          resultSet: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
            hasMoreRows: false,
            results: {
              columns: [
                {
                  i32Val: {
                    values: [5, 6],
                  },
                },
              ],
            },
          },
        },
      });

      const results = await operation.fetchChunk({ disableBuffering: true });

      expect(results).to.deep.equal([{ test: 5 }, { test: 6 }]);
      expect(context.driver.getResultSetMetadata.called).to.be.true;
      expect(context.driver.fetchResults.called).to.be.false;
    });

    it('should return data from directResults (first chunk in directResults, next chunk fetched)', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      sinon.spy(context.driver, 'getResultSetMetadata');
      sinon.spy(context.driver, 'fetchResults');

      const operation = new DBSQLOperation({
        handle,
        context,
        directResults: {
          resultSet: {
            status: { statusCode: TStatusCode.SUCCESS_STATUS },
            hasMoreRows: true,
            results: {
              columns: [
                {
                  i32Val: {
                    values: [5, 6],
                  },
                },
              ],
            },
          },
        },
      });

      const results1 = await operation.fetchChunk({ disableBuffering: true });

      expect(results1).to.deep.equal([{ test: 5 }, { test: 6 }]);
      expect(context.driver.getResultSetMetadata.callCount).to.be.eq(1);
      expect(context.driver.fetchResults.callCount).to.be.eq(0);

      const results2 = await operation.fetchChunk({ disableBuffering: true });

      expect(results2).to.deep.equal([{ test: 1 }, { test: 2 }, { test: 3 }]);
      expect(context.driver.getResultSetMetadata.callCount).to.be.eq(1);
      expect(context.driver.fetchResults.callCount).to.be.eq(1);
    });

    it('should fail on unsupported result format', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;

      context.driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.ROW_BASED_SET;
      context.driver.getResultSetMetadataResp.schema = { columns: [] };

      const operation = new DBSQLOperation({ handle, context });

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
      const context = new ClientContextMock();
      const handle = new OperationHandleMock();
      const operation = new DBSQLOperation({ handle, context });

      const originalData = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0];

      const tempData = [...originalData];
      sinon.stub(operation, 'fetchChunk').callsFake(() => {
        return Promise.resolve(tempData.splice(0, 3));
      });
      sinon.stub(operation, 'hasMoreRows').callsFake(() => {
        return tempData.length > 0;
      });

      const fetchedData = await operation.fetchAll();

      // Warning: this check is implementation-specific
      // `fetchAll` should wait for operation to complete. In current implementation
      // it does so by calling `fetchChunk` at least once, which internally does
      // all the job. But since here we mock `fetchChunk` it won't really wait,
      // therefore here we ensure it was called at least once
      expect(operation.fetchChunk.callCount).to.be.gte(1);

      expect(operation.fetchChunk.called).to.be.true;
      expect(operation.hasMoreRows.called).to.be.true;
      expect(fetchedData).to.deep.equal(originalData);
    });
  });

  describe('hasMoreRows', () => {
    it('should return initial value prior to first fetch', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = false;
      context.driver.fetchResultsResp.results = undefined;
      const operation = new DBSQLOperation({ handle, context });

      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation._data.hasMoreRowsFlag).to.be.undefined;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.false;
      expect(operation._data.hasMoreRowsFlag).to.be.false;
    });

    it('should return False if operation was closed', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = true;
      const operation = new DBSQLOperation({ handle, context });

      expect(await operation.hasMoreRows()).to.be.true;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.true;
      await operation.close();
      expect(await operation.hasMoreRows()).to.be.false;
    });

    it('should return False if operation was cancelled', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = true;
      const operation = new DBSQLOperation({ handle, context });

      expect(await operation.hasMoreRows()).to.be.true;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.true;
      await operation.cancel();
      expect(await operation.hasMoreRows()).to.be.false;
    });

    it('should return True if hasMoreRows flag was set in response', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = true;
      const operation = new DBSQLOperation({ handle, context });

      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation._data.hasMoreRowsFlag).to.be.undefined;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation._data.hasMoreRowsFlag).to.be.true;
    });

    it('should return True if hasMoreRows flag is False but there is actual data', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = false;
      const operation = new DBSQLOperation({ handle, context });

      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation._data.hasMoreRowsFlag).to.be.undefined;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation._data.hasMoreRowsFlag).to.be.true;
    });

    it('should return True if hasMoreRows flag is unset but there is actual data', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = undefined;
      const operation = new DBSQLOperation({ handle, context });

      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation._data.hasMoreRowsFlag).to.be.undefined;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation._data.hasMoreRowsFlag).to.be.true;
    });

    it('should return False if hasMoreRows flag is False and there is no data', async () => {
      const context = new ClientContextMock();

      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      context.driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      context.driver.getOperationStatusResp.hasResultSet = true;
      context.driver.fetchResultsResp.hasMoreRows = false;
      context.driver.fetchResultsResp.results = undefined;
      const operation = new DBSQLOperation({ handle, context });

      expect(await operation.hasMoreRows()).to.be.true;
      expect(operation._data.hasMoreRowsFlag).to.be.undefined;
      await operation.fetchChunk({ disableBuffering: true });
      expect(await operation.hasMoreRows()).to.be.false;
      expect(operation._data.hasMoreRowsFlag).to.be.false;
    });
  });
});
