const { expect, AssertionError } = require('chai');
const sinon = require('sinon');
const { DBSQLLogger, LogLevel } = require('../../dist');
const { TStatusCode, TOperationState, TTypeId, TSparkRowSetType } = require('../../thrift/TCLIService_types');
const DBSQLOperation = require('../../dist/DBSQLOperation').default;
const StatusError = require('../../dist/errors/StatusError').default;
const OperationStateError = require('../../dist/errors/OperationStateError').default;
const HiveDriverError = require('../../dist/errors/HiveDriverError').default;
const JsonResult = require('../../dist/result/JsonResult').default;
const ArrowResult = require('../../dist/result/ArrowResult').default;

// Create logger that won't emit
//
const logger = new DBSQLLogger({ level: LogLevel.error });

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

describe('DBSQLOperation', () => {
  describe('status', () => {
    it('should pick up state from operation handle', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(operation._status.state).to.equal(TOperationState.INITIALIZED_STATE);
      expect(operation._status.hasResultSet).to.be.true;
    });

    it('should pick up state from directResults', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();

      const operation = new DBSQLOperation(driver, handle, logger, {
        operationStatus: {
          status: { statusCode: TStatusCode.SUCCESS_STATUS },
          operationState: TOperationState.FINISHED_STATE,
          hasResultSet: true,
        },
      });

      expect(operation._status.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation._status.hasResultSet).to.be.true;
    });

    it('should fetch status and update internal state', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      const driver = new DriverMock();
      sinon.spy(driver, 'getOperationStatus');
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation(driver, handle, logger);

      expect(operation._status.state).to.equal(TOperationState.INITIALIZED_STATE);
      expect(operation._status.hasResultSet).to.be.false;

      const status = await operation.status();

      expect(driver.getOperationStatus.called).to.be.true;
      expect(status.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation._status.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation._status.hasResultSet).to.be.true;
    });

    it('should request progress', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      const driver = new DriverMock();
      sinon.spy(driver, 'getOperationStatus');
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;

      const operation = new DBSQLOperation(driver, handle, logger);
      await operation.status(true);

      expect(driver.getOperationStatus.called).to.be.true;
      const request = driver.getOperationStatus.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should not fetch status once operation is finished', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      const driver = new DriverMock();
      sinon.spy(driver, 'getOperationStatus');
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation(driver, handle, logger);

      expect(operation._status.state).to.equal(TOperationState.INITIALIZED_STATE);
      expect(operation._status.hasResultSet).to.be.false;

      // First call - should fetch data and cache
      driver.getOperationStatusResp = {
        ...driver.getOperationStatusResp,
        operationState: TOperationState.FINISHED_STATE,
      };
      const status1 = await operation.status();

      expect(driver.getOperationStatus.callCount).to.equal(1);
      expect(status1.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation._status.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation._status.hasResultSet).to.be.true;

      // Second call - should return cached data
      driver.getOperationStatusResp = {
        ...driver.getOperationStatusResp,
        operationState: TOperationState.RUNNING_STATE,
      };
      const status2 = await operation.status();

      expect(driver.getOperationStatus.callCount).to.equal(1);
      expect(status2.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation._status.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation._status.hasResultSet).to.be.true;
    });

    it('should fetch status if directResults status is not finished', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      const driver = new DriverMock();
      sinon.spy(driver, 'getOperationStatus');
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation(driver, handle, logger, {
        operationStatus: {
          status: { statusCode: TStatusCode.SUCCESS_STATUS },
          operationState: TOperationState.RUNNING_STATE,
          hasResultSet: false,
        },
      });

      expect(operation._status.state).to.equal(TOperationState.RUNNING_STATE); // from directResults
      expect(operation._status.hasResultSet).to.be.false;

      const status = await operation.status(false);

      expect(driver.getOperationStatus.called).to.be.true;
      expect(status.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation._status.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation._status.hasResultSet).to.be.true;
    });

    it('should not fetch status if directResults status is finished', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      const driver = new DriverMock();
      sinon.spy(driver, 'getOperationStatus');
      driver.getOperationStatusResp.operationState = TOperationState.RUNNING_STATE;
      driver.getOperationStatusResp.hasResultSet = true;

      const operation = new DBSQLOperation(driver, handle, logger, {
        operationStatus: {
          status: { statusCode: TStatusCode.SUCCESS_STATUS },
          operationState: TOperationState.FINISHED_STATE,
          hasResultSet: false,
        },
      });

      expect(operation._status.state).to.equal(TOperationState.FINISHED_STATE); // from directResults
      expect(operation._status.hasResultSet).to.be.false;

      const status = await operation.status(false);

      expect(driver.getOperationStatus.called).to.be.false;
      expect(status.operationState).to.equal(TOperationState.FINISHED_STATE);
      expect(operation._status.state).to.equal(TOperationState.FINISHED_STATE);
      expect(operation._status.hasResultSet).to.be.false;
    });

    it('should throw an error in case of a status error', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.getOperationStatusResp.status.statusCode = TStatusCode.ERROR_STATUS;
      const operation = new DBSQLOperation(driver, handle, logger);

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
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      sinon.spy(driver, 'cancelOperation');
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.false;

      await operation.cancel();

      expect(driver.cancelOperation.called).to.be.true;
      expect(operation._completeOperation.cancelled).to.be.true;
      expect(operation._completeOperation.closed).to.be.false;
    });

    it('should return immediately if already cancelled', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      sinon.spy(driver, 'cancelOperation');
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.false;

      await operation.cancel();
      expect(driver.cancelOperation.callCount).to.be.equal(1);
      expect(operation._completeOperation.cancelled).to.be.true;
      expect(operation._completeOperation.closed).to.be.false;

      await operation.cancel();
      expect(driver.cancelOperation.callCount).to.be.equal(1);
      expect(operation._completeOperation.cancelled).to.be.true;
      expect(operation._completeOperation.closed).to.be.false;
    });

    it('should return immediately if already closed', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      sinon.spy(driver, 'cancelOperation');
      sinon.spy(driver, 'closeOperation');
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.false;

      await operation.close();
      expect(driver.closeOperation.callCount).to.be.equal(1);
      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.true;

      await operation.cancel();
      expect(driver.cancelOperation.callCount).to.be.equal(0);
      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.true;
    });

    it('should throw an error in case of a status error and keep state', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.cancelOperationResp.status.statusCode = TStatusCode.ERROR_STATUS;
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.false;

      try {
        await operation.cancel();
        expect.fail('It should throw a StatusError');
      } catch (e) {
        if (e instanceof AssertionError) {
          throw e;
        }
        expect(e).to.be.instanceOf(StatusError);
        expect(operation._completeOperation.cancelled).to.be.false;
        expect(operation._completeOperation.closed).to.be.false;
      }
    });

    it('should reject all methods once cancelled', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      const operation = new DBSQLOperation(driver, handle, logger);

      await operation.cancel();
      expect(operation._completeOperation.cancelled).to.be.true;

      await expectFailure(() => operation.fetchAll());
      await expectFailure(() => operation.fetchChunk());
      await expectFailure(() => operation.status());
      await expectFailure(() => operation.finished());
      await expectFailure(() => operation.getSchema());
    });
  });

  describe('close', () => {
    it('should close operation and update state', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      sinon.spy(driver, 'closeOperation');
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.false;

      await operation.close();

      expect(driver.closeOperation.called).to.be.true;
      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.true;
    });

    it('should return immediately if already closed', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      sinon.spy(driver, 'closeOperation');
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.false;

      await operation.close();
      expect(driver.closeOperation.callCount).to.be.equal(1);
      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.true;

      await operation.close();
      expect(driver.closeOperation.callCount).to.be.equal(1);
      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.true;
    });

    it('should return immediately if already cancelled', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      sinon.spy(driver, 'closeOperation');
      sinon.spy(driver, 'cancelOperation');
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.false;

      await operation.cancel();
      expect(driver.cancelOperation.callCount).to.be.equal(1);
      expect(operation._completeOperation.cancelled).to.be.true;
      expect(operation._completeOperation.closed).to.be.false;

      await operation.close();
      expect(driver.closeOperation.callCount).to.be.equal(0);
      expect(operation._completeOperation.cancelled).to.be.true;
      expect(operation._completeOperation.closed).to.be.false;
    });

    it('should initialize from directResults', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      sinon.spy(driver, 'closeOperation');
      const operation = new DBSQLOperation(driver, handle, logger, {
        closeOperation: {
          status: { statusCode: TStatusCode.SUCCESS_STATUS },
        },
      });

      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.false;

      await operation.close();

      expect(driver.closeOperation.called).to.be.false;
      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.true;
      expect(driver.closeOperation.callCount).to.be.equal(0);
    });

    it('should throw an error in case of a status error and keep state', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.closeOperationResp.status.statusCode = TStatusCode.ERROR_STATUS;
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(operation._completeOperation.cancelled).to.be.false;
      expect(operation._completeOperation.closed).to.be.false;

      try {
        await operation.close();
        expect.fail('It should throw a StatusError');
      } catch (e) {
        if (e instanceof AssertionError) {
          throw e;
        }
        expect(e).to.be.instanceOf(StatusError);
        expect(operation._completeOperation.cancelled).to.be.false;
        expect(operation._completeOperation.closed).to.be.false;
      }
    });

    it('should reject all methods once closed', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      const operation = new DBSQLOperation(driver, handle, logger);

      await operation.close();
      expect(operation._completeOperation.closed).to.be.true;

      await expectFailure(() => operation.fetchAll());
      await expectFailure(() => operation.fetchChunk());
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

          const handle = new OperationHandleMock();
          const driver = new DriverMock();
          driver.getOperationStatusResp.operationState = operationState;
          sinon
            .stub(driver, 'getOperationStatus')
            .callThrough()
            .onCall(attemptsUntilFinished - 1) // count is zero-based
            .callsFake((...args) => {
              driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
              return driver.getOperationStatus.wrappedMethod.apply(driver, args);
            });

          const operation = new DBSQLOperation(driver, handle, logger);

          expect(operation._status.state).to.equal(TOperationState.INITIALIZED_STATE);

          await operation.finished();

          expect(driver.getOperationStatus.callCount).to.be.equal(attemptsUntilFinished);
          expect(operation._status.state).to.equal(TOperationState.FINISHED_STATE);
        });
      },
    );

    it('should request progress', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(driver, 'getOperationStatus')
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return driver.getOperationStatus.wrappedMethod.apply(driver, args);
        });

      const operation = new DBSQLOperation(driver, handle, logger);
      await operation.finished({ progress: true });

      expect(driver.getOperationStatus.called).to.be.true;
      const request = driver.getOperationStatus.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should invoke progress callback', async () => {
      const attemptsUntilFinished = 3;

      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(driver, 'getOperationStatus')
        .callThrough()
        .onCall(attemptsUntilFinished - 1) // count is zero-based
        .callsFake((...args) => {
          driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return driver.getOperationStatus.wrappedMethod.apply(driver, args);
        });

      const operation = new DBSQLOperation(driver, handle, logger);

      const callback = sinon.stub();

      await operation.finished({ callback });

      expect(driver.getOperationStatus.called).to.be.true;
      expect(callback.callCount).to.be.equal(attemptsUntilFinished);
    });

    it('should pick up finished state from directResults', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      sinon.spy(driver, 'getOperationStatus');
      driver.getOperationStatusResp.status.statusCode = TStatusCode.SUCCESS_STATUS;
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;

      const operation = new DBSQLOperation(driver, handle, logger, {
        operationStatus: {
          status: { statusCode: TStatusCode.SUCCESS_STATUS },
          operationState: TOperationState.FINISHED_STATE,
          hasResultSet: true,
        },
      });

      await operation.finished();

      // Once operation is finished - no need to fetch status again
      expect(driver.getOperationStatus.called).to.be.false;
    });

    it('should throw an error in case of a status error', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.getOperationStatusResp.status.statusCode = TStatusCode.ERROR_STATUS;
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      const operation = new DBSQLOperation(driver, handle, logger);

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
        const handle = new OperationHandleMock();
        const driver = new DriverMock();
        driver.getOperationStatusResp.status.statusCode = TStatusCode.SUCCESS_STATUS;
        driver.getOperationStatusResp.operationState = operationState;
        const operation = new DBSQLOperation(driver, handle, logger);

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
      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = false;
      sinon.spy(driver, 'getResultSetMetadata');
      const operation = new DBSQLOperation(driver, handle, logger);

      const schema = await operation.getSchema();

      expect(schema).to.be.null;
      expect(driver.getResultSetMetadata.called).to.be.false;
    });

    it('should wait for operation to complete', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(driver, 'getOperationStatus')
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return driver.getOperationStatus.wrappedMethod.apply(driver, args);
        });

      driver.getResultSetMetadataResp.schema = { columns: [] };

      const operation = new DBSQLOperation(driver, handle, logger);

      const schema = await operation.getSchema();

      expect(driver.getOperationStatus.called).to.be.true;
      expect(schema).to.deep.equal(driver.getResultSetMetadataResp.schema);
      expect(operation._status.state).to.equal(TOperationState.FINISHED_STATE);
    });

    it('should request progress', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(driver, 'getOperationStatus')
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return driver.getOperationStatus.wrappedMethod.apply(driver, args);
        });

      const operation = new DBSQLOperation(driver, handle, logger);
      await operation.getSchema({ progress: true });

      expect(driver.getOperationStatus.called).to.be.true;
      const request = driver.getOperationStatus.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should invoke progress callback', async () => {
      const attemptsUntilFinished = 3;

      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(driver, 'getOperationStatus')
        .callThrough()
        .onCall(attemptsUntilFinished - 1) // count is zero-based
        .callsFake((...args) => {
          driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return driver.getOperationStatus.wrappedMethod.apply(driver, args);
        });

      const operation = new DBSQLOperation(driver, handle, logger);

      const callback = sinon.stub();

      await operation.getSchema({ callback });

      expect(driver.getOperationStatus.called).to.be.true;
      expect(callback.callCount).to.be.equal(attemptsUntilFinished);
    });

    it('should fetch schema if operation has data', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      sinon.spy(driver, 'getResultSetMetadata');
      const operation = new DBSQLOperation(driver, handle, logger);

      const schema = await operation.getSchema();

      expect(schema).to.deep.equal(driver.getResultSetMetadataResp.schema);
      expect(driver.getResultSetMetadata.called).to.be.true;
    });

    it('should return cached schema on subsequent calls', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      sinon.spy(driver, 'getResultSetMetadata');
      const operation = new DBSQLOperation(driver, handle, logger);

      const schema1 = await operation.getSchema();
      expect(schema1).to.deep.equal(driver.getResultSetMetadataResp.schema);
      expect(driver.getResultSetMetadata.callCount).to.equal(1);

      const schema2 = await operation.getSchema();
      expect(schema2).to.deep.equal(driver.getResultSetMetadataResp.schema);
      expect(driver.getResultSetMetadata.callCount).to.equal(1); // no additional requests
    });

    it('should use schema from directResults', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      sinon.spy(driver, 'getResultSetMetadata');

      const directResults = {
        resultSetMetadata: {
          status: { statusCode: TStatusCode.SUCCESS_STATUS },
          schema: {
            columns: [{ columnName: 'another' }],
          },
        },
      };
      const operation = new DBSQLOperation(driver, handle, logger, directResults);

      const schema = await operation.getSchema();

      expect(schema).to.deep.equal(directResults.resultSetMetadata.schema);
      expect(driver.getResultSetMetadata.called).to.be.false;
    });

    it('should throw an error in case of a status error', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      driver.getResultSetMetadataResp.status.statusCode = TStatusCode.ERROR_STATUS;
      const operation = new DBSQLOperation(driver, handle, logger);

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
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      sinon.spy(driver, 'getResultSetMetadata');

      jsonHandler: {
        driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.COLUMN_BASED_SET;
        driver.getResultSetMetadata.resetHistory();

        const operation = new DBSQLOperation(driver, handle, logger);
        const resultHandler = await operation._schema.getResultHandler();
        expect(driver.getResultSetMetadata.called).to.be.true;
        expect(resultHandler).to.be.instanceOf(JsonResult);
      }

      arrowHandler: {
        driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.ARROW_BASED_SET;
        driver.getResultSetMetadata.resetHistory();

        const operation = new DBSQLOperation(driver, handle, logger);
        const resultHandler = await operation._schema.getResultHandler();
        expect(driver.getResultSetMetadata.called).to.be.true;
        expect(resultHandler).to.be.instanceOf(ArrowResult);
      }
    });
  });

  describe('fetchChunk', () => {
    it('should return immediately if operation has no results', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = false;

      const driver = new DriverMock();
      sinon.spy(driver, 'getResultSetMetadata');
      sinon.spy(driver, 'fetchResults');
      const operation = new DBSQLOperation(driver, handle, logger);

      const results = await operation.fetchChunk();

      expect(results).to.deep.equal([]);
      expect(driver.getResultSetMetadata.called).to.be.false;
      expect(driver.fetchResults.called).to.be.false;
    });

    it('should wait for operation to complete', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(driver, 'getOperationStatus')
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return driver.getOperationStatus.wrappedMethod.apply(driver, args);
        });

      driver.getResultSetMetadataResp.schema = { columns: [] };
      driver.fetchResultsResp.hasMoreRows = false;
      driver.fetchResultsResp.results.columns = [];

      const operation = new DBSQLOperation(driver, handle, logger);

      const results = await operation.fetchChunk();

      expect(driver.getOperationStatus.called).to.be.true;
      expect(results).to.deep.equal([]);
      expect(operation._status.state).to.equal(TOperationState.FINISHED_STATE);
    });

    it('should request progress', async () => {
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(driver, 'getOperationStatus')
        .callThrough()
        .onSecondCall()
        .callsFake((...args) => {
          driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return driver.getOperationStatus.wrappedMethod.apply(driver, args);
        });

      driver.getResultSetMetadataResp.schema = { columns: [] };
      driver.fetchResultsResp.hasMoreRows = false;
      driver.fetchResultsResp.results.columns = [];

      const operation = new DBSQLOperation(driver, handle, logger);
      await operation.fetchChunk({ progress: true });

      expect(driver.getOperationStatus.called).to.be.true;
      const request = driver.getOperationStatus.getCall(0).args[0];
      expect(request.getProgressUpdate).to.be.true;
    });

    it('should invoke progress callback', async () => {
      const attemptsUntilFinished = 3;

      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.INITIALIZED_STATE;
      sinon
        .stub(driver, 'getOperationStatus')
        .callThrough()
        .onCall(attemptsUntilFinished - 1) // count is zero-based
        .callsFake((...args) => {
          driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
          return driver.getOperationStatus.wrappedMethod.apply(driver, args);
        });

      driver.getResultSetMetadataResp.schema = { columns: [] };
      driver.fetchResultsResp.hasMoreRows = false;
      driver.fetchResultsResp.results.columns = [];

      const operation = new DBSQLOperation(driver, handle, logger);

      const callback = sinon.stub();

      await operation.fetchChunk({ callback });

      expect(driver.getOperationStatus.called).to.be.true;
      expect(callback.callCount).to.be.equal(attemptsUntilFinished);
    });

    it('should fetch schema and data and return array of records', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      sinon.spy(driver, 'getResultSetMetadata');
      sinon.spy(driver, 'fetchResults');

      const operation = new DBSQLOperation(driver, handle, logger);

      const results = await operation.fetchChunk();

      expect(results).to.deep.equal([{ test: 1 }, { test: 2 }, { test: 3 }]);
      expect(driver.getResultSetMetadata.called).to.be.true;
      expect(driver.fetchResults.called).to.be.true;
    });

    it('should return data from directResults (all the data in directResults)', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      sinon.spy(driver, 'getResultSetMetadata');
      sinon.spy(driver, 'fetchResults');

      const operation = new DBSQLOperation(driver, handle, logger, {
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
      });

      const results = await operation.fetchChunk();

      expect(results).to.deep.equal([{ test: 5 }, { test: 6 }]);
      expect(driver.getResultSetMetadata.called).to.be.true;
      expect(driver.fetchResults.called).to.be.false;
    });

    it('should return data from directResults (first chunk in directResults, next chunk fetched)', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      sinon.spy(driver, 'getResultSetMetadata');
      sinon.spy(driver, 'fetchResults');

      const operation = new DBSQLOperation(driver, handle, logger, {
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
      });

      const results1 = await operation.fetchChunk();

      expect(results1).to.deep.equal([{ test: 5 }, { test: 6 }]);
      expect(driver.getResultSetMetadata.callCount).to.be.eq(1);
      expect(driver.fetchResults.callCount).to.be.eq(0);

      const results2 = await operation.fetchChunk();

      expect(results2).to.deep.equal([{ test: 1 }, { test: 2 }, { test: 3 }]);
      expect(driver.getResultSetMetadata.callCount).to.be.eq(1);
      expect(driver.fetchResults.callCount).to.be.eq(1);
    });

    it('should fail on unsupported result format', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;

      driver.getResultSetMetadataResp.resultFormat = TSparkRowSetType.ROW_BASED_SET;
      driver.getResultSetMetadataResp.schema = { columns: [] };

      const operation = new DBSQLOperation(driver, handle, logger);

      try {
        await operation.fetchChunk();
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
      const handle = new OperationHandleMock();
      const driver = new DriverMock();
      const operation = new DBSQLOperation(driver, handle, logger);

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
    it('should return False until first chunk of data fetched', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      driver.fetchResultsResp.hasMoreRows = true;
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(await operation.hasMoreRows()).to.be.false;
      await operation.fetchChunk();
      expect(await operation.hasMoreRows()).to.be.true;
    });

    it('should return False if operation was closed', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      driver.fetchResultsResp.hasMoreRows = true;
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(await operation.hasMoreRows()).to.be.false;
      await operation.fetchChunk();
      expect(await operation.hasMoreRows()).to.be.true;
      await operation.close();
      expect(await operation.hasMoreRows()).to.be.false;
    });

    it('should return False if operation was cancelled', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      driver.fetchResultsResp.hasMoreRows = true;
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(await operation.hasMoreRows()).to.be.false;
      await operation.fetchChunk();
      expect(await operation.hasMoreRows()).to.be.true;
      await operation.cancel();
      expect(await operation.hasMoreRows()).to.be.false;
    });

    it('should return True if hasMoreRows flag was set in response', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      driver.fetchResultsResp.hasMoreRows = true;
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(await operation.hasMoreRows()).to.be.false;
      await operation.fetchChunk();
      expect(await operation.hasMoreRows()).to.be.true;
    });

    it('should return True if hasMoreRows flag is False but there is actual data', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      driver.fetchResultsResp.hasMoreRows = false;
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(await operation.hasMoreRows()).to.be.false;
      await operation.fetchChunk();
      expect(await operation.hasMoreRows()).to.be.true;
    });

    it('should return True if hasMoreRows flag is unset but there is actual data', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      driver.fetchResultsResp.hasMoreRows = undefined;
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(await operation.hasMoreRows()).to.be.false;
      await operation.fetchChunk();
      expect(await operation.hasMoreRows()).to.be.true;
    });

    it('should return False if hasMoreRows flag is False and there is no data', async () => {
      const handle = new OperationHandleMock();
      handle.hasResultSet = true;

      const driver = new DriverMock();
      driver.getOperationStatusResp.operationState = TOperationState.FINISHED_STATE;
      driver.getOperationStatusResp.hasResultSet = true;
      driver.fetchResultsResp.hasMoreRows = false;
      driver.fetchResultsResp.results = undefined;
      const operation = new DBSQLOperation(driver, handle, logger);

      expect(await operation.hasMoreRows()).to.be.false;
      await operation.fetchChunk();
      expect(await operation.hasMoreRows()).to.be.false;
    });
  });
});
