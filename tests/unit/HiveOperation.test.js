const { expect } = require('chai');
const DBSQLOperation = require('../../dist/DBSQLOperation').default;
const { TCLIService_types } = require('../../').thrift;

const getMock = (parent, prototype) => {
  const mock = function (...args) {
    parent.call(this, ...args);
  };
  mock.prototype = Object.create(parent.prototype);
  mock.prototype.constructor = mock;

  mock.prototype = Object.assign(mock.prototype, prototype);

  return mock;
};

const driverMock = {};
const operationHandle = {};

describe('DBSQLOperation.fetch', () => {
  it('should return success status if there is no results or it is not initialized', (cb) => {
    const operation = new DBSQLOperation(driverMock, operationHandle, TCLIService_types);

    operation
      .fetch()
      .then((status) => {
        expect(status.success()).to.be.eq(true);
        cb();
      })
      .catch((error) => {
        cb(error);
      });
  });

  it('should return executing status if initialization still is not finished', (cb) => {
    const operation = new DBSQLOperation(driverMock, operationHandle, TCLIService_types);
    operation.hasResultSet = true;

    operation
      .fetch()
      .then((status) => {
        expect(status.executing()).to.be.eq(true);
        cb();
      })
      .catch((error) => {
        cb(error);
      });
  });

  it('should initialize schema and make the first fetch request', (cb) => {
    const mockOperation = getMock(DBSQLOperation, {
      initializeSchema() {
        return Promise.resolve('schema');
      },

      firstFetch() {
        return Promise.resolve('data');
      },

      processFetchResponse(data) {
        this.data.push(data);
      },
    });
    const operation = new mockOperation(driverMock, operationHandle, TCLIService_types);
    operation.hasResultSet = true;
    operation.state = TCLIService_types.TOperationState.FINISHED_STATE;

    operation
      .fetch()
      .then((status) => {
        expect(operation.schema).to.be.eq('schema');
        expect(operation.data).includes('data');
        cb();
      })
      .catch((error) => {
        cb(error);
      });
  });

  it('should make the next fetch request if the schema has been set', (cb) => {
    const mockOperation = getMock(DBSQLOperation, {
      nextFetch() {
        return Promise.resolve('data');
      },

      processFetchResponse(data) {
        this.data.push(data);
      },
    });
    const operation = new mockOperation(driverMock, operationHandle, TCLIService_types);
    operation.schema = 'schema';
    operation.hasResultSet = true;
    operation.state = TCLIService_types.TOperationState.FINISHED_STATE;

    operation
      .fetch()
      .then((status) => {
        expect(operation.data).includes('data');
        cb();
      })
      .catch((error) => {
        cb(error);
      });
  });
});

describe('DBSQLOperation.status', () => {
  it('should set operationState and hasResultSet', (cb) => {
    const operation = new DBSQLOperation(
      {
        getOperationStatus() {
          return Promise.resolve({
            status: {
              statusCode: TCLIService_types.TStatusCode.SUCCESS_STATUS,
            },
            operationState: 'state',
            hasResultSet: true,
          });
        },
      },
      operationHandle,
      TCLIService_types,
    );

    operation
      .status()
      .then(() => {
        expect(operation.state).to.be.eq('state');
        expect(operation.hasResultSet).to.be.eq(true);
        cb();
      })
      .catch((error) => {
        cb(error);
      });
  });

  it('should throw an error in case of a status error', (cb) => {
    const operation = new DBSQLOperation(
      {
        getOperationStatus() {
          return Promise.resolve({
            status: {
              statusCode: TCLIService_types.TStatusCode.ERROR_STATUS,
              errorMessage: 'error',
            },
          });
        },
      },
      operationHandle,
      TCLIService_types,
    );

    operation
      .status()
      .then(() => {
        cb(new Error('must not be executed'));
      })
      .catch((error) => {
        expect(error.message).to.be.eq('error');
        cb();
      });
  });
});

describe('DBSQLOperation.cancel', () => {
  it('should run cancelOperation and return the status', (cb) => {
    const operation = new DBSQLOperation(
      {
        cancelOperation() {
          return Promise.resolve({
            status: {
              statusCode: TCLIService_types.TStatusCode.SUCCESS_STATUS,
            },
          });
        },
      },
      operationHandle,
      TCLIService_types,
    );

    operation
      .cancel()
      .then((status) => {
        expect(status.success()).to.be.true;
        cb();
      })
      .catch((error) => {
        cb(error);
      });
  });
});

describe('DBSQLOperation.close', () => {
  it('should run closeOperation and return the status', (cb) => {
    const operation = new DBSQLOperation(
      {
        closeOperation() {
          return Promise.resolve({
            status: {
              statusCode: TCLIService_types.TStatusCode.SUCCESS_STATUS,
            },
          });
        },
      },
      operationHandle,
      TCLIService_types,
    );

    operation
      .close()
      .then((status) => {
        expect(status.success()).to.be.true;
        cb();
      })
      .catch((error) => {
        cb(error);
      });
  });
});

describe('DBSQLOperation.checkIfOperationHasMoreRows', () => {
  it('should return True if hasMoreRows is set True', () => {
    const operation = new DBSQLOperation(driverMock, operationHandle, TCLIService_types);

    const result = operation.checkIfOperationHasMoreRows({
      hasMoreRows: true,
    });

    expect(result).to.be.true;
  });

  it('should return False if the response has no columns', () => {
    const operation = new DBSQLOperation(driverMock, operationHandle, TCLIService_types);

    const result = operation.checkIfOperationHasMoreRows({});

    expect(result).to.be.false;
  });

  it('should return True if at least one of the columns is not empty', () => {
    const operation = new DBSQLOperation(driverMock, operationHandle, TCLIService_types);

    const result = (columnType) =>
      operation.checkIfOperationHasMoreRows({
        results: { columns: [{ [columnType]: { values: ['a'] } }] },
      });

    expect(result('binaryVal')).to.be.true;
    expect(result('boolVal')).to.be.true;
    expect(result('byteVal')).to.be.true;
    expect(result('doubleVal')).to.be.true;
    expect(result('i16Val')).to.be.true;
    expect(result('i32Val')).to.be.true;
    expect(result('i64Val')).to.be.true;
    expect(result('stringVal')).to.be.true;
    expect(result('not_existed_type')).to.be.false;
  });

  it('should return False if all columns are empty', () => {
    const operation = new DBSQLOperation(driverMock, operationHandle, TCLIService_types);

    const result = operation.checkIfOperationHasMoreRows({
      results: { columns: [{ boolVal: { values: [] } }] },
    });

    expect(result).to.be.false;
  });
});

describe('DBSQLOperation.processFetchResponse', () => {
  it('should throw an error if the status is an error', () => {
    const operation = new DBSQLOperation(driverMock, operationHandle, TCLIService_types);

    expect(() =>
      operation.processFetchResponse({
        status: {
          statusCode: TCLIService_types.TStatusCode.ERROR_STATUS,
          errorMessage: 'error',
        },
      }),
    ).throws;
  });

  it('should set hasMoreRows and push data', () => {
    const mockOperation = getMock(DBSQLOperation, {
      checkIfOperationHasMoreRows() {
        return true;
      },
    });
    const operation = new mockOperation(driverMock, operationHandle, TCLIService_types);
    const result = operation.processFetchResponse({
      status: {
        statusCode: TCLIService_types.TStatusCode.SUCCESS_STATUS,
      },
      results: 'data',
    });

    expect(result.success()).to.be.true;
    expect(operation.hasMoreRows()).to.be.true;
    expect(operation.data).includes('data');
  });
});

describe('DBSQLOperation.flush', () => {
  it('should flush data', () => {
    const operation = new DBSQLOperation(driverMock, operationHandle, TCLIService_types);
    operation.data = [1, 2, 3];
    operation.flush();

    expect(operation.data).empty;
  });
});
