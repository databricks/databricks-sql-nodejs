const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const ExecuteStatementCommand = require('../../../../dist/hive/Commands/ExecuteStatementCommand').default;

const requestMock = {
  sessionHandle: {
    sessionId: { guid: '', secret: '' },
  },
  statement: 'SHOW TABLES',
  confOverlay: {},
  queryTimeout: 0,
};

const EXECUTE_STATEMENT = 0;

const responseMock = {
  status: { statusCode: 0 },
  operationHandle: {
    hasResultSet: true,
    operationId: { guid: '', secret: '' },
    operationType: EXECUTE_STATEMENT,
    modifiedRowCount: 0,
  },
};

function TExecuteStatementReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  ExecuteStatement(request, callback) {
    return callback(null, responseMock);
  },
};

describe('ExecuteStatementCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TExecuteStatementReq', TExecuteStatementReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new ExecuteStatementCommand(thriftClientMock);

    command
      .execute(requestMock)
      .then((response) => {
        expect(response).to.be.deep.eq(responseMock);
        cb();
      })
      .catch((error) => {
        cb(error);
      });
  });
});
