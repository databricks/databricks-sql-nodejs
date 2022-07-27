const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const GetFunctionsCommand = require('../../../../dist/hive/Commands/GetFunctionsCommand').default;

const requestMock = {
  sessionHandle: {
    sessionId: { guid: '', secret: '' },
  },
};

const GET_FUNCTIONS = 7;

const responseMock = {
  status: { statusCode: 0 },
  operationHandle: {
    hasResultSet: true,
    operationId: { guid: '', secret: '' },
    operationType: GET_FUNCTIONS,
    modifiedRowCount: 0,
  },
};

function TGetFunctionsReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  GetFunctions(request, callback) {
    return callback(null, responseMock);
  },
};

describe('GetFunctionsCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TGetFunctionsReq', TGetFunctionsReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new GetFunctionsCommand(thriftClientMock);

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
