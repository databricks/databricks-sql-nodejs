const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const CloseOperationCommand = require('../../../../lib/hive/Commands/CloseOperationCommand').default;

const requestMock = {
  operationHandle: {
    hasResultSet: true,
    operationId: { guid: '', secret: '' },
    operationType: 0,
    modifiedRowCount: 0,
  },
};

const responseMock = {
  status: { statusCode: 0 },
};

function TCloseOperationReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  CloseOperation(request, callback) {
    return callback(null, responseMock);
  },
};

describe('CloseOperationCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TCloseOperationReq', TCloseOperationReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new CloseOperationCommand(thriftClientMock);

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
