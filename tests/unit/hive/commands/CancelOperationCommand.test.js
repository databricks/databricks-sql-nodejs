const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const CancelOperationCommand = require('../../../../dist/hive/Commands/CancelOperationCommand').default;

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

function TCancelOperationReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  CancelOperation(request, callback) {
    return callback(null, responseMock);
  },
};

describe('CancelOperationCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TCancelOperationReq', TCancelOperationReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new CancelOperationCommand(thriftClientMock);

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
