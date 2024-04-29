const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const GetPrimaryKeysCommand = require('../../../../lib/hive/Commands/GetPrimaryKeysCommand').default;

const requestMock = {
  sessionHandle: {
    sessionId: { guid: '', secret: '' },
  },
};

const GET_PRIMARY_KEYS = 7;

const responseMock = {
  status: { statusCode: 0 },
  operationHandle: {
    hasResultSet: true,
    operationId: { guid: '', secret: '' },
    operationType: GET_PRIMARY_KEYS,
    modifiedRowCount: 0,
  },
};

function TGetPrimaryKeysReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  GetPrimaryKeys(request, callback) {
    return callback(null, responseMock);
  },
};

describe('GetPrimaryKeysCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TGetPrimaryKeysReq', TGetPrimaryKeysReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new GetPrimaryKeysCommand(thriftClientMock);

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
