const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const CancelDelegationTokenCommand = require('../../../../dist/hive/Commands/CancelDelegationTokenCommand').default;

const requestMock = {
  sessionHandle: {
    sessionId: { guid: '', secret: '' },
  },
  delegationToken: 'token',
};

const responseMock = {
  status: { statusCode: 0 },
};

function TCancelDelegationTokenReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  CancelDelegationToken(request, callback) {
    return callback(null, responseMock);
  },
};

describe('CancelDelegationTokenCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TCancelDelegationTokenReq', TCancelDelegationTokenReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new CancelDelegationTokenCommand(thriftClientMock);

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
