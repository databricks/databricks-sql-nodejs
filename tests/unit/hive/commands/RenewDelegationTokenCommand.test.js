const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const RenewDelegationTokenCommand = require('../../../../lib/hive/Commands/RenewDelegationTokenCommand').default;

const requestMock = {
  sessionHandle: {
    sessionId: { guid: '', secret: '' },
  },
  delegationToken: 'token',
};

const responseMock = {
  status: { statusCode: 0 },
};

function TRenewDelegationTokenReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  RenewDelegationToken(request, callback) {
    return callback(null, responseMock);
  },
};

describe('RenewDelegationTokenCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TRenewDelegationTokenReq', TRenewDelegationTokenReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new RenewDelegationTokenCommand(thriftClientMock);

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
