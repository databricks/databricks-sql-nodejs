const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const GetDelegationTokenCommand = require('../../../../lib/hive/Commands/GetDelegationTokenCommand').default;

const requestMock = {
  sessionHandle: {
    sessionId: { guid: '', secret: '' },
  },
  owner: 'user1',
  renewer: 'user2',
};

const responseMock = {
  status: { statusCode: 0 },
  delegationToken: 'token',
};

function TGetDelegationTokenReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  GetDelegationToken(request, callback) {
    return callback(null, responseMock);
  },
};

describe('GetDelegationTokenCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TGetDelegationTokenReq', TGetDelegationTokenReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new GetDelegationTokenCommand(thriftClientMock);

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
