const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const CloseSessionCommand = require('../../../../lib/hive/Commands/CloseSessionCommand').default;

const responseMock = {
  status: { statusCode: 0 },
};

function TCloseSessionReqMock(options) {
  this.options = options;

  expect(options).has.property('sessionHandle');
}

const thriftClientMock = {
  CloseSession(request, callback) {
    return callback(null, responseMock);
  },
};

describe('CloseSessionCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TCloseSessionReq', TCloseSessionReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new CloseSessionCommand(thriftClientMock);

    command
      .execute({
        sessionHandle: {
          sessionId: { guid: '', secret: '' },
        },
      })
      .then((response) => {
        expect(response).to.be.deep.eq(responseMock);
        cb();
      })
      .catch((error) => {
        cb(error);
      });
  });
});
