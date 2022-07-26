const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const OpenSessionCommand = require('../../../../dist/hive/Commands/OpenSessionCommand').default;

const CLIENT_PROTOCOL = 8;

const responseMock = {
  status: { statusCode: 0 },
  serverProtocolVersion: CLIENT_PROTOCOL,
  sessionHandle: {
    sessionId: { guid: '', secret: '' },
  },
  configuration: {},
};

function TOpenSessionReqMock(options) {
  this.options = options;

  expect(options.client_protocol).to.be.eq(CLIENT_PROTOCOL);
}

const thriftClientMock = {
  OpenSession(request, callback) {
    return callback(null, responseMock);
  },
};

describe('OpenSessionCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TOpenSessionReq', TOpenSessionReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new OpenSessionCommand(thriftClientMock);

    command
      .execute({
        client_protocol: CLIENT_PROTOCOL,
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
