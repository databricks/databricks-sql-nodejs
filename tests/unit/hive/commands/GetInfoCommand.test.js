const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const GetInfoCommand = require('../../../../dist/hive/Commands/GetInfoCommand').default;

const requestMock = {
  sessionHandle: {
    sessionId: { guid: '', secret: '' },
  },
  infoType: 0,
};

const responseMock = {
  status: { statusCode: 0 },
  infoValue: {
    stringValue: '',
    smallIntValue: 0,
    integerBitmask: 1,
    integerFlag: 0,
    binaryValue: Buffer.from([]),
    lenValue: Buffer.from([]),
  },
};

function TGetInfoReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  GetInfo(request, callback) {
    return callback(null, responseMock);
  },
};

describe('GetInfoCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TGetInfoReq', TGetInfoReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new GetInfoCommand(thriftClientMock);

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
