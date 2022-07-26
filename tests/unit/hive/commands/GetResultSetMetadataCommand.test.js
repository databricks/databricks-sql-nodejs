const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const GetResultSetMetadataCommand = require('../../../../dist/hive/Commands/GetResultSetMetadataCommand').default;

const requestMock = {
  operationHandle: {
    sessionId: { guid: '', secret: '' },
  },
};

const responseMock = {
  status: { statusCode: 0 },
  schema: {
    columns: [
      {
        columnName: 'column1',
        typeDesc: {
          types: [
            {
              type: 0,
            },
          ],
        },
        position: 0,
        comment: '',
      },
    ],
  },
};

function TGetResultSetMetadataReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  GetResultSetMetadata(request, callback) {
    return callback(null, responseMock);
  },
};

describe('GetResultSetMetadataCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TGetResultSetMetadataReq', TGetResultSetMetadataReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new GetResultSetMetadataCommand(thriftClientMock);

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
