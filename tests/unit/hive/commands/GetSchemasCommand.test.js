const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const GetSchemasCommand = require('../../../../dist/hive/Commands/GetSchemasCommand').default;

const requestMock = {
  sessionHandle: {
    sessionId: { guid: '', secret: '' },
  },
  catalogName: 'catalog',
  schemaName: 'schema',
};

const GET_SCHEMAS = 3;

const responseMock = {
  status: { statusCode: 0 },
  operationHandle: {
    hasResultSet: true,
    operationId: { guid: '', secret: '' },
    operationType: GET_SCHEMAS,
    modifiedRowCount: 0,
  },
};

function TGetSchemasReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  GetSchemas(request, callback) {
    return callback(null, responseMock);
  },
};

describe('GetSchemasCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TGetSchemasReq', TGetSchemasReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new GetSchemasCommand(thriftClientMock);

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
