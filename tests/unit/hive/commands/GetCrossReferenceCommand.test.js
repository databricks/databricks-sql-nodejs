const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const GetCrossReferenceCommand = require('../../../../lib/hive/Commands/GetCrossReferenceCommand').default;

const requestMock = {
  sessionHandle: {
    sessionId: { guid: '', secret: '' },
  },
  parentCatalogName: 'parentCatalogName',
  parentSchemaName: 'parentSchemaName',
  parentTableName: 'parentTableName',
  foreignCatalogName: 'foreignCatalogName',
  foreignSchemaName: 'foreignSchemaName',
  foreignTableName: 'foreignTableName',
};

const GET_CROSS_REFERENCE = 7;

const responseMock = {
  status: { statusCode: 0 },
  operationHandle: {
    hasResultSet: true,
    operationId: { guid: '', secret: '' },
    operationType: GET_CROSS_REFERENCE,
    modifiedRowCount: 0,
  },
};

function TGetCrossReferenceReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  GetCrossReference(request, callback) {
    return callback(null, responseMock);
  },
};

describe('GetCrossReferenceCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TGetCrossReferenceReq', TGetCrossReferenceReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new GetCrossReferenceCommand(thriftClientMock);

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
