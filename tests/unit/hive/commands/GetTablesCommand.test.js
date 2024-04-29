const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const GetTablesCommand = require('../../../../lib/hive/Commands/GetTablesCommand').default;

const requestMock = {
  sessionHandle: {
    sessionId: { guid: '', secret: '' },
  },
  catalogName: 'catalog',
  schemaName: 'schema',
  tableName: 'table',
  tableTypes: ['TABLE', 'VIEW', 'SYSTEM TABLE', 'GLOBAL TEMPORARY', 'LOCAL TEMPORARY', 'ALIAS', 'SYNONYM'],
};

const GET_TABLES = 4;

const responseMock = {
  status: { statusCode: 0 },
  operationHandle: {
    hasResultSet: true,
    operationId: { guid: '', secret: '' },
    operationType: GET_TABLES,
    modifiedRowCount: 0,
  },
};

function TGetTablesReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  GetTables(request, callback) {
    return callback(null, responseMock);
  },
};

describe('GetTablesCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TGetTablesReq', TGetTablesReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new GetTablesCommand(thriftClientMock);

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
