const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const FetchResultsCommand = require('../../../../lib/hive/Commands/FetchResultsCommand').default;

const requestMock = {
  operationHandle: {
    sessionId: { guid: '', secret: '' },
  },
  orientation: 0,
  maxRows: 100,
  fetchType: 0,
};

const responseMock = {
  status: { statusCode: 0 },
  hasMoreRows: false,
  results: {
    startRowOffset: 0,
    rows: [
      {
        colVals: [true, 'value'],
      },
    ],
    columns: [
      {
        values: [true],
      },
      {
        values: ['value'],
      },
    ],
    binaryColumns: Buffer.from([]),
    columnCount: 2,
  },
};

function TFetchResultsReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  FetchResults(request, callback) {
    return callback(null, responseMock);
  },
};

describe('FetchResultsCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TFetchResultsReq', TFetchResultsReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new FetchResultsCommand(thriftClientMock);

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
