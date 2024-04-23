const { expect } = require('chai');
const sinon = require('sinon');
const TCLIService_types = require('../../../../thrift/TCLIService_types');
const GetOperationStatusCommand = require('../../../../lib/hive/Commands/GetOperationStatusCommand').default;

const requestMock = {
  operationHandle: {
    hasResultSet: true,
    operationId: { guid: '', secret: '' },
    operationType: 0,
    modifiedRowCount: 0,
  },
  getProgressUpdate: true,
};

const responseMock = {
  status: { statusCode: 0 },
  operationState: 2,
  sqlState: '',
  errorCode: 0,
  errorMessage: '',
  taskStatus: '',
  operationStarted: Buffer.from([]),
  operationCompleted: Buffer.from([]),
  hasResultSet: true,
  progressUpdateResponse: {
    headerNames: [''],
    rows: [['']],
    progressedPercentage: 50,
    status: 0,
    footerSummary: '',
    startTime: Buffer.from([]),
  },
  numModifiedRows: Buffer.from([]),
};

function TGetOperationStatusReqMock(options) {
  this.options = options;

  expect(options).to.be.deep.eq(requestMock);
}

const thriftClientMock = {
  GetOperationStatus(request, callback) {
    return callback(null, responseMock);
  },
};

describe('GetOperationStatusCommand', () => {
  let sandbox;

  before(() => {
    sandbox = sinon.createSandbox();
    sandbox.replace(TCLIService_types, 'TGetOperationStatusReq', TGetOperationStatusReqMock);
  });

  after(() => {
    sandbox.restore();
  });

  it('should return response', (cb) => {
    const command = new GetOperationStatusCommand(thriftClientMock);

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
