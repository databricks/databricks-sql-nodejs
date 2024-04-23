const { expect } = require('chai');
const { TCLIService_types } = require('../../../lib').thrift;
const Status = require('../../../lib/dto/Status').default;

describe('StatusFactory', () => {
  it('should be success', () => {
    const status = new Status({
      statusCode: TCLIService_types.TStatusCode.SUCCESS_STATUS,
    });

    expect(status.isSuccess).to.be.true;
    expect(status.isExecuting).to.be.false;
    expect(status.isError).to.be.false;
    expect(status.info).to.be.deep.eq([]);
  });

  it('should be success and have info messages', () => {
    const status = new Status({
      statusCode: TCLIService_types.TStatusCode.SUCCESS_WITH_INFO_STATUS,
      infoMessages: ['message1', 'message2'],
    });

    expect(status.isSuccess).to.be.true;
    expect(status.isExecuting).to.be.false;
    expect(status.isError).to.be.false;
    expect(status.info).to.be.deep.eq(['message1', 'message2']);
  });

  it('should be executing', () => {
    const status = new Status({
      statusCode: TCLIService_types.TStatusCode.STILL_EXECUTING_STATUS,
    });

    expect(status.isSuccess).to.be.false;
    expect(status.isExecuting).to.be.true;
    expect(status.isError).to.be.false;
    expect(status.info).to.be.deep.eq([]);
  });

  it('should be error', () => {
    const statusCodes = [
      TCLIService_types.TStatusCode.ERROR_STATUS,
      TCLIService_types.TStatusCode.INVALID_HANDLE_STATUS,
    ];

    for (const statusCode of statusCodes) {
      const status = new Status({
        statusCode: TCLIService_types.TStatusCode.ERROR_STATUS,
      });

      expect(status.isSuccess).to.be.false;
      expect(status.isExecuting).to.be.false;
      expect(status.isError).to.be.true;
      expect(status.info).to.be.deep.eq([]);
    }
  });

  it('should create success status object', () => {
    const status = Status.success();

    expect(status.isSuccess).to.be.true;
    expect(status.isExecuting).to.be.false;
    expect(status.isError).to.be.false;
    expect(status.info).to.be.deep.eq([]);
  });

  it('should create success status object with info messages', () => {
    const status = Status.success(['message1', 'message2']);

    expect(status.isSuccess).to.be.true;
    expect(status.isExecuting).to.be.false;
    expect(status.isError).to.be.false;
    expect(status.info).to.be.deep.eq(['message1', 'message2']);
  });

  describe('assert', () => {
    it('should throw exception on error status', () => {
      const error = expect(() => {
        Status.assert({
          statusCode: TCLIService_types.TStatusCode.ERROR_STATUS,
          errorMessage: 'error',
          errorCode: 1,
          infoMessages: ['line1', 'line2'],
        });
      }).to.throw('error');
      error.with.property('stack', 'line1\nline2');
      error.with.property('code', 1);
      error.with.property('name', 'Status Error');
    });

    it('should throw exception on invalid handle status', () => {
      const error = expect(() => {
        Status.assert({
          statusCode: TCLIService_types.TStatusCode.INVALID_HANDLE_STATUS,
          errorMessage: 'error',
        });
      }).to.throw('error');
      error.with.property('name', 'Status Error');
      error.with.property('message', 'error');
      error.with.property('code', -1);
    });

    it('should not throw exception on success and execution status', () => {
      const statusCodes = [
        TCLIService_types.TStatusCode.SUCCESS_STATUS,
        TCLIService_types.TStatusCode.SUCCESS_WITH_INFO_STATUS,
        TCLIService_types.TStatusCode.STILL_EXECUTING_STATUS,
      ];

      for (const statusCode of statusCodes) {
        expect(() => {
          Status.assert({ statusCode });
        }).to.not.throw();
      }
    });
  });
});
