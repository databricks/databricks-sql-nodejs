import { expect } from 'chai';
import { TStatusCode } from '../../../thrift/TCLIService_types';
import Status from '../../../lib/dto/Status';
import StatusError from '../../../lib/errors/StatusError';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

describe('StatusFactory', () => {
  it('should be success', () => {
    const status = new Status({
      statusCode: TStatusCode.SUCCESS_STATUS,
    });

    expect(status.isSuccess).to.be.true;
    expect(status.isExecuting).to.be.false;
    expect(status.isError).to.be.false;
    expect(status.info).to.be.deep.eq([]);
  });

  it('should be success and have info messages', () => {
    const status = new Status({
      statusCode: TStatusCode.SUCCESS_WITH_INFO_STATUS,
      infoMessages: ['message1', 'message2'],
    });

    expect(status.isSuccess).to.be.true;
    expect(status.isExecuting).to.be.false;
    expect(status.isError).to.be.false;
    expect(status.info).to.be.deep.eq(['message1', 'message2']);
  });

  it('should be executing', () => {
    const status = new Status({
      statusCode: TStatusCode.STILL_EXECUTING_STATUS,
    });

    expect(status.isSuccess).to.be.false;
    expect(status.isExecuting).to.be.true;
    expect(status.isError).to.be.false;
    expect(status.info).to.be.deep.eq([]);
  });

  it('should be error', () => {
    const statusCodes = [TStatusCode.ERROR_STATUS, TStatusCode.INVALID_HANDLE_STATUS];

    for (const statusCode of statusCodes) {
      const status = new Status({
        statusCode: TStatusCode.ERROR_STATUS,
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
      let caught: unknown;
      try {
        Status.assert({
          statusCode: TStatusCode.ERROR_STATUS,
          errorMessage: 'error',
          errorCode: 0,
          sqlState: '22023',
          infoMessages: ['line1', 'line2'],
        });
      } catch (error) {
        caught = error;
      }

      expect(caught).to.be.instanceOf(StatusError);
      expect(caught).to.be.instanceOf(HiveDriverError);
      expect(caught).to.be.instanceOf(Error);
      const statusError = caught as StatusError;
      expect(statusError.message).to.equal('error');
      expect(statusError.stack).to.equal('line1\nline2');
      expect(statusError.code).to.equal(0);
      expect(statusError.sqlState).to.equal('22023');
      expect(statusError.name).to.equal('Status Error');
    });

    it('should throw exception on invalid handle status', () => {
      const error = expect(() => {
        Status.assert({
          statusCode: TStatusCode.INVALID_HANDLE_STATUS,
          errorMessage: 'error',
        });
      }).to.throw('error');
      error.with.property('name', 'Status Error');
      error.with.property('message', 'error');
      error.with.property('code', -1);
    });

    it('should not throw exception on success and execution status', () => {
      const statusCodes = [
        TStatusCode.SUCCESS_STATUS,
        TStatusCode.SUCCESS_WITH_INFO_STATUS,
        TStatusCode.STILL_EXECUTING_STATUS,
      ];

      for (const statusCode of statusCodes) {
        expect(() => {
          Status.assert({ statusCode });
        }).to.not.throw();
      }
    });
  });
});
