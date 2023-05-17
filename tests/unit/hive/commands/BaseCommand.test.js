const { expect, AssertionError } = require('chai');
const { Thrift } = require('thrift');
const { IncomingMessage } = require('http');
const DriverError = require('../../../../dist/errors/DriverError').default;
const { default: RetryError, RetryErrorCode } = require('../../../../dist/errors/RetryError');
const TransportError = require('../../../../dist/errors/TransportError').default;
const BaseCommand = require('../../../../dist/hive/Commands/BaseCommand').default;
const globalConfig = require('../../../../dist/globalConfig').default;

const savedGlobalConfig = { ...globalConfig };

class ThriftClientMock {
  constructor(methodHandler) {
    this.methodHandler = methodHandler;
  }

  CustomMethod(request, callback) {
    try {
      const response = this.methodHandler();
      return callback(undefined, response !== undefined ? response : ThriftClientMock.defaultResponse);
    } catch (error) {
      return callback(error);
    }
  }
}

ThriftClientMock.defaultResponse = {
  status: { statusCode: 0 },
};

class CustomCommand extends BaseCommand {
  execute(request) {
    return this.executeCommand(request, this.client.CustomMethod);
  }
}

// Mock THTTPException from `thrift` (not exported from the library, but used in http_connection)
class ThriftHTTPException extends Thrift.TApplicationException {
  constructor(statusCode, headers) {
    super(
      Thrift.TApplicationExceptionType.PROTOCOL_ERROR,
      `Received a response with a bad HTTP status code: ${statusCode}`,
    );
    this.statusCode = statusCode;
    this.response = new IncomingMessage({});
    this.response.statusCode = statusCode;
    this.response.headers = { ...headers };
  }
}

describe('BaseCommand', () => {
  afterEach(() => {
    Object.assign(globalConfig, savedGlobalConfig);
  });

  it('should fail if trying to invoke non-existing command', async () => {
    const command = new CustomCommand({});

    try {
      await command.execute();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error).to.be.instanceof(DriverError);
      expect(error.message).to.contain('The operation does not exist');
    }
  });

  it('should handle exceptions thrown by command', async () => {
    const errorMessage = 'Unexpected error';

    const command = new CustomCommand({
      CustomMethod() {
        throw new Error(errorMessage);
      },
    });

    try {
      await command.execute();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error).to.be.instanceof(Error);
      expect(error.message).to.contain(errorMessage);
    }
  });

  [429, 503].forEach((statusCode) => {
    describe(`HTTP ${statusCode} error`, () => {
      it('should fail on max retry attempts exceeded', async () => {
        globalConfig.retriesTimeout = 200; // ms
        globalConfig.retryDelayMin = 5; // ms
        globalConfig.retryDelayMax = 20; // ms
        globalConfig.retryMaxAttempts = 3;

        let methodCallCount = 0;
        const command = new CustomCommand(
          new ThriftClientMock(() => {
            methodCallCount += 1;
            throw new ThriftHTTPException(statusCode);
          }),
        );

        try {
          await command.execute();
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }

          expect(error).to.be.instanceof(RetryError);
          expect(error.errorCode).to.equal(RetryErrorCode.OutOfAttempts);
          expect(error.message).to.contain('Max retry count exceeded');
          expect(methodCallCount).to.equal(globalConfig.retryMaxAttempts);
        }
      });

      it('should fail on retry timeout exceeded', async () => {
        globalConfig.retriesTimeout = 200; // ms
        globalConfig.retryDelayMin = 5; // ms
        globalConfig.retryDelayMax = 20; // ms
        globalConfig.retryMaxAttempts = 50;

        let methodCallCount = 0;
        const command = new CustomCommand(
          new ThriftClientMock(() => {
            methodCallCount += 1;
            throw new ThriftHTTPException(statusCode);
          }),
        );

        try {
          await command.execute();
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(error).to.be.instanceof(RetryError);
          expect(error.errorCode).to.equal(RetryErrorCode.OutOfTime);
          expect(error.message).to.contain('Retry timeout exceeded');
          // We set pretty low intervals/timeouts to make this test pass faster, but it also means
          // that it's harder to predict how much times command will be invoked. So we check that
          // it is within some meaningful range to reduce false-negatives
          expect(methodCallCount).to.be.within(10, 20);
        }
      });

      it('should succeed after few attempts', async () => {
        globalConfig.retriesTimeout = 200; // ms
        globalConfig.retryDelayMin = 5; // ms
        globalConfig.retryDelayMax = 20; // ms
        globalConfig.retryMaxAttempts = 5;

        let methodCallCount = 0;
        const command = new CustomCommand(
          new ThriftClientMock(() => {
            methodCallCount += 1;
            if (methodCallCount <= 3) {
              throw new ThriftHTTPException(statusCode);
            }
            return ThriftClientMock.defaultResponse;
          }),
        );

        const response = await command.execute();
        expect(response).to.deep.equal(ThriftClientMock.defaultResponse);
        expect(methodCallCount).to.equal(4); // 3 failed attempts + 1 succeeded
      });
    });
  });

  it(`should re-throw unrecognized HTTP errors`, async () => {
    const command = new CustomCommand(
      new ThriftClientMock(() => {
        throw new ThriftHTTPException(500);
      }),
    );

    try {
      await command.execute();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error).to.be.instanceof(TransportError);
      expect(error.message).to.contain('HTTP status code: 500');
    }
  });

  it(`should re-throw unrecognized Thrift errors`, async () => {
    const errorMessage = 'Unrecognized error';

    const command = new CustomCommand(
      new ThriftClientMock(() => {
        throw new Thrift.TApplicationException(undefined, errorMessage);
      }),
    );

    try {
      await command.execute();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error).to.be.instanceof(DriverError);
      expect(error.message).to.contain(errorMessage);
    }
  });

  it(`should re-throw unrecognized errors`, async () => {
    const errorMessage = 'Unrecognized error';

    const command = new CustomCommand(
      new ThriftClientMock(() => {
        throw new Error(errorMessage);
      }),
    );

    try {
      await command.execute();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error).to.be.instanceof(Error);
      expect(error.message).to.contain(errorMessage);
    }
  });
});
