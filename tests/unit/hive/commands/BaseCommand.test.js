const { expect, AssertionError } = require('chai');
const { Thrift } = require('thrift');
const HiveDriverError = require('../../../../dist/errors/HiveDriverError').default;
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
      expect(error).to.be.instanceof(HiveDriverError);
      expect(error.message).to.contain('the operation does not exist');
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

  [401, 403, 404].forEach((statusCode) => {
    describe(`HTTP ${statusCode} error`, () => {
      it(`should throw custom error`, async () => {
        const command = new CustomCommand(
          new ThriftClientMock(() => {
            const error = new Thrift.TApplicationException();
            error.statusCode = statusCode;
            throw error;
          }),
        );

        try {
          await command.execute();
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(error).to.be.instanceof(HiveDriverError);
          expect(error.message).to.contain(`${statusCode} when connecting to resource`);
        }
      });
    });
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
            const error = new Thrift.TApplicationException();
            error.statusCode = statusCode;
            throw error;
          }),
        );

        try {
          await command.execute();
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(error).to.be.instanceof(HiveDriverError);
          expect(error.message).to.contain(`${statusCode} when connecting to resource`);
          expect(error.message).to.contain('Max retry count exceeded');
          expect(methodCallCount).to.equal(globalConfig.retryMaxAttempts);
        }
      });

      it('should fail on max retry attempts exceeded', async () => {
        globalConfig.retriesTimeout = 200; // ms
        globalConfig.retryDelayMin = 5; // ms
        globalConfig.retryDelayMax = 20; // ms
        globalConfig.retryMaxAttempts = 50;

        let methodCallCount = 0;
        const command = new CustomCommand(
          new ThriftClientMock(() => {
            methodCallCount += 1;
            const error = new Thrift.TApplicationException();
            error.statusCode = statusCode;
            throw error;
          }),
        );

        try {
          await command.execute();
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(error).to.be.instanceof(HiveDriverError);
          expect(error.message).to.contain(`${statusCode} when connecting to resource`);
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
              const error = new Thrift.TApplicationException();
              error.statusCode = statusCode;
              throw error;
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
    const errorMessage = 'Unrecognized HTTP error';

    const command = new CustomCommand(
      new ThriftClientMock(() => {
        const error = new Thrift.TApplicationException(undefined, errorMessage);
        error.statusCode = 500;
        throw error;
      }),
    );

    try {
      await command.execute();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error).to.be.instanceof(Thrift.TApplicationException);
      expect(error.message).to.contain(errorMessage);
    }
  });

  it(`should re-throw unrecognized Thrift errors`, async () => {
    const errorMessage = 'Unrecognized HTTP error';

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
      expect(error).to.be.instanceof(Thrift.TApplicationException);
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
