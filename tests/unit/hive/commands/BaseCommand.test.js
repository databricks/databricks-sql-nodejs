const { expect, AssertionError } = require('chai');
const { Request, Response } = require('node-fetch');
const { Thrift } = require('thrift');
const HiveDriverError = require('../../../../dist/errors/HiveDriverError').default;
const BaseCommand = require('../../../../dist/hive/Commands/BaseCommand').default;
const HttpRetryPolicy = require('../../../../dist/connection/connections/HttpRetryPolicy').default;
const DBSQLClient = require('../../../../dist/DBSQLClient').default;

class ThriftClientMock {
  constructor(context, methodHandler) {
    this.context = context;
    this.methodHandler = methodHandler;
  }

  CustomMethod(request, callback) {
    try {
      const retryPolicy = new HttpRetryPolicy(this.context);
      retryPolicy
        .invokeWithRetry(this.methodHandler)
        .then((response) => {
          callback(undefined, response?.body ?? ThriftClientMock.defaultResponse);
        })
        .catch((error) => {
          callback(error);
        });
    } catch (error) {
      callback(error);
    }
  }
}

ThriftClientMock.defaultResponse = {
  status: { statusCode: 0 },
};

class CustomCommand extends BaseCommand {
  constructor(...args) {
    super(...args);
  }

  execute(request) {
    return this.executeCommand(request, this.client.CustomMethod);
  }
}

describe('BaseCommand', () => {
  it('should fail if trying to invoke non-existing command', async () => {
    const clientConfig = DBSQLClient.getDefaultConfig();

    const context = {
      getConfig: () => clientConfig,
    };

    const command = new CustomCommand({}, context);

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

    const clientConfig = DBSQLClient.getDefaultConfig();

    const context = {
      getConfig: () => clientConfig,
    };

    const command = new CustomCommand(
      {
        CustomMethod() {
          throw new Error(errorMessage);
        },
      },
      context,
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

  [429, 503].forEach((statusCode) => {
    describe(`HTTP ${statusCode} error`, () => {
      it('should fail on max retry attempts exceeded', async () => {
        const clientConfig = DBSQLClient.getDefaultConfig();

        clientConfig.retriesTimeout = 200; // ms
        clientConfig.retryDelayMin = 5; // ms
        clientConfig.retryDelayMax = 20; // ms
        clientConfig.retryMaxAttempts = 3;

        const context = {
          getConfig: () => clientConfig,
        };

        let methodCallCount = 0;
        const command = new CustomCommand(
          new ThriftClientMock(context, () => {
            methodCallCount += 1;
            const request = new Request('http://localhost/', { method: 'POST' });
            const response = new Response(undefined, {
              status: statusCode,
            });
            return { request, response };
          }),
          context,
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
          expect(methodCallCount).to.equal(clientConfig.retryMaxAttempts);
        }
      });

      it('should fail on retry timeout exceeded', async () => {
        const clientConfig = DBSQLClient.getDefaultConfig();

        clientConfig.retriesTimeout = 200; // ms
        clientConfig.retryDelayMin = 5; // ms
        clientConfig.retryDelayMax = 20; // ms
        clientConfig.retryMaxAttempts = 50;

        const context = {
          getConfig: () => clientConfig,
        };

        let methodCallCount = 0;
        const command = new CustomCommand(
          new ThriftClientMock(context, () => {
            methodCallCount += 1;
            const request = new Request('http://localhost/', { method: 'POST' });
            const response = new Response(undefined, {
              status: statusCode,
            });
            return { request, response };
          }),
          context,
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
        const clientConfig = DBSQLClient.getDefaultConfig();

        clientConfig.retriesTimeout = 200; // ms
        clientConfig.retryDelayMin = 5; // ms
        clientConfig.retryDelayMax = 20; // ms
        clientConfig.retryMaxAttempts = 5;

        const context = {
          getConfig: () => clientConfig,
        };

        let methodCallCount = 0;
        const command = new CustomCommand(
          new ThriftClientMock(context, () => {
            const request = new Request('http://localhost/', { method: 'POST' });

            methodCallCount += 1;
            if (methodCallCount <= 3) {
              const response = new Response(undefined, {
                status: statusCode,
              });
              return { request, response };
            }

            const response = new Response(undefined, {
              status: 200,
            });
            response.body = ThriftClientMock.defaultResponse;
            return { request, response };
          }),
          context,
        );

        const response = await command.execute();
        expect(response).to.deep.equal(ThriftClientMock.defaultResponse);
        expect(methodCallCount).to.equal(4); // 3 failed attempts + 1 succeeded
      });
    });
  });

  it(`should re-throw unrecognized HTTP errors`, async () => {
    const errorMessage = 'Unrecognized HTTP error';

    const clientConfig = DBSQLClient.getDefaultConfig();

    const context = {
      getConfig: () => clientConfig,
    };

    const command = new CustomCommand(
      new ThriftClientMock(context, () => {
        const error = new Thrift.TApplicationException(undefined, errorMessage);
        error.statusCode = 500;
        throw error;
      }),
      context,
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

    const clientConfig = DBSQLClient.getDefaultConfig();

    const context = {
      getConfig: () => clientConfig,
    };

    const command = new CustomCommand(
      new ThriftClientMock(context, () => {
        throw new Thrift.TApplicationException(undefined, errorMessage);
      }),
      context,
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

    const clientConfig = DBSQLClient.getDefaultConfig();

    const context = {
      getConfig: () => clientConfig,
    };

    const command = new CustomCommand(
      new ThriftClientMock(context, () => {
        throw new Error(errorMessage);
      }),
      context,
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
