import { expect, AssertionError } from 'chai';
import sinon from 'sinon';
import { Request, Response } from 'node-fetch';
import { Thrift } from 'thrift';
import HiveDriverError from '../../../../lib/errors/HiveDriverError';
import BaseCommand from '../../../../lib/hive/Commands/BaseCommand';
import HttpRetryPolicy from '../../../../lib/connection/connections/HttpRetryPolicy';
import { THTTPException } from '../../../../lib/connection/connections/ThriftHttpConnection';
import { HttpTransactionDetails } from '../../../../lib/connection/contracts/IConnectionProvider';
import IClientContext from '../../../../lib/contracts/IClientContext';

import ClientContextStub from '../../.stubs/ClientContextStub';

class TCustomReq {}

class TCustomResp {}

class ThriftClientMock {
  static defaultResponse = {
    status: { statusCode: 0 },
  };

  private readonly context: IClientContext;

  private readonly methodHandler: () => Promise<HttpTransactionDetails>;

  constructor(context: IClientContext, methodHandler: () => Promise<HttpTransactionDetails>) {
    this.context = context;
    this.methodHandler = methodHandler;
  }

  CustomMethod(req: TCustomReq, callback?: (error: any, resp?: TCustomResp) => void) {
    try {
      const retryPolicy = new HttpRetryPolicy(this.context);
      retryPolicy
        .invokeWithRetry(this.methodHandler)
        .then(({ response }) => response.json())
        .then((response) => {
          callback?.(undefined, response);
        })
        .catch?.((error) => {
          callback?.(error, undefined);
        });
    } catch (error) {
      callback?.(error, undefined);
    }
  }
}

class CustomCommand extends BaseCommand<ThriftClientMock> {
  public async execute(request: TCustomReq): Promise<TCustomResp> {
    return this.executeCommand<TCustomResp>(request, this.client.CustomMethod);
  }
}

describe('BaseCommand', () => {
  it('should fail if trying to invoke non-existing command', async () => {
    const context = new ClientContextStub();

    // Here we have a special test condition - when invalid Thrift client is passed to
    // a command. Normally TS should catch this (and therefore we have a type cast here),
    // but there is an additional check in the code, which we need to verify as well
    const command = new CustomCommand({} as ThriftClientMock, context);

    try {
      await command.execute({});
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error).to.be.instanceof(HiveDriverError);
      expect(error.message).to.contain('the operation does not exist');
    }
  });

  it('should handle exceptions thrown by command', async () => {
    const errorMessage = 'Unexpected error';

    const context = new ClientContextStub();

    const thriftClient = new ThriftClientMock(context, async () => {
      throw new Error('Not implemented');
    });
    sinon.stub(thriftClient, 'CustomMethod').callsFake(() => {
      throw new Error(errorMessage);
    });

    const command = new CustomCommand(thriftClient, context);

    try {
      await command.execute({});
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error).to.be.instanceof(Error);
      expect(error.message).to.contain(errorMessage);
    }
  });

  [429, 503].forEach((statusCode) => {
    describe(`HTTP ${statusCode} error`, () => {
      it('should fail on max retry attempts exceeded', async () => {
        const context = new ClientContextStub({
          retriesTimeout: 200, // ms
          retryDelayMin: 5, // ms
          retryDelayMax: 20, // ms
          retryMaxAttempts: 3,
        });

        let methodCallCount = 0;
        const command = new CustomCommand(
          new ThriftClientMock(context, async () => {
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
          await command.execute({});
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError || !(error instanceof Error)) {
            throw error;
          }
          expect(error).to.be.instanceof(HiveDriverError);
          expect(error.message).to.contain(`${statusCode} when connecting to resource`);
          expect(error.message).to.contain('Max retry count exceeded');
          expect(methodCallCount).to.equal(context.getConfig().retryMaxAttempts);
        }
      });

      it('should fail on retry timeout exceeded', async () => {
        const context = new ClientContextStub({
          retriesTimeout: 200, // ms
          retryDelayMin: 5, // ms
          retryDelayMax: 20, // ms
          retryMaxAttempts: 50,
        });

        let methodCallCount = 0;
        const command = new CustomCommand(
          new ThriftClientMock(context, async () => {
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
          await command.execute({});
          expect.fail('It should throw an error');
        } catch (error) {
          if (error instanceof AssertionError || !(error instanceof Error)) {
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
        const context = new ClientContextStub({
          retriesTimeout: 200, // ms
          retryDelayMin: 5, // ms
          retryDelayMax: 20, // ms
          retryMaxAttempts: 5,
        });

        let methodCallCount = 0;
        const command = new CustomCommand(
          new ThriftClientMock(context, async () => {
            const request = new Request('http://localhost/', { method: 'POST' });

            methodCallCount += 1;
            if (methodCallCount <= 3) {
              const response = new Response(undefined, {
                status: statusCode,
              });
              return { request, response };
            }

            const response = new Response(JSON.stringify(ThriftClientMock.defaultResponse), {
              status: 200,
            });
            return { request, response };
          }),
          context,
        );

        const response = await command.execute({});
        expect(response).to.deep.equal(ThriftClientMock.defaultResponse);
        expect(methodCallCount).to.equal(4); // 3 failed attempts + 1 succeeded
      });
    });
  });

  it(`should re-throw unrecognized HTTP errors`, async () => {
    const context = new ClientContextStub();

    const command = new CustomCommand(
      new ThriftClientMock(context, async () => {
        throw new THTTPException(
          new Response(undefined, {
            status: 500,
          }),
        );
      }),
      context,
    );

    try {
      await command.execute({});
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error).to.be.instanceof(Thrift.TApplicationException);
      expect(error.message).to.contain('bad HTTP status code');
    }
  });

  it(`should re-throw unrecognized Thrift errors`, async () => {
    const errorMessage = 'Unrecognized HTTP error';

    const context = new ClientContextStub();

    const command = new CustomCommand(
      new ThriftClientMock(context, async () => {
        throw new Thrift.TApplicationException(undefined, errorMessage);
      }),
      context,
    );

    try {
      await command.execute({});
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error).to.be.instanceof(Thrift.TApplicationException);
      expect(error.message).to.contain(errorMessage);
    }
  });

  it(`should re-throw unrecognized errors`, async () => {
    const errorMessage = 'Unrecognized error';

    const context = new ClientContextStub();

    const command = new CustomCommand(
      new ThriftClientMock(context, async () => {
        throw new Error(errorMessage);
      }),
      context,
    );

    try {
      await command.execute({});
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError || !(error instanceof Error)) {
        throw error;
      }
      expect(error.message).to.contain(errorMessage);
    }
  });
});
