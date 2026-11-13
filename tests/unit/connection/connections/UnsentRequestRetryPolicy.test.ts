import { expect, AssertionError } from 'chai';
import sinon from 'sinon';
import { Request, Response, FetchError } from 'node-fetch';
import UnsentRequestRetryPolicy from '../../../../lib/connection/connections/UnsentRequestRetryPolicy';
import RetryError, { RetryErrorCode } from '../../../../lib/errors/RetryError';

import ClientContextStub from '../../.stubs/ClientContextStub';

function systemError(code: string, reason = code): FetchError {
  return new FetchError(`request to https://example.com failed, reason: ${reason}`, 'system', {
    code,
  } as unknown as NodeJS.ErrnoException);
}

function okResponse() {
  return { request: new Request('http://localhost'), response: new Response(undefined, { status: 200 }) };
}

function createPolicy(retryMaxAttempts = 5) {
  return new UnsentRequestRetryPolicy(new ClientContextStub({ retryDelayMin: 1, retryDelayMax: 2, retryMaxAttempts }));
}

async function expectRejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    if (error instanceof AssertionError) {
      throw error;
    }
    return error;
  }
  return expect.fail('It should throw an error');
}

describe('UnsentRequestRetryPolicy', () => {
  for (const code of ['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN']) {
    it(`retries ${code}, which is raised before the request is sent`, async () => {
      const operation = sinon.stub();
      operation.onCall(0).rejects(systemError(code));
      operation.onCall(1).resolves(okResponse());

      const result = await createPolicy().invokeWithRetry(operation);

      expect(result.response.status).to.equal(200);
      expect(operation.callCount).to.equal(2);
    });
  }

  const sentRequestErrors: Array<[string, Error]> = [
    ['ECONNRESET (socket hang up)', systemError('ECONNRESET', 'socket hang up')],
    ['EPIPE', systemError('EPIPE')],
    ['ETIMEDOUT', systemError('ETIMEDOUT')],
    ['EHOSTUNREACH', systemError('EHOSTUNREACH')],
    ['a body timeout', new FetchError('Response timeout while trying to fetch', 'body-timeout')],
    ['a premature close', new Error('Premature close')],
  ];

  for (const [label, error] of sentRequestErrors) {
    it(`does not retry ${label}`, async () => {
      const operation = sinon.stub().rejects(error);

      expect(await expectRejection(createPolicy().invokeWithRetry(operation))).to.equal(error);
      expect(operation.callCount).to.equal(1);
    });
  }

  for (const status of [429, 500, 503]) {
    it(`does not retry an HTTP ${status} response`, async () => {
      const details = { request: new Request('http://localhost'), response: new Response(undefined, { status }) };
      const policy = createPolicy();
      const operation = sinon.stub().resolves(details);

      expect(await policy.invokeWithRetry(operation)).to.equal(details);
      expect(await policy.shouldRetry(details)).to.deep.equal({ shouldRetry: false });
      expect(operation.callCount).to.equal(1);
    });
  }

  it('rethrows the original error once max attempts is reached', async () => {
    const error = systemError('ECONNREFUSED');
    const operation = sinon.stub().rejects(error);

    expect(await expectRejection(createPolicy(3).invokeWithRetry(operation))).to.equal(error);
    expect(operation.callCount).to.equal(3);
  });

  it('rethrows the original error once the retry timeout is exceeded', async () => {
    const error = systemError('ECONNREFUSED');
    const policy = new UnsentRequestRetryPolicy(
      new ClientContextStub({ retryDelayMin: 1, retryDelayMax: 2, retryMaxAttempts: 100, retriesTimeout: 0 }),
    );
    const operation = sinon.stub().rejects(error);

    expect(await expectRejection(policy.invokeWithRetry(operation))).to.equal(error);
    expect(operation.callCount).to.equal(1);
  });

  it('passes through a RetryError raised by the operation itself', async () => {
    for (const error of [
      new RetryError(RetryErrorCode.AttemptsExceeded),
      new RetryError(RetryErrorCode.TimeoutExceeded, new TypeError('unrelated')),
    ]) {
      const operation = sinon.stub().rejects(error);

      // eslint-disable-next-line no-await-in-loop
      expect(await expectRejection(createPolicy().invokeWithRetry(operation))).to.equal(error);
      expect(operation.callCount).to.equal(1);
    }
  });
});
