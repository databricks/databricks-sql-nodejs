import { expect, AssertionError } from 'chai';
import sinon from 'sinon';
import { Request, Response, FetchError } from 'node-fetch';
import NonIdempotentRetryPolicy from '../../../../lib/connection/connections/NonIdempotentRetryPolicy';
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
  return new NonIdempotentRetryPolicy(new ClientContextStub({ retryDelayMin: 1, retryDelayMax: 2, retryMaxAttempts }));
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

function statusResponse(status: number, headers: Record<string, string> = {}) {
  return { request: new Request('http://localhost'), response: new Response(undefined, { status, headers }) };
}

describe('NonIdempotentRetryPolicy', () => {
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

  const serverRejections: Array<[string, ReturnType<typeof statusResponse>]> = [
    ['HTTP 429', statusResponse(429)],
    ['HTTP 429 with Retry-After', statusResponse(429, { 'Retry-After': '1' })],
    ['HTTP 503 with Retry-After', statusResponse(503, { 'Retry-After': '1' })],
  ];

  for (const [label, rejection] of serverRejections) {
    it(`retries an ${label} response, which the server did not run`, async () => {
      const operation = sinon.stub();
      operation.onCall(0).resolves(rejection);
      operation.onCall(1).resolves(okResponse());

      const result = await createPolicy().invokeWithRetry(operation);

      expect(result.response.status).to.equal(200);
      expect(operation.callCount).to.equal(2);
    });
  }

  it('throws a RetryError once a persistent 429 runs out of attempts', async () => {
    const operation = sinon.stub().resolves(statusResponse(429));

    const error = await expectRejection(createPolicy(3).invokeWithRetry(operation));

    expect(error).to.be.instanceOf(RetryError);
    expect((error as RetryError).errorCode).to.equal(RetryErrorCode.AttemptsExceeded);
    expect(operation.callCount).to.equal(3);
  });

  const ambiguousStatuses: Array<[string, ReturnType<typeof statusResponse>]> = [
    ['HTTP 503 without Retry-After', statusResponse(503)],
    ['HTTP 500', statusResponse(500)],
    ['HTTP 500 with Retry-After', statusResponse(500, { 'Retry-After': '1' })],
    ['HTTP 502', statusResponse(502)],
    ['HTTP 504', statusResponse(504)],
    ['HTTP 408', statusResponse(408)],
  ];

  for (const [label, details] of ambiguousStatuses) {
    it(`does not retry an ${label} response`, async () => {
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
    const policy = new NonIdempotentRetryPolicy(
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
