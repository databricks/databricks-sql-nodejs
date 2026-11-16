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
  // Failures to connect (the kernel's `is_connect`): the request never left the client
  const connectErrors: Array<[string, Error]> = [
    ['ECONNREFUSED', systemError('ECONNREFUSED', 'connect ECONNREFUSED 10.0.0.1:443')],
    ['EAI_AGAIN', systemError('EAI_AGAIN', 'getaddrinfo EAI_AGAIN example.com')],
    ['a connect timeout', systemError('ETIMEDOUT', 'connect ETIMEDOUT 10.0.0.1:443')],
    ['an unreachable host', systemError('EHOSTUNREACH', 'connect EHOSTUNREACH 10.0.0.1:443')],
    ['an unreachable network', systemError('ENETUNREACH', 'connect ENETUNREACH 10.0.0.1:443')],
    [
      'a socket closed during the TLS handshake',
      systemError('ECONNRESET', 'Client network socket disconnected before secure TLS connection was established'),
    ],
    // A host with several addresses: Node's AggregateError has the code but no message
    ['a connect timeout on every address', systemError('ETIMEDOUT', '')],
    ['an unreachable host on every address', systemError('EHOSTUNREACH', '')],
    ['an unreachable network on every address', systemError('ENETUNREACH', '')],
  ];

  for (const [label, error] of connectErrors) {
    it(`retries ${label}, which is raised before the request is sent`, async () => {
      const operation = sinon.stub();
      operation.onCall(0).rejects(error);
      operation.onCall(1).resolves(okResponse());

      const result = await createPolicy().invokeWithRetry(operation);

      expect(result.response.status).to.equal(200);
      expect(operation.callCount).to.equal(2);
    });
  }

  const sentRequestErrors: Array<[string, Error]> = [
    ['ECONNRESET (socket hang up)', systemError('ECONNRESET', 'socket hang up')],
    ['EPIPE', systemError('EPIPE')],
    ['ETIMEDOUT on a connected socket', systemError('ETIMEDOUT', 'read ETIMEDOUT')],
    ['EHOSTUNREACH on a connected socket', systemError('EHOSTUNREACH', 'write EHOSTUNREACH')],
    ['a body timeout', new FetchError('Response timeout while trying to fetch', 'body-timeout')],
    ['a premature close', new Error('Premature close')],
    ['ENOTFOUND, a host name that does not resolve', systemError('ENOTFOUND')],
    ['a request timeout', new FetchError('network timeout at: https://example.com', 'request-timeout')],
  ];

  for (const [label, error] of sentRequestErrors) {
    it(`does not retry ${label}`, async () => {
      const operation = sinon.stub().rejects(error);

      expect(await expectRejection(createPolicy().invokeWithRetry(operation))).to.equal(error);
      expect(operation.callCount).to.equal(1);
    });
  }

  function statusResponse(status: number, headers: Record<string, string> = {}) {
    return { request: new Request('http://localhost'), response: new Response(undefined, { status, headers }) };
  }

  for (const [status, headers] of [
    [500, {}],
    [501, {}],
    [502, {}],
    [503, {}],
    [504, {}],
    [408, {}],
    [500, { 'Retry-After': '1' }],
  ] as const) {
    it(`does not retry an HTTP ${status} response ${
      Object.keys(headers).length ? 'with' : 'without'
    } Retry-After`, async () => {
      const details = statusResponse(status, headers);
      const policy = createPolicy();
      const operation = sinon.stub().resolves(details);

      expect(await policy.invokeWithRetry(operation)).to.equal(details);
      expect(await policy.shouldRetry(details)).to.deep.equal({ shouldRetry: false });
      expect(operation.callCount).to.equal(1);
    });
  }

  for (const [label, rejection] of [
    ['an HTTP 503 with Retry-After', statusResponse(503, { 'Retry-After': '1' })],
    ['an HTTP 429', statusResponse(429)],
    ['an HTTP 429 with Retry-After', statusResponse(429, { 'Retry-After': '1' })],
  ] as const) {
    it(`retries ${label}, which the server did not run`, async () => {
      const operation = sinon.stub();
      operation.onCall(0).resolves(rejection);
      operation.onCall(1).resolves(okResponse());

      const result = await createPolicy().invokeWithRetry(operation);

      expect(result.response.status).to.equal(200);
      expect(operation.callCount).to.equal(2);
    });
  }

  it('waits the Retry-After delay before retrying a 503, with the default config', async () => {
    const policy = new UnsentRequestRetryPolicy(new ClientContextStub());

    expect(await policy.shouldRetry(statusResponse(503, { 'Retry-After': '5' }))).to.deep.equal({
      shouldRetry: true,
      retryAfter: 5_000,
    });
  });

  it('throws a RetryError once a persistent 503 with Retry-After runs out of attempts', async () => {
    const operation = sinon.stub().resolves(statusResponse(503, { 'Retry-After': '1' }));

    const error = await expectRejection(createPolicy(3).invokeWithRetry(operation));

    expect(error).to.be.instanceOf(RetryError);
    expect((error as RetryError).errorCode).to.equal(RetryErrorCode.AttemptsExceeded);
    expect(operation.callCount).to.equal(3);
  });

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
