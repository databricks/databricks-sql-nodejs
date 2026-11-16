import { expect, AssertionError } from 'chai';
import sinon from 'sinon';
import { Request, Response, HeadersInit, FetchError } from 'node-fetch';
import HttpRetryPolicy from '../../../../lib/connection/connections/HttpRetryPolicy';
import RetryError, { RetryErrorCode } from '../../../../lib/errors/RetryError';

import ClientContextStub from '../../.stubs/ClientContextStub';

describe('HttpRetryPolicy', () => {
  it('should properly compute backoff delay', async () => {
    const context = new ClientContextStub({ retryDelayMin: 3, retryDelayMax: 20 });
    const { retryDelayMin, retryDelayMax } = context.getConfig();
    const policy = new HttpRetryPolicy(context);

    expect(policy['getBackoffDelay'](0, retryDelayMin, retryDelayMax)).to.equal(3);
    expect(policy['getBackoffDelay'](1, retryDelayMin, retryDelayMax)).to.equal(6);
    expect(policy['getBackoffDelay'](2, retryDelayMin, retryDelayMax)).to.equal(12);
    expect(policy['getBackoffDelay'](3, retryDelayMin, retryDelayMax)).to.equal(retryDelayMax);
    expect(policy['getBackoffDelay'](4, retryDelayMin, retryDelayMax)).to.equal(retryDelayMax);
  });

  it('should extract delay from `Retry-After` header', async () => {
    const context = new ClientContextStub({ retryDelayMin: 3, retryDelayMax: 20 });
    const { retryDelayMin } = context.getConfig();
    const policy = new HttpRetryPolicy(context);

    function createStub(headers: HeadersInit) {
      return {
        request: new Request('http://localhost'),
        response: new Response(undefined, { headers }),
      };
    }

    // Missing `Retry-After` header
    expect(policy['getRetryAfterHeader'](createStub({}), retryDelayMin)).to.be.undefined;

    // Valid `Retry-After` in seconds, several header name variants, returned in milliseconds
    expect(policy['getRetryAfterHeader'](createStub({ 'Retry-After': '10' }), retryDelayMin)).to.equal(10_000);
    expect(policy['getRetryAfterHeader'](createStub({ 'retry-after': '10' }), retryDelayMin)).to.equal(10_000);
    expect(policy['getRetryAfterHeader'](createStub({ 'RETRY-AFTER': '10' }), retryDelayMin)).to.equal(10_000);

    // Invalid header values (non-numeric, negative)
    expect(policy['getRetryAfterHeader'](createStub({ 'Retry-After': 'test' }), retryDelayMin)).to.be.undefined;
    expect(policy['getRetryAfterHeader'](createStub({ 'Retry-After': '-10' }), retryDelayMin)).to.be.undefined;

    // It should not be smaller than min value, but can be greater than max value
    expect(policy['getRetryAfterHeader'](createStub({ 'Retry-After': '0.001' }), retryDelayMin)).to.equal(
      retryDelayMin,
    );
    expect(policy['getRetryAfterHeader'](createStub({ 'Retry-After': '200' }), retryDelayMin)).to.equal(200_000);
  });

  it('should check if HTTP transaction is safe to retry', async () => {
    const policy = new HttpRetryPolicy(new ClientContextStub());

    function createStub(status: number) {
      return {
        request: new Request('http://localhost'),
        response: new Response(undefined, { status }),
      };
    }

    // Status codes below 100 can be retried
    for (let status = 1; status < 100; status += 1) {
      expect(policy['isRetryable'](createStub(status))).to.be.true;
    }

    // Status codes between 100 (including) and 500 (excluding) should not be retried
    // The only exceptions are 408 (Request Timeout) and 429 (Too many requests)
    for (let status = 100; status < 500; status += 1) {
      const expectedResult = status === 408 || status === 429;
      expect(policy['isRetryable'](createStub(status))).to.equal(expectedResult);
    }

    // Status codes above 500 can be retried, except for 501
    for (let status = 500; status < 1000; status += 1) {
      const expectedResult = status === 501 ? false : true;
      expect(policy['isRetryable'](createStub(status))).to.equal(expectedResult);
    }
  });

  describe('shouldRetry', () => {
    it('should not retry if transaction succeeded', async () => {
      const context = new ClientContextStub({ retryMaxAttempts: 3 });
      const clientConfig = context.getConfig();
      const policy = new HttpRetryPolicy(context);

      function createStub(status: number) {
        return {
          request: new Request('http://localhost'),
          response: new Response(undefined, { status }),
        };
      }

      // Try several times to make sure it doesn't increment an attempts counter
      for (let attempt = 1; attempt <= clientConfig.retryMaxAttempts + 1; attempt += 1) {
        const result = await policy.shouldRetry(createStub(200));
        expect(result.shouldRetry).to.be.false;
        expect(policy['attempt']).to.equal(0);
      }

      // Make sure it doesn't trigger timeout when not needed
      policy['startTime'] = Date.now() - clientConfig.retriesTimeout * 2;
      const result = await policy.shouldRetry(createStub(200));
      expect(result.shouldRetry).to.be.false;
    });

    it('should wait as long as `Retry-After` asks, within retryDelayMin and retryDelayMax', async () => {
      const context = new ClientContextStub({ retryDelayMin: 1000, retryDelayMax: 60_000, retryMaxAttempts: 10 });
      const policy = new HttpRetryPolicy(context);

      function createStub(headers: HeadersInit) {
        return {
          request: new Request('http://localhost'),
          response: new Response(undefined, { status: 503, headers }),
        };
      }

      // The header is the wait, whatever the attempt (no exponential growth)
      expect(await policy.shouldRetry(createStub({ 'Retry-After': '5' }))).to.deep.equal({
        shouldRetry: true,
        retryAfter: 5_000,
      });
      expect(await policy.shouldRetry(createStub({ 'Retry-After': '30' }))).to.deep.equal({
        shouldRetry: true,
        retryAfter: 30_000,
      });
      // Clamped up to retryDelayMin and down to retryDelayMax
      expect(await policy.shouldRetry(createStub({ 'Retry-After': '0.2' }))).to.deep.equal({
        shouldRetry: true,
        retryAfter: 1_000,
      });
      expect(await policy.shouldRetry(createStub({ 'Retry-After': '600' }))).to.deep.equal({
        shouldRetry: true,
        retryAfter: 60_000,
      });
    });

    it('should not wait for a `Retry-After` that would end after retriesTimeout', async () => {
      const shortBudget = { retryDelayMin: 1000, retryDelayMax: 60_000, retriesTimeout: 5000 };

      function createStub(retryAfter: string) {
        return {
          request: new Request('http://localhost', { method: 'POST' }),
          response: new Response(undefined, { status: 429, headers: { 'Retry-After': retryAfter } }),
        };
      }

      const operation = sinon.stub().resolves(createStub('60'));
      const started = Date.now();
      try {
        await new HttpRetryPolicy(new ClientContextStub(shortBudget)).invokeWithRetry(operation);
        expect.fail('It should throw an error');
      } catch (error) {
        if (error instanceof AssertionError || !(error instanceof Error)) {
          throw error;
        }
        expect(error).to.be.instanceOf(RetryError);
        expect((error as RetryError).errorCode).to.equal(RetryErrorCode.TimeoutExceeded);
      }
      expect(operation.callCount).to.equal(1);
      expect(Date.now() - started).to.be.lt(1000);

      // A wait that fits is kept
      const fitting = new HttpRetryPolicy(new ClientContextStub(shortBudget));
      expect(await fitting.shouldRetry(createStub('3'))).to.deep.equal({ shouldRetry: true, retryAfter: 3000 });
    });

    it('should use backoff when `Retry-After` header is missing', async () => {
      const context = new ClientContextStub({
        retryDelayMin: 3,
        retryDelayMax: 20,
        retryMaxAttempts: Number.POSITIVE_INFINITY, // remove limit on max attempts
      });
      const clientConfig = context.getConfig();
      const policy = new HttpRetryPolicy(context);

      function createStub(headers: HeadersInit) {
        return {
          request: new Request('http://localhost'),
          response: new Response(undefined, { status: 500, headers }),
        };
      }

      const result1 = await policy.shouldRetry(createStub({}));
      expect(result1.shouldRetry).to.be.true;
      if (result1.shouldRetry) {
        expect(result1.retryAfter).to.equal(6);
      }

      policy['attempt'] = 4;
      const result2 = await policy.shouldRetry(createStub({ 'Retry-After': 'test' }));
      expect(result2.shouldRetry).to.be.true;
      if (result2.shouldRetry) {
        expect(result2.retryAfter).to.equal(clientConfig.retryDelayMax);
      }
    });

    it('should check if retry timeout reached', async () => {
      const context = new ClientContextStub();
      const clientConfig = context.getConfig();
      const policy = new HttpRetryPolicy(context);

      function createStub() {
        return {
          request: new Request('http://localhost', { method: 'POST' }),
          response: new Response(undefined, { status: 500 }),
        };
      }

      const result = await policy.shouldRetry(createStub());
      expect(result.shouldRetry).to.be.true;

      // Modify start time to be in the past so the next `shouldRetry` would fail
      policy['startTime'] = Date.now() - clientConfig.retriesTimeout * 2;
      try {
        await policy.shouldRetry(createStub());
        expect.fail('It should throw an error');
      } catch (error) {
        if (error instanceof AssertionError || !(error instanceof Error)) {
          throw error;
        }
        expect(error).to.be.instanceOf(RetryError);
        expect((error as RetryError).errorCode).to.equal(RetryErrorCode.TimeoutExceeded);
      }
    });

    it('should check if retry attempts exceeded', async () => {
      const context = new ClientContextStub({ retryMaxAttempts: 3 });
      const clientConfig = context.getConfig();
      const policy = new HttpRetryPolicy(context);

      function createStub() {
        return {
          request: new Request('http://localhost', { method: 'POST' }),
          response: new Response(undefined, { status: 500 }),
        };
      }

      // First attempts should succeed
      for (let attempt = 1; attempt < clientConfig.retryMaxAttempts; attempt += 1) {
        const result = await policy.shouldRetry(createStub());
        expect(result.shouldRetry).to.be.true;
      }

      // Modify start time to be in the past so the next `shouldRetry` would fail
      try {
        await policy.shouldRetry(createStub());
        expect.fail('It should throw an error');
      } catch (error) {
        if (error instanceof AssertionError || !(error instanceof Error)) {
          throw error;
        }
        expect(error).to.be.instanceOf(RetryError);
        expect((error as RetryError).errorCode).to.equal(RetryErrorCode.AttemptsExceeded);
      }
    });
  });

  describe('invokeWithRetry', () => {
    it('should retry an operation until it succeeds', async () => {
      const context = new ClientContextStub({
        retryDelayMin: 1,
        retryDelayMax: 2,
        retryMaxAttempts: 20,
      });
      const policy = sinon.spy(new HttpRetryPolicy(context));

      function createStub(status: number) {
        return {
          request: new Request('http://localhost'),
          response: new Response(undefined, { status }),
        };
      }

      const expectedAttempts = 3;

      const operation = sinon
        .stub()
        .returns(createStub(500))
        .onCall(expectedAttempts - 1) // call numbers are zero-based
        .returns(createStub(200));

      const result = await policy.invokeWithRetry(operation);
      expect(policy.shouldRetry.callCount).to.equal(expectedAttempts);
      expect(result.response.status).to.equal(200);
      expect(operation.callCount).to.equal(expectedAttempts);
    });

    it('should stop retrying if retry limits reached', async () => {
      const context = new ClientContextStub({
        retryDelayMin: 1,
        retryDelayMax: 2,
        retryMaxAttempts: 3,
      });
      const clientConfig = context.getConfig();
      const policy = sinon.spy(new HttpRetryPolicy(context));

      function createStub(status: number) {
        return {
          request: new Request('http://localhost'),
          response: new Response(undefined, { status }),
        };
      }

      const expectedAttempts = clientConfig.retryMaxAttempts;

      const operation = sinon.stub().returns(createStub(500));

      try {
        await policy.invokeWithRetry(operation);
        expect.fail('It should throw an error');
      } catch (error) {
        if (error instanceof AssertionError) {
          throw error;
        }
        expect(error).to.be.instanceOf(RetryError);
        expect(policy.shouldRetry.callCount).to.equal(expectedAttempts);
        expect(operation.callCount).to.equal(expectedAttempts);
      }
    });

    it('should retry transient network errors (FetchError ECONNRESET / socket hang up)', async () => {
      const context = new ClientContextStub({
        retryDelayMin: 1,
        retryDelayMax: 2,
        retryMaxAttempts: 20,
      });
      const policy = new HttpRetryPolicy(context);

      function fetchError(): Error {
        // node-fetch FetchError for a low-level socket failure carries
        // `type: 'system'` and the underlying errno on `code`.
        return new FetchError('request to https://example.com failed, reason: socket hang up', 'system', {
          code: 'ECONNRESET',
        } as unknown as NodeJS.ErrnoException);
      }

      const expectedAttempts = 3;
      const operation = sinon.stub();
      for (let i = 0; i < expectedAttempts - 1; i += 1) {
        operation.onCall(i).rejects(fetchError());
      }
      operation.onCall(expectedAttempts - 1).resolves({
        request: new Request('http://localhost'),
        response: new Response(undefined, { status: 200 }),
      });

      const result = await policy.invokeWithRetry(operation);
      expect(result.response.status).to.equal(200);
      expect(operation.callCount).to.equal(expectedAttempts);
    });

    it('should retry "Premature close" body-stream errors', async () => {
      const context = new ClientContextStub({
        retryDelayMin: 1,
        retryDelayMax: 2,
        retryMaxAttempts: 20,
      });
      const policy = new HttpRetryPolicy(context);

      const operation = sinon.stub();
      // First call: body stream closes early — node-fetch surfaces this via
      // `response.buffer()` / `response.arrayBuffer()` as a plain Error whose
      // message contains "Premature close". With our fix the operation is
      // expected to consume the body inline and re-throw this as a retryable
      // failure.
      operation.onCall(0).rejects(new Error('Premature close'));
      operation.onCall(1).resolves({
        request: new Request('http://localhost'),
        response: new Response(undefined, { status: 200 }),
      });

      const result = await policy.invokeWithRetry(operation);
      expect(result.response.status).to.equal(200);
      expect(operation.callCount).to.equal(2);
    });

    it('should not retry non-transient errors (programmer errors, etc.)', async () => {
      const context = new ClientContextStub({
        retryDelayMin: 1,
        retryDelayMax: 2,
        retryMaxAttempts: 20,
      });
      const policy = new HttpRetryPolicy(context);

      // A regular Error with no `code`/`type` hints and no matching message
      // pattern must not be retried — those are programmer errors, not
      // transient network failures, and silently retrying would mask bugs.
      const operation = sinon.stub().rejects(new TypeError('cannot read property of undefined'));

      try {
        await policy.invokeWithRetry(operation);
        expect.fail('It should re-throw the error');
      } catch (error) {
        if (error instanceof AssertionError) {
          throw error;
        }
        expect(error).to.be.instanceOf(TypeError);
        expect(operation.callCount).to.equal(1);
      }
    });

    it('should stop retrying network errors once max attempts is reached', async () => {
      const context = new ClientContextStub({
        retryDelayMin: 1,
        retryDelayMax: 2,
        retryMaxAttempts: 3,
      });
      const clientConfig = context.getConfig();
      const policy = new HttpRetryPolicy(context);

      const fetchError = new FetchError('socket hang up', 'system', {
        code: 'ECONNRESET',
      } as unknown as NodeJS.ErrnoException);
      const operation = sinon.stub().rejects(fetchError);

      try {
        await policy.invokeWithRetry(operation);
        expect.fail('It should throw a RetryError after exhausting retries');
      } catch (error) {
        if (error instanceof AssertionError) {
          throw error;
        }
        // Once attempts are exhausted we surface a RetryError carrying the
        // underlying network failure as the payload — matches the existing
        // semantics for HTTP-status-driven exhaustion so callers have one
        // consistent terminal exception shape regardless of failure mode.
        expect(error).to.be.instanceOf(RetryError);
        expect((error as RetryError).errorCode).to.equal(RetryErrorCode.AttemptsExceeded);
        expect((error as RetryError).payload).to.equal(fetchError);
        expect(operation.callCount).to.equal(clientConfig.retryMaxAttempts);
      }
    });

    describe('network error classification', () => {
      function systemError(code: string): FetchError {
        return new FetchError(`request to https://example.com failed, reason: ${code}`, 'system', {
          code,
        } as unknown as NodeJS.ErrnoException);
      }

      function createPolicy() {
        return new HttpRetryPolicy(new ClientContextStub({ retryDelayMin: 1, retryDelayMax: 2, retryMaxAttempts: 5 }));
      }

      // Certificate verification failures and a host name that doesn't resolve
      for (const code of [
        'DEPTH_ZERO_SELF_SIGNED_CERT',
        'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
        'SELF_SIGNED_CERT_IN_CHAIN',
        'CERT_HAS_EXPIRED',
        'ERR_TLS_CERT_ALTNAME_INVALID',
        'ENOTFOUND',
      ]) {
        it(`throws ${code} at once, without retrying`, async () => {
          const error = systemError(code);
          const operation = sinon.stub().rejects(error);

          try {
            await createPolicy().invokeWithRetry(operation);
            expect.fail('It should throw the error');
          } catch (e) {
            if (e instanceof AssertionError) {
              throw e;
            }
            expect(e).to.equal(error);
          }
          expect(operation.callCount).to.equal(1);
        });
      }

      // Transient connect/DNS failures, a TLS alert sent by the server, and unavailable revocation data
      for (const code of [
        'ECONNREFUSED',
        'ECONNRESET',
        'EAI_AGAIN',
        'ERR_SSL_TLSV13_ALERT_CERTIFICATE_REQUIRED',
        'UNABLE_TO_GET_CRL',
      ]) {
        it(`retries ${code}`, async () => {
          const operation = sinon.stub();
          operation.onCall(0).rejects(systemError(code));
          operation.onCall(1).resolves({
            request: new Request('http://localhost'),
            response: new Response(undefined, { status: 200 }),
          });

          const result = await createPolicy().invokeWithRetry(operation);

          expect(result.response.status).to.equal(200);
          expect(operation.callCount).to.equal(2);
        });
      }
    });

    it('should cancel a pending backoff sleep when the signal is aborted', async () => {
      const context = new ClientContextStub({
        retryDelayMin: 10000,
        retryDelayMax: 60000,
        retryMaxAttempts: 20,
      });
      const policy = new HttpRetryPolicy(context);
      const operation = sinon.stub().resolves({
        request: new Request('http://localhost'),
        response: new Response(undefined, { status: 503 }),
      });
      const controller = new AbortController();
      const reason = new Error('deadline');
      const clearTimeoutSpy = sinon.spy(global, 'clearTimeout');
      const setTimeoutSpy = sinon.spy(global, 'setTimeout');

      try {
        const invocation = policy.invokeWithRetry(operation, controller.signal);
        // Let the first attempt complete and the 20s backoff sleep start
        await new Promise((resolve) => {
          setImmediate(resolve);
        });
        const backoffCall = setTimeoutSpy.getCalls().find((call) => call.args[1] === 20000);
        expect(backoffCall).to.not.be.undefined;

        controller.abort(reason);

        try {
          await invocation;
          expect.fail('It should reject with the abort reason');
        } catch (error) {
          if (error instanceof AssertionError) {
            throw error;
          }
          expect(error).to.equal(reason);
        }
        expect(clearTimeoutSpy.calledWith(backoffCall!.returnValue)).to.be.true;
        expect(operation.callCount).to.equal(1);
      } finally {
        setTimeoutSpy.restore();
        clearTimeoutSpy.restore();
      }
    });

    it('should not start another attempt when the signal is already aborted before the backoff', async () => {
      const context = new ClientContextStub({
        retryDelayMin: 10000,
        retryDelayMax: 60000,
        retryMaxAttempts: 20,
      });
      const policy = new HttpRetryPolicy(context);
      const controller = new AbortController();
      const reason = new Error('deadline');
      const operation = sinon.stub().callsFake(async () => {
        controller.abort(reason);
        return {
          request: new Request('http://localhost'),
          response: new Response(undefined, { status: 503 }),
        };
      });

      try {
        await policy.invokeWithRetry(operation, controller.signal);
        expect.fail('It should reject with the abort reason');
      } catch (error) {
        if (error instanceof AssertionError) {
          throw error;
        }
        expect(error).to.equal(reason);
      }
      expect(operation.callCount).to.equal(1);
    });
  });
});
