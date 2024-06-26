import { expect, AssertionError } from 'chai';
import sinon from 'sinon';
import { Request, Response, HeadersInit } from 'node-fetch';
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

    // Valid `Retry-After`, several header name variants
    expect(policy['getRetryAfterHeader'](createStub({ 'Retry-After': '10' }), retryDelayMin)).to.equal(10);
    expect(policy['getRetryAfterHeader'](createStub({ 'retry-after': '10' }), retryDelayMin)).to.equal(10);
    expect(policy['getRetryAfterHeader'](createStub({ 'RETRY-AFTER': '10' }), retryDelayMin)).to.equal(10);

    // Invalid header values (non-numeric, negative)
    expect(policy['getRetryAfterHeader'](createStub({ 'Retry-After': 'test' }), retryDelayMin)).to.be.undefined;
    expect(policy['getRetryAfterHeader'](createStub({ 'Retry-After': '-10' }), retryDelayMin)).to.be.undefined;

    // It should not be smaller than min value, but can be greater than max value
    expect(policy['getRetryAfterHeader'](createStub({ 'Retry-After': '1' }), retryDelayMin)).to.equal(retryDelayMin);
    expect(policy['getRetryAfterHeader'](createStub({ 'Retry-After': '200' }), retryDelayMin)).to.equal(200);
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
    // The only exception is 429 (Too many requests)
    for (let status = 100; status < 500; status += 1) {
      const expectedResult = status === 429 ? true : false;
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

    it('should use `Retry-After` header as a base for backoff', async () => {
      const context = new ClientContextStub({ retryDelayMin: 3, retryDelayMax: 100, retryMaxAttempts: 10 });
      const policy = new HttpRetryPolicy(context);

      function createStub(headers: HeadersInit) {
        return {
          request: new Request('http://localhost'),
          response: new Response(undefined, { status: 500, headers }),
        };
      }

      const result1 = await policy.shouldRetry(createStub({ 'Retry-After': '5' }));
      expect(result1.shouldRetry).to.be.true;
      if (result1.shouldRetry) {
        expect(result1.retryAfter).to.equal(10);
      }

      const result2 = await policy.shouldRetry(createStub({ 'Retry-After': '8' }));
      expect(result2.shouldRetry).to.be.true;
      if (result2.shouldRetry) {
        expect(result2.retryAfter).to.equal(32);
      }

      policy['attempt'] = 4;
      const result3 = await policy.shouldRetry(createStub({ 'Retry-After': '10' }));
      expect(result3.shouldRetry).to.be.true;
      if (result3.shouldRetry) {
        expect(result3.retryAfter).to.equal(100);
      }
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
  });
});
