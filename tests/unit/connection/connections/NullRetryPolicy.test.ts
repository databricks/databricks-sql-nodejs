import { expect, AssertionError } from 'chai';
import sinon from 'sinon';
import NullRetryPolicy from '../../../../lib/connection/connections/NullRetryPolicy';

describe('NullRetryPolicy', () => {
  it('should never allow retries', async () => {
    const policy = new NullRetryPolicy();

    // make several attempts
    for (let i = 0; i < 5; i += 1) {
      const { shouldRetry } = await policy.shouldRetry(undefined);
      expect(shouldRetry).to.be.false;
    }
  });

  it('should not retry the provided callback', async () => {
    const policy = new NullRetryPolicy();

    const operation = sinon.stub().returns(Promise.reject(new Error()));
    try {
      await policy.invokeWithRetry(operation);
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }

      expect(operation.callCount).to.equal(1);
    }
  });
});
