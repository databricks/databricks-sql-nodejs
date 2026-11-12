import { expect } from 'chai';
import sinon from 'sinon';
import asyncDisposable from '../../lib/asyncDisposable';
import * as driver from '../../lib';
import DBSQLClient from '../../lib/DBSQLClient';
import Status from '../../lib/dto/Status';
import OperationStub from './.stubs/OperationStub';
import ClientContextStub from './.stubs/ClientContextStub';
import { createSessionForTest } from './.stubs/createSessionForTest';

describe('asyncDisposable', () => {
  it('is exported from the package entry point', () => {
    expect(driver.asyncDisposable).to.equal(asyncDisposable);
  });

  it("returns the driver's own resources unchanged", async () => {
    const client = new DBSQLClient();
    const session = createSessionForTest({
      handle: { sessionId: { guid: Buffer.alloc(16), secret: Buffer.alloc(16) } },
      context: new ClientContextStub(),
    });
    const operation = await session.executeStatement('SELECT 1');

    for (const resource of [client, session, operation]) {
      const dispose = (resource as any)[Symbol.asyncDispose];
      expect(asyncDisposable(resource)).to.equal(resource);
      expect((resource as any)[Symbol.asyncDispose]).to.equal(dispose);
    }

    const closeSpy = sinon.spy(operation, 'close');
    await asyncDisposable(operation)[Symbol.asyncDispose]();
    expect(closeSpy.calledOnce).to.be.true;
  });

  it('adds a non-enumerable disposer that closes a custom implementation', async () => {
    const operation = new OperationStub([]);
    const disposable = asyncDisposable(operation);

    expect(disposable).to.equal(operation);
    expect(Object.getOwnPropertyDescriptor(operation, Symbol.asyncDispose)?.enumerable).to.be.false;
    expect(await disposable[Symbol.asyncDispose]()).to.be.undefined;
    expect(operation.closed).to.be.true;
  });

  it('keeps an existing disposer of a custom implementation', async () => {
    const dispose = sinon.stub().resolves();
    const resource = { close: async () => Status.success(), [Symbol.asyncDispose]: dispose };

    await asyncDisposable(resource)[Symbol.asyncDispose]();
    expect(dispose.calledOnce).to.be.true;
  });

  it('propagates close errors from the added disposer', async () => {
    const error = new Error('close failed');
    const resource = { close: () => Promise.reject(error) };

    try {
      await asyncDisposable(resource)[Symbol.asyncDispose]();
      expect.fail('It should throw an error');
    } catch (e) {
      expect(e).to.equal(error);
    }
  });

  it('throws a TypeError for a non-extensible custom implementation', () => {
    const resource = Object.freeze({ close: async () => Status.success() });
    expect(() => asyncDisposable(resource)).to.throw(TypeError);
  });
});
