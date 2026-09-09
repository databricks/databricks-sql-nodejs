import { expect } from 'chai';
import sinon from 'sinon';
import ThriftBackend from '../../../lib/thrift-backend/ThriftBackend';
import StatusError from '../../../lib/errors/StatusError';
import reydenCache from '../../../lib/ReydenWarehouseCache';
import { TStatusCode } from '../../../thrift/TCLIService_types';

/**
 * Orchestration tests for Reyden Thrift auto-recovery.
 *
 * These drive the REAL `ThriftBackend.openSession` recovery flow (pre-check,
 * KP001 detection, cache marking, kernel fallback, cause-chaining) and stub only
 * the two leaf I/O methods — `openSessionWithThrift` and
 * `openSessionWithKernelBackend` — the same boundary Python mocks at
 * (`KernelDatabricksClient`). Mirrors Python's `TestReydenThriftFallback`.
 *
 * The explicit-backend guardrail is intentionally not covered here: backend
 * selection happens upstream in DBSQLClient, so `ThriftBackend` is only ever
 * reached on the default (Thrift) path — an explicit `useKernel` never
 * constructs it.
 */
describe('Reyden Thrift Auto-Recovery — Orchestration', () => {
  let sandbox: sinon.SinonSandbox;

  const HOST = 'reyden.example.com';
  const WAREHOUSE_PATH = '/sql/1.0/warehouses/wh-reyden';
  const WAREHOUSE_ID = 'wh-reyden';

  // Minimal context: openSession only calls context.getLogger().
  function makeContext(): any {
    return { getLogger: () => ({ log: () => {} }) };
  }

  function makeBackend(): ThriftBackend {
    const backend = new ThriftBackend({ context: makeContext(), onConnectionEvent: () => {} });
    // openSession reads host/path from the stored connection options.
    (backend as any).connectionOptions = { host: HOST, path: WAREHOUSE_PATH };
    return backend;
  }

  function kp001Error(): StatusError {
    return new StatusError({
      statusCode: TStatusCode.ERROR_STATUS,
      errorMessage: 'Lakehouse/RT is not supported for Thrift protocol',
      sqlState: 'KP001',
    });
  }

  beforeEach(() => {
    sandbox = sinon.createSandbox();
    reydenCache.clear();
  });

  afterEach(() => {
    sandbox.restore();
    reydenCache.clear();
  });

  it('recovers onto the kernel backend when Thrift is rejected with KP001', async () => {
    const backend = makeBackend();
    const kernelSession = { marker: 'kernel-session' } as any;
    const thriftStub = sandbox.stub(backend as any, 'openSessionWithThrift').rejects(kp001Error());
    const kernelStub = sandbox.stub(backend as any, 'openSessionWithKernelBackend').resolves(kernelSession);

    const result = await backend.openSession({} as any);

    expect(result).to.equal(kernelSession);
    expect(thriftStub.calledOnce).to.be.true;
    expect(kernelStub.calledOnce).to.be.true;
    // The rejection is remembered for later connects.
    expect(reydenCache.isKnownReyden(HOST, WAREHOUSE_ID)).to.be.true;
  });

  it('pre-checks the cache and skips Thrift for a known-Reyden warehouse', async () => {
    reydenCache.markReyden(HOST, WAREHOUSE_ID);
    const backend = makeBackend();
    const kernelSession = { marker: 'kernel-session' } as any;
    const thriftStub = sandbox.stub(backend as any, 'openSessionWithThrift').resolves({ marker: 'thrift' } as any);
    const kernelStub = sandbox.stub(backend as any, 'openSessionWithKernelBackend').resolves(kernelSession);

    const result = await backend.openSession({} as any);

    expect(result).to.equal(kernelSession);
    expect(kernelStub.calledOnce).to.be.true;
    expect(thriftStub.called).to.be.false; // Thrift round-trip skipped entirely.
  });

  it('propagates a non-KP001 Thrift error without falling back', async () => {
    const backend = makeBackend();
    const genericError = new StatusError({
      statusCode: TStatusCode.ERROR_STATUS,
      errorMessage: 'a syntax error',
      sqlState: '42000',
    });
    sandbox.stub(backend as any, 'openSessionWithThrift').rejects(genericError);
    const kernelStub = sandbox.stub(backend as any, 'openSessionWithKernelBackend').resolves({} as any);

    let thrown: unknown;
    try {
      await backend.openSession({} as any);
    } catch (e) {
      thrown = e;
    }

    expect(thrown).to.equal(genericError);
    expect(kernelStub.called).to.be.false; // No kernel fallback for a non-Reyden error.
    expect(reydenCache.isKnownReyden(HOST, WAREHOUSE_ID)).to.be.undefined; // Not marked.
  });

  it('preserves the Thrift rejection as cause when the kernel fallback also fails', async () => {
    const backend = makeBackend();
    const thriftError = kp001Error();
    const kernelError = new Error('kernel open failed');
    sandbox.stub(backend as any, 'openSessionWithThrift').rejects(thriftError);
    sandbox.stub(backend as any, 'openSessionWithKernelBackend').rejects(kernelError);

    let thrown: any;
    try {
      await backend.openSession({} as any);
    } catch (e) {
      thrown = e;
    }

    // Kernel failure surfaced as primary; Thrift rejection preserved in the chain.
    expect(thrown).to.equal(kernelError);
    expect(thrown.cause).to.equal(thriftError);
  });
});
