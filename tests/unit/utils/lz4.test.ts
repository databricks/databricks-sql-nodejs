import { expect } from 'chai';
import sinon from 'sinon';

// Exercises the lz4 loader (lib/utils/lz4.ts), which now wraps `lz4-napi`
// and exposes a stable { encode, decode } frame codec with a
// load-failure-tolerant contract.
//
// This suite uses CJS-only primitives (Module._load, require.cache) to
// inject a fake/failed 'lz4-napi'. Node 22+ loads .ts specs via the ESM
// loader where those aren't available; skip there — the production loader
// works fine, only this test's interception mechanism is CJS-bound.
describe('lz4 module loader', function () {
  let moduleLoadRestore: (() => void) | undefined;
  let consoleWarnStub: sinon.SinonStub;

  before(function () {
    if (typeof require === 'undefined') {
      this.skip();
    }
  });

  beforeEach(() => {
    consoleWarnStub = sinon.stub(console, 'warn');
  });

  afterEach(() => {
    consoleWarnStub.restore();
    if (moduleLoadRestore) {
      moduleLoadRestore();
      moduleLoadRestore = undefined;
    }
    Object.keys(require.cache).forEach((key) => {
      if (key.includes('lz4')) {
        delete require.cache[key];
      }
    });
  });

  // Intercept require('lz4-napi') to return a mock or throw a given error.
  const mockNapiLoad = (napiMockOrError: unknown): { wasLoadAttempted: () => boolean } => {
    // eslint-disable-next-line global-require
    const Module = require('module');
    const originalLoad = Module._load;
    let loadAttempted = false;

    Module._load = (request: string, parent: unknown, isMain: boolean) => {
      if (request === 'lz4-napi') {
        loadAttempted = true;
        if (napiMockOrError instanceof Error) {
          throw napiMockOrError;
        }
        return napiMockOrError;
      }
      return originalLoad.call(Module, request, parent, isMain);
    };

    moduleLoadRestore = () => {
      Module._load = originalLoad;
    };
    return { wasLoadAttempted: () => loadAttempted };
  };

  const loadLz4Module = () => {
    delete require.cache[require.resolve('../../../lib/utils/lz4')];
    // eslint-disable-next-line global-require
    return require('../../../lib/utils/lz4');
  };

  it('exposes an { encode, decode } codec backed by lz4-napi frame APIs', () => {
    // Fake lz4-napi: frame APIs the adapter calls.
    const fakeNapi = {
      compressFrameSync: (buf: Buffer) => Buffer.from(`frame:${buf.toString()}`),
      decompressFrameSync: (buf: Buffer) => Buffer.from(buf.toString().replace('frame:', '')),
    };

    mockNapiLoad(fakeNapi);
    const lz4Module = loadLz4Module().default();

    expect(lz4Module).to.not.be.undefined;
    expect(lz4Module.encode).to.be.a('function');
    expect(lz4Module.decode).to.be.a('function');

    const testData = Buffer.from('Hello, World!');
    const roundTripped = lz4Module.decode(lz4Module.encode(testData));
    expect(roundTripped.toString()).to.equal('Hello, World!');
    expect(consoleWarnStub.called).to.be.false;
  });

  it('returns undefined (no warning) when lz4-napi is not installed (MODULE_NOT_FOUND)', () => {
    const err: NodeJS.ErrnoException = new Error("Cannot find module 'lz4-napi'");
    err.code = 'MODULE_NOT_FOUND';

    mockNapiLoad(err);
    const lz4Module = loadLz4Module().default();

    expect(lz4Module).to.be.undefined;
    expect(consoleWarnStub.called).to.be.false;
  });

  it('returns undefined and warns on any other load failure', () => {
    const err: NodeJS.ErrnoException = new Error('unexpected dlopen failure');
    err.code = 'ERR_DLOPEN_FAILED';

    mockNapiLoad(err);
    const lz4Module = loadLz4Module().default();

    expect(lz4Module).to.be.undefined;
    expect(consoleWarnStub.calledOnce).to.be.true;
    expect(consoleWarnStub.firstCall.args[0]).to.include('lz4-napi');
  });

  it('does not attempt to load lz4-napi until the codec is requested', () => {
    const fakeNapi = {
      compressFrameSync: () => Buffer.from(''),
      decompressFrameSync: () => Buffer.from(''),
    };

    const { wasLoadAttempted } = mockNapiLoad(fakeNapi);

    // Import the module but do NOT call the default export (getResolvedModule).
    loadLz4Module();

    expect(wasLoadAttempted()).to.be.false;
    expect(consoleWarnStub.called).to.be.false;
  });
});
