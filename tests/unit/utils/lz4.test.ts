import { expect } from 'chai';
import sinon from 'sinon';

describe('lz4 module loader', () => {
  let moduleLoadStub: sinon.SinonStub | undefined;
  let consoleWarnStub: sinon.SinonStub;

  beforeEach(() => {
    consoleWarnStub = sinon.stub(console, 'warn');
  });

  afterEach(() => {
    consoleWarnStub.restore();
    if (moduleLoadStub) {
      moduleLoadStub.restore();
    }
    // Clear module cache
    Object.keys(require.cache).forEach((key) => {
      if (key.includes('lz4')) {
        delete require.cache[key];
      }
    });
  });

  const mockModuleLoad = (lz4MockOrError: unknown): { restore: () => void; wasLz4LoadAttempted: () => boolean } => {
    // eslint-disable-next-line global-require
    const Module = require('module');
    const originalLoad = Module._load;
    let lz4LoadAttempted = false;

    Module._load = (request: string, parent: unknown, isMain: boolean) => {
      if (request === 'lz4') {
        lz4LoadAttempted = true;
        if (lz4MockOrError instanceof Error) {
          throw lz4MockOrError;
        }
        return lz4MockOrError;
      }
      return originalLoad.call(Module, request, parent, isMain);
    };

    return {
      restore: () => {
        Module._load = originalLoad;
      },
      wasLz4LoadAttempted: () => lz4LoadAttempted,
    };
  };

  const loadLz4Module = () => {
    delete require.cache[require.resolve('../../../lib/utils/lz4')];
    // eslint-disable-next-line global-require
    return require('../../../lib/utils/lz4');
  };

  it('should successfully load and use lz4 module when available', () => {
    const fakeLz4 = {
      encode: (buf: Buffer) => {
        const compressed = Buffer.from(`compressed:${buf.toString()}`);
        return compressed;
      },
      decode: (buf: Buffer) => {
        const decompressed = buf.toString().replace('compressed:', '');
        return Buffer.from(decompressed);
      },
    };

    const { restore } = mockModuleLoad(fakeLz4);
    const moduleExports = loadLz4Module();
    const lz4Module = moduleExports.default();
    restore();

    expect(lz4Module).to.not.be.undefined;
    expect(lz4Module.encode).to.be.a('function');
    expect(lz4Module.decode).to.be.a('function');

    const testData = Buffer.from('Hello, World!');
    const compressed = lz4Module.encode(testData);
    const decompressed = lz4Module.decode(compressed);

    expect(decompressed.toString()).to.equal('Hello, World!');
    expect(consoleWarnStub.called).to.be.false;
  });

  it('should return undefined when lz4 module fails to load with MODULE_NOT_FOUND', () => {
    const err: NodeJS.ErrnoException = new Error("Cannot find module 'lz4'");
    err.code = 'MODULE_NOT_FOUND';

    const { restore } = mockModuleLoad(err);
    const moduleExports = loadLz4Module();
    const lz4Module = moduleExports.default();
    restore();

    expect(lz4Module).to.be.undefined;
    expect(consoleWarnStub.called).to.be.false;
  });

  it('should return undefined and log warning when lz4 fails with ERR_DLOPEN_FAILED', () => {
    const err: NodeJS.ErrnoException = new Error('Module did not self-register');
    err.code = 'ERR_DLOPEN_FAILED';

    const { restore } = mockModuleLoad(err);
    const moduleExports = loadLz4Module();
    const lz4Module = moduleExports.default();
    restore();

    expect(lz4Module).to.be.undefined;
    expect(consoleWarnStub.calledOnce).to.be.true;
    expect(consoleWarnStub.firstCall.args[0]).to.include('Architecture or version mismatch');
  });

  it('should return undefined and log warning when lz4 fails with unknown error code', () => {
    const err: NodeJS.ErrnoException = new Error('Some unknown error');
    err.code = 'UNKNOWN_ERROR';

    const { restore } = mockModuleLoad(err);
    const moduleExports = loadLz4Module();
    const lz4Module = moduleExports.default();
    restore();

    expect(lz4Module).to.be.undefined;
    expect(consoleWarnStub.calledOnce).to.be.true;
    expect(consoleWarnStub.firstCall.args[0]).to.include('Unhandled error code');
  });

  it('should return undefined and log warning when error has no code property', () => {
    const err = new Error('Error without code');

    const { restore } = mockModuleLoad(err);
    const moduleExports = loadLz4Module();
    const lz4Module = moduleExports.default();
    restore();

    expect(lz4Module).to.be.undefined;
    expect(consoleWarnStub.calledOnce).to.be.true;
    expect(consoleWarnStub.firstCall.args[0]).to.include('Invalid error object');
  });

  it('should not attempt to load lz4 module when getResolvedModule is not called', () => {
    const fakeLz4 = {
      encode: () => Buffer.from(''),
      decode: () => Buffer.from(''),
    };

    const { restore, wasLz4LoadAttempted } = mockModuleLoad(fakeLz4);

    // Load the module but don't call getResolvedModule
    loadLz4Module();
    // Note: we're NOT calling .default() here

    restore();

    expect(wasLz4LoadAttempted()).to.be.false;
    expect(consoleWarnStub.called).to.be.false;
  });
});
