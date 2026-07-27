import { expect } from 'chai';
import sinon from 'sinon';
import getLZ4 from '../../../lib/utils/lz4';

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

// Real (unmocked) frame-format compatibility, exercised through the driver's
// own codec (lib/utils/lz4.ts) with the actual `lz4-napi`. This is the test
// that backs the "LZ4 frame compatible" claim: unlike the loader suite above
// it does NOT mock lz4-napi, and it runs on every supported Node version
// (import-based, so no CJS-only interception → no ESM skip).
describe('lz4 frame codec (real lz4-napi)', function () {
  // The Databricks server sends LZ4 **frame** format (magic 0x184D2204). This
  // golden frame was produced by the reference `lz4` CLI (an implementation
  // independent of lz4-napi) from the known plaintext below — so decoding it
  // proves the driver reads frames it did not itself produce, not merely that
  // its own encode/decode round-trip agrees.
  const goldenPlaintext = Buffer.from(
    'Databricks LZ4 frame golden test — the quick brown fox jumps over the lazy dog 0123456789',
  );
  const goldenFrame = Buffer.from([
    4, 34, 77, 24, 100, 64, 167, 91, 0, 0, 128, 68, 97, 116, 97, 98, 114, 105, 99, 107, 115, 32, 76, 90, 52, 32, 102,
    114, 97, 109, 101, 32, 103, 111, 108, 100, 101, 110, 32, 116, 101, 115, 116, 32, 226, 128, 148, 32, 116, 104, 101,
    32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120, 32, 106, 117, 109, 112, 115, 32, 111,
    118, 101, 114, 32, 116, 104, 101, 32, 108, 97, 122, 121, 32, 100, 111, 103, 32, 48, 49, 50, 51, 52, 53, 54, 55, 56,
    57, 0, 0, 0, 0, 153, 39, 109, 21,
  ]);

  // lib/utils/lz4.ts is deliberately optional: it returns undefined when the
  // native binding can't load, so the driver degrades gracefully instead of
  // crashing. lz4-napi ships prebuilds for the mainstream platforms but not
  // every arch (e.g. linux-ppc64le/s390x), and `npm ci --omit=optional` also
  // yields undefined. So these cases test the CODEC when it is present rather
  // than asserting the environment has it — a missing binding skips (with a
  // visible warning) instead of failing the whole unit run. CI's linux-x64
  // matrix always has a prebuild, so the frame-compat path is always covered
  // there.
  let codec: ReturnType<typeof getLZ4>;

  before(function () {
    codec = getLZ4();
    if (!codec) {
      // eslint-disable-next-line no-console
      console.warn(
        'lz4-napi is not available on this platform — skipping real LZ4 frame codec tests. ' +
          'This is expected on platforms without an lz4-napi prebuild or with --omit=optional.',
      );
      this.skip();
    }
  });

  it('decodes a golden LZ4 frame produced by an independent encoder', () => {
    // Frame magic 0x184D2204, little-endian.
    expect(goldenFrame.readUInt32LE(0)).to.equal(0x184d2204);

    const decoded = codec!.decode(goldenFrame);
    expect(decoded.equals(goldenPlaintext)).to.be.true;
  });

  it('round-trips arbitrary binary payloads (encode then decode)', () => {
    for (const payload of [
      Buffer.alloc(0),
      Buffer.from('short'),
      Buffer.from('a'.repeat(100000)), // larger than a single block
      Buffer.from(Array.from({ length: 4096 }, (_, i) => i % 256)), // non-repetitive binary
    ]) {
      const encoded = codec!.encode(payload);
      // Non-empty inputs must carry the LZ4 frame magic.
      if (payload.length > 0) {
        expect(encoded.readUInt32LE(0)).to.equal(0x184d2204);
      }
      expect(codec!.decode(encoded).equals(payload)).to.be.true;
    }
  });
});
