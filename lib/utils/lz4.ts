// LZ4 frame codec, backed by `lz4-napi`.
//
// Historical note: this driver previously depended on the unmaintained
// `lz4` package (last published 2021), a node-gyp native addon with no
// prebuilt binaries. It failed to compile on Node 22+, so it was silently
// skipped as an optional dependency and LZ4-compressed results could not be
// decoded. `lz4-napi` is maintained and ships prebuilt binaries for all
// supported platforms/Node versions, so no local toolchain build is needed.
//
// The Databricks server sends LZ4-compressed Arrow/CloudFetch payloads in
// the LZ4 **frame** format (magic 0x184D2204). `lz4-napi`'s
// `compressFrameSync`/`decompressFrameSync` are byte-compatible with the
// frames the old `lz4` package produced/consumed — verified by round-trip
// and cross-decoding both directions. We expose the same `{ encode, decode }`
// surface the rest of the driver already calls, so call sites are unchanged.

type Lz4Napi = {
  compressFrameSync(data: Buffer): Buffer;
  decompressFrameSync(data: Buffer): Buffer;
};

// Stable interface used across the driver (lib call sites do `LZ4()!.decode(buf)`).
export interface LZ4Codec {
  encode(data: Buffer): Buffer;
  decode(data: Buffer): Buffer;
}

function tryLoadLZ4Module(): LZ4Codec | undefined {
  let napi: Lz4Napi;
  try {
    napi = require('lz4-napi'); // eslint-disable-line global-require
  } catch (err) {
    // lz4-napi ships prebuilds so this should not normally happen, but keep
    // the load-failure-tolerant contract: if it can't load, LZ4 support is
    // reported as unavailable rather than crashing the driver.
    if (err instanceof Error && 'code' in err && err.code === 'MODULE_NOT_FOUND') {
      return undefined;
    }
    // eslint-disable-next-line no-console
    console.warn('LZ4 module (lz4-napi) failed to load; LZ4-compressed results unavailable', err);
    return undefined;
  }

  return {
    encode: (data: Buffer): Buffer => Buffer.from(napi.compressFrameSync(data)),
    decode: (data: Buffer): Buffer => Buffer.from(napi.decompressFrameSync(data)),
  };
}

// The null means we already tried resolving and it failed.
let resolvedModule: LZ4Codec | null | undefined;

function getResolvedModule(): LZ4Codec | undefined {
  if (resolvedModule === undefined) {
    resolvedModule = tryLoadLZ4Module() ?? null;
  }
  return resolvedModule === null ? undefined : resolvedModule;
}

export default getResolvedModule;
