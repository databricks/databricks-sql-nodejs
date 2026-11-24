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

import HiveDriverError from '../errors/HiveDriverError';

type Lz4Napi = {
  compressFrameSync(data: Buffer): Buffer;
  decompressFrameSync(data: Buffer): Buffer;
};

type LZ4Frame = { bytes: Buffer; index: number; offset: number; contentSize?: bigint };

// Stable interface used across the driver (lib call sites do `LZ4()!.decode(buf)`).
export interface LZ4Codec {
  encode(data: Buffer): Buffer;
  decode(data: Buffer): Buffer;
  decodeFrames(data: Buffer): Array<Buffer>;
}

function frameError(frame: { index: number; offset: number }, reason: unknown): HiveDriverError {
  return new HiveDriverError(
    `LZ4 frame ${frame.index} at offset ${frame.offset}: ${reason instanceof Error ? reason.message : String(reason)}`,
  );
}

function* readFrames(data: Buffer): Generator<LZ4Frame> {
  let offset = 0;
  let index = 0;
  let start = 0;
  const advance = (size: number) => {
    offset += size;
    if (offset > data.length) {
      throw frameError({ index, offset: start }, 'Truncated LZ4 frame');
    }
  };

  while (offset < data.length) {
    start = offset;
    const frame = { index, offset: start };
    advance(7);
    if (data.readUInt32LE(start) !== 0x184d2204) {
      throw frameError(frame, 'Invalid LZ4 frame magic');
    }
    const flags = data[start + 4];
    let contentSize: bigint | undefined;
    if (flags & 0x08) {
      advance(8);
      contentSize = data.readBigUInt64LE(start + 6);
    }
    if (flags & 0x01) advance(4);

    let blockSize: number;
    do {
      advance(4);
      blockSize = data.readUInt32LE(offset - 4);
      if (blockSize === 0x80000000) {
        throw frameError(frame, 'Invalid zero-length LZ4 block');
      }
      if (blockSize !== 0) {
        advance((blockSize & 0x7fffffff) + (flags & 0x10 ? 4 : 0));
      }
    } while (blockSize !== 0);
    if (flags & 0x04) advance(4);

    yield { ...frame, bytes: data.subarray(start, offset), contentSize };
    index += 1;
  }
}

function validateContentSize(frame: LZ4Frame, decoded: Buffer): Buffer {
  if (frame.contentSize !== undefined && BigInt(decoded.length) !== frame.contentSize) {
    throw new Error(`Content size mismatch: expected ${frame.contentSize}, got ${decoded.length}`);
  }
  return decoded;
}

function decodeFrames(data: Buffer, napi: Lz4Napi): Array<Buffer> {
  return Array.from(readFrames(data), (frame) => {
    try {
      return validateContentSize(frame, napi.decompressFrameSync(frame.bytes));
    } catch (error) {
      throw frameError(frame, error);
    }
  });
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

  // lz4-napi's frame APIs already return freshly-allocated, owned `Buffer`s
  // (napi-rs materializes Node Buffers), so we return them directly rather
  // than copying again with `Buffer.from` — decode runs on the CloudFetch
  // hot path (once per Arrow batch).
  return {
    encode: (data: Buffer): Buffer => napi.compressFrameSync(data),
    decode: (data: Buffer): Buffer => {
      const frames = decodeFrames(data, napi);
      return frames.length === 1 ? frames[0] : Buffer.concat(frames);
    },
    decodeFrames: (data: Buffer): Array<Buffer> => decodeFrames(data, napi),
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
