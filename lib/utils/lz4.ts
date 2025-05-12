import type LZ4Namespace from 'lz4';

type LZ4Module = typeof LZ4Namespace;

function tryLoadLZ4Module(): LZ4Module | undefined {
  try {
    return require('lz4'); // eslint-disable-line global-require
  } catch (err) {
    if (!(err instanceof Error) || !('code' in err)) {
      console.warn('Unexpected error loading LZ4 module: Invalid error object', err);
      return undefined;
    }

    if (err.code === 'MODULE_NOT_FOUND') {
      console.warn('LZ4 module not installed: Missing dependency', err);
      return undefined;
    }

    if (err.code === 'ERR_DLOPEN_FAILED') {
      console.warn('LZ4 native module failed to load: Architecture or version mismatch', err);
      return undefined;
    }

    // If it's not a known error, return undefined
    console.warn('Unknown error loading LZ4 module: Unhandled error code', err);
    return undefined;
  }
}

export default tryLoadLZ4Module();
