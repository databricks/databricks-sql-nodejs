import type LZ4Namespace from 'lz4';

type LZ4Module = typeof LZ4Namespace;

function tryLoadLZ4Module(): LZ4Module | undefined {
  try {
    return require('lz4'); // eslint-disable-line global-require
  } catch (err) {
    if (!(err instanceof Error) || !('code' in err)) {
      // eslint-disable-next-line no-console
      console.warn('Unexpected error loading LZ4 module: Invalid error object', err);
      return undefined;
    }

    if (err.code === 'MODULE_NOT_FOUND') {
      return undefined;
    }

    if (err.code === 'ERR_DLOPEN_FAILED') {
      // eslint-disable-next-line no-console
      console.warn('LZ4 native module failed to load: Architecture or version mismatch', err);
      return undefined;
    }

    // If it's not a known error, return undefined
    // eslint-disable-next-line no-console
    console.warn('Unknown error loading LZ4 module: Unhandled error code', err);
    return undefined;
  }
}

// The null is already tried resolving that failed
let resolvedModule: LZ4Module | null | undefined;

function getResolvedModule() {
  if (resolvedModule === undefined) {
    resolvedModule = tryLoadLZ4Module() ?? null;
  }
  return resolvedModule === null ? undefined : resolvedModule;
}

export default getResolvedModule;
