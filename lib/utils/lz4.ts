import type LZ4Namespace from 'lz4';

type LZ4Module = typeof LZ4Namespace;

function tryLoadLZ4Module(): LZ4Module | undefined {
  try {
    return require('lz4'); // eslint-disable-line global-require
  } catch (err) {
    const isModuleNotFoundError = err instanceof Error && 'code' in err && err.code === 'MODULE_NOT_FOUND';
    if (!isModuleNotFoundError) {
      throw err;
    }
  }
}

export default tryLoadLZ4Module();
