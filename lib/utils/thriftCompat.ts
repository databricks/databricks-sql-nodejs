import Module from 'module';

type InternalModuleLoader = {
  _load: (request: string, parent: NodeJS.Module | undefined, isMain: boolean) => unknown;
};

const PATCH_KEY = Symbol.for('databricks.sql.thriftUuidCompatInstalled');
const globalState = globalThis as typeof globalThis & { [PATCH_KEY]?: boolean };

if (!globalState[PATCH_KEY]) {
  const moduleWithLoader = Module as unknown as InternalModuleLoader;
  const originalLoad = moduleWithLoader._load.bind(Module);

  moduleWithLoader._load = (request, parent, isMain) => {
    if (request === 'uuid' && parent?.filename && /[\\/]node_modules[\\/]thrift[\\/]/.test(parent.filename)) {
      // thrift 0.23.x still loads uuid using require(), which fails with uuid@13 on older Node.js.
      // Force thrift internals to use our direct uuid dependency (v9, CommonJS-compatible).
      return require('uuid'); // eslint-disable-line global-require
    }

    return originalLoad(request, parent, isMain);
  };

  globalState[PATCH_KEY] = true;
}
