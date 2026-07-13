'use strict';

const allSpecs = 'tests/unit/**/*.test.ts';

const argvSpecs = process.argv.slice(4);

module.exports = {
  spec: argvSpecs.length > 0 ? argvSpecs : allSpecs,
  // Force ts-node's CommonJS require-hook to own .ts compilation. Without an
  // explicit loader, mocha on Node 24+ loads .ts through Node's built-in
  // --experimental-strip-types (ESM), whose strip-only mode rejects TS
  // parameter properties. Registering ts-node makes .ts resolve via its CJS
  // hook on every supported Node version (no version-specific flags — the
  // --no-experimental-strip-types flag doesn't exist on Node < 22.6).
  require: 'ts-node/register',
};
