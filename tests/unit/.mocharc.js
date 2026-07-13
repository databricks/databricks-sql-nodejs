'use strict';

const allSpecs = 'tests/unit/**/*.test.ts';

const argvSpecs = process.argv.slice(4);

module.exports = {
  spec: argvSpecs.length > 0 ? argvSpecs : allSpecs,
  // Compile .ts specs with ts-node, not Node's built-in type stripping.
  // Node >= 22.6 enables --experimental-strip-types, which becomes the
  // default loader for .ts on Node 24+. Its "strip-only" mode cannot handle
  // TypeScript features that emit code (e.g. constructor parameter
  // properties), so it throws "parameter property is not supported in
  // strip-only mode" on files ts-node compiles fine. Force ts-node and turn
  // off Node's stripper so ts-node owns .ts on every supported Node version.
  require: 'ts-node/register',
  'node-option': ['no-experimental-strip-types'],
};
