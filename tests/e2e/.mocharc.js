'use strict';

const allSpecs = 'tests/e2e/**/*.test.ts';

const argvSpecs = process.argv.slice(4);

module.exports = {
  spec: argvSpecs.length > 0 ? argvSpecs : allSpecs,
  timeout: '300000',
  // See tests/unit/.mocharc.js — ts-node handles .ts on both the CJS
  // (require) and ESM (import) load paths, so Node 24+'s built-in
  // strip-types never intercepts specs using TS parameter properties.
  require: 'ts-node/register',
  loader: 'ts-node/esm',
};
