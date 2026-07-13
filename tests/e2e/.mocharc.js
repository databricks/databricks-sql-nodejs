'use strict';

const allSpecs = 'tests/e2e/**/*.test.ts';

const argvSpecs = process.argv.slice(4);

module.exports = {
  spec: argvSpecs.length > 0 ? argvSpecs : allSpecs,
  timeout: '300000',
  // See tests/unit/.mocharc.js — force ts-node's CJS require-hook so .ts
  // specs compile via ts-node (not Node 24+'s built-in strip-types, which
  // rejects TS parameter properties) on every supported Node version.
  require: 'ts-node/register',
};
