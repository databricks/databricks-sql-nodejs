'use strict';

const allSpecs = 'tests/e2e/**/*.test.ts';

const argvSpecs = process.argv.slice(4);

module.exports = {
  spec: argvSpecs.length > 0 ? argvSpecs : allSpecs,
  timeout: '300000',
  // See tests/unit/.mocharc.js — force ts-node and disable Node's built-in
  // --experimental-strip-types (default on Node 24+) so .ts specs using
  // parameter properties etc. compile via ts-node on every supported Node.
  require: 'ts-node/register',
  'node-option': ['no-experimental-strip-types'],
};
