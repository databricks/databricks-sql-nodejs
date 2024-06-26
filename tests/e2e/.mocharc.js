'use strict';

const allSpecs = 'tests/e2e/**/*.test.ts';

const argvSpecs = process.argv.slice(4);

module.exports = {
  spec: argvSpecs.length > 0 ? argvSpecs : allSpecs,
  timeout: '300000',
};
