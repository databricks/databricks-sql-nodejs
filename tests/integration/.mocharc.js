'use strict';

const allSpecs = 'tests/integration/**/*.test.js';

const argvSpecs = process.argv.slice(4);

module.exports = {
  spec: argvSpecs.length > 0 ? argvSpecs : allSpecs,
  timeout: '300000',
};
