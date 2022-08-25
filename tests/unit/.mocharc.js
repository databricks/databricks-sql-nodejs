'use strict';

const allSpecs = 'tests/unit/**/*.test.js';

const argvSpecs = process.argv.slice(4);

module.exports = {
  spec: argvSpecs.length > 0 ? argvSpecs : allSpecs,
};
