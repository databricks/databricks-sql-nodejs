'use strict';

const allSpecs = 'tests/unit/**/*.test.ts';

const argvSpecs = process.argv.slice(4);

module.exports = {
  spec: argvSpecs.length > 0 ? argvSpecs : allSpecs,
  // Compile .ts specs with ts-node on every supported Node version. Both
  // hooks are needed: mocha loads some specs via `require` (CJS) and others
  // via `import()` (ESM), and on Node 24+ any .ts reaching the ESM path is
  // grabbed by Node's built-in --experimental-strip-types, whose strip-only
  // mode rejects TS parameter properties ("parameter property is not
  // supported in strip-only mode"). `require: ts-node/register` covers the
  // CJS path; `loader: ts-node/esm` covers the import() path. Verified on
  // Node 20 and 24. (Avoid the --no-experimental-strip-types node flag — it
  // doesn't exist before Node 22.6 and crashes older Node at startup.)
  require: 'ts-node/register',
  loader: 'ts-node/esm',
};
