'use strict';

module.exports = {
  require: ['ts-node/register'],
  reporter: ['lcov'],
  all: true,
  include: ['lib/**'],
  exclude: ['lib/index.ts', 'thrift/**', 'tests/**'],
};
