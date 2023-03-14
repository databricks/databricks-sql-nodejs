const Int64 = require('node-int64');

const thriftSchema = require('../thrift_schema');
const data = require('./data');
const expected = require('../expected');

exports.schema = thriftSchema;

exports.rowSets = [
  {
    startRowOffset: new Int64(Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]), 0),
    rows: [],
    columns: data,
  },
];

exports.expected = expected;
