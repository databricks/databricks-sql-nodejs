const Int64 = require('node-int64');

const fs = require('fs');
const path = require('path');

const thriftSchema = require('../thrift_schema');
const arrowSchema = fs.readFileSync(path.join(__dirname, 'schema.arrow'));
const data = fs.readFileSync(path.join(__dirname, 'data.arrow'));
const expected = require('../expected');

exports.schema = thriftSchema;

exports.arrowSchema = arrowSchema;

exports.rowSets = [
  {
    startRowOffset: new Int64(Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]), 0),
    rows: [],
    arrowBatches: [
      {
        batch: data,
        rowCount: new Int64(Buffer.from([0, 0, 0, 0, 0, 0, 0, 1]), 0),
      },
    ],
  },
];

exports.expected = expected.map((row) => ({
  ...row,
  dat: new Date(Date.parse(`${row.dat} UTC`)),
  ts: new Date(Date.parse(`${row.ts} UTC`)),
}));
