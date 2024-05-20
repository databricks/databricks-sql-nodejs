import Int64 from 'node-int64';
import fs from 'fs';
import path from 'path';

import schema from '../thrift_schema';
import expectedData from '../expected';
import { TRowSet } from '../../../../thrift/TCLIService_types';

const arrowSchema = fs.readFileSync(path.join(__dirname, 'schema.arrow'));
const data = fs.readFileSync(path.join(__dirname, 'data.arrow'));

export { schema, arrowSchema };

export const rowSets: Array<TRowSet> = [
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

export const expected = expectedData.map((row) => ({
  ...row,
  dat: new Date(Date.parse(`${row.dat} UTC`)),
  ts: new Date(Date.parse(`${row.ts} UTC`)),
}));
