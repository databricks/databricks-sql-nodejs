import Int64 from 'node-int64';

import schema from '../thrift_schema';
import data from './data';
import expected from '../expected';
import { TRowSet } from '../../../../thrift/TCLIService_types';

export { schema, expected };

export const rowSets: Array<TRowSet> = [
  {
    startRowOffset: new Int64(Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]), 0),
    rows: [],
    columns: data,
  },
];
