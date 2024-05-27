import Int64 from 'node-int64';
import { TColumn } from '../../../../thrift/TCLIService_types';

const data: Array<TColumn> = [
  {
    boolVal: {
      values: [true],
      nulls: Buffer.from([0]),
    },
  },
  {
    byteVal: {
      values: [127],
      nulls: Buffer.from([0]),
    },
  },
  {
    i16Val: {
      values: [32000],
      nulls: Buffer.from([0]),
    },
  },
  {
    i32Val: {
      values: [4000000],
      nulls: Buffer.from([0]),
    },
  },
  {
    i64Val: {
      values: [new Int64(Buffer.from([0, 1, 82, 93, 148, 146, 127, 255]), 0)],
      nulls: Buffer.from([0]),
    },
  },
  {
    doubleVal: {
      values: [1.4142],
      nulls: Buffer.from([0]),
    },
  },
  {
    doubleVal: {
      values: [2.71828182],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['3.14'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['string value'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['char value'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['varchar value'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['2014-01-17 00:17:13'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['2014-01-17'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['1 00:00:00.000000000'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['0-1'],
      nulls: Buffer.from([0]),
    },
  },
  {
    binaryVal: {
      values: [Buffer.from([98, 105, 110, 97, 114, 121, 32, 118, 97, 108, 117, 101])],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: [
        '{"bool":false,"int_type":4000000,"big_int":372036854775807,"dbl":2.71828182,"dec":1.4142,"str":"string value","arr1":[1.41,2.71,3.14],"arr2":[{"sqrt2":1.414},{"e":2.718},{"pi":3.142}],"arr3":[{"s":"e","d":2.71},{"s":"pi","d":3.14}],"map1":{"e":2.71,"pi":3.14,"sqrt2":1.41},"map2":{"arr1":[1.414],"arr2":[2.718,3.141]},"map3":{"struct1":{"d":3.14,"n":314159265359},"struct2":{"d":2.71,"n":271828182846}},"struct1":{"s":"string value","d":3.14,"n":314159265359,"a":[2.718,3.141]}}',
      ],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['[1.41,2.71,3.14]'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['[{"sqrt2":1.4142},{"e":2.7182},{"pi":3.1415}]'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['[{"s":"sqrt2","d":1.41},{"s":"e","d":2.71},{"s":"pi","d":3.14}]'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['[[1.414],[2.718,3.141]]'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['{"e":2.71,"pi":3.14,"sqrt2":1.41}'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['{"arr1":[1.414],"arr2":[2.718,3.141]}'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['{"struct1":{"d":3.14,"n":314159265359},"struct2":{"d":2.71,"n":271828182846}}'],
      nulls: Buffer.from([0]),
    },
  },
  {
    stringVal: {
      values: ['{"e":[271828182846],"pi":[314159265359]}'],
      nulls: Buffer.from([0]),
    },
  },
];

export default data;
