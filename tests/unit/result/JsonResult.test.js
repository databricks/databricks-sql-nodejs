const { expect } = require('chai');
const JsonResult = require('../../../dist/result/JsonResult').default;
const { TCLIService_types } = require('../../../').thrift;
const Int64 = require('node-int64');

const getColumnSchema = (name, type, position) => {
  return {
    columnName: name,
    typeDesc: {
      types: [
        {
          primitiveEntry: {
            type,
          },
        },
      ],
    },
    position,
  };
};

describe('JsonResult', () => {
  it('should convert schema with primitive types to json', () => {
    const schema = {
      columns: [
        getColumnSchema('table.str', TCLIService_types.TTypeId.STRING_TYPE, 1),
        getColumnSchema('table.int64', TCLIService_types.TTypeId.BIGINT_TYPE, 2),
        getColumnSchema('table.bin', TCLIService_types.TTypeId.BINARY_TYPE, 3),
        getColumnSchema('table.bool', TCLIService_types.TTypeId.BOOLEAN_TYPE, 4),
        getColumnSchema('table.char', TCLIService_types.TTypeId.CHAR_TYPE, 5),
        getColumnSchema('table.dbl', TCLIService_types.TTypeId.DOUBLE_TYPE, 6),
        getColumnSchema('table.flt', TCLIService_types.TTypeId.FLOAT_TYPE, 7),
        getColumnSchema('table.int', TCLIService_types.TTypeId.INT_TYPE, 8),
        getColumnSchema('table.small_int', TCLIService_types.TTypeId.SMALLINT_TYPE, 9),
        getColumnSchema('table.tiny_int', TCLIService_types.TTypeId.TINYINT_TYPE, 10),
        getColumnSchema('table.varch', TCLIService_types.TTypeId.VARCHAR_TYPE, 11),
        getColumnSchema('table.dec', TCLIService_types.TTypeId.DECIMAL_TYPE, 12),
        getColumnSchema('table.ts', TCLIService_types.TTypeId.TIMESTAMP_TYPE, 13),
        getColumnSchema('table.date', TCLIService_types.TTypeId.DATE_TYPE, 14),
        getColumnSchema('table.day_interval', TCLIService_types.TTypeId.INTERVAL_DAY_TIME_TYPE, 15),
        getColumnSchema('table.month_interval', TCLIService_types.TTypeId.INTERVAL_YEAR_MONTH_TYPE, 16),
      ],
    };
    const data = [
      {
        columns: [
          {
            stringVal: { values: ['a', 'b'] },
          },
          {
            i64Val: {
              values: [
                new Int64(Buffer.from([0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01])),
                new Int64(Buffer.from([0x00, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02])),
              ],
            },
          },
          {
            binaryVal: { values: [Buffer.from([1]), Buffer.from([2])] },
          },
          {
            boolVal: { values: [true, false] },
          },
          {
            stringVal: { values: ['c', 'd'] },
          },
          {
            doubleVal: { values: [1.2, 1.3] },
          },
          {
            doubleVal: { values: [2.2, 2.3] },
          },
          {
            i32Val: { values: [1, 2] },
          },
          {
            i16Val: { values: [3, 4] },
          },
          {
            byteVal: { values: [5, 6] },
          },
          {
            stringVal: { values: ['e', 'f'] },
          },
          {
            stringVal: { values: ['2.1', '2.2'] },
          },
          {
            stringVal: { values: ['2020-01-17 00:17:13.0', '2020-01-17 00:17:13.0'] },
          },
          {
            stringVal: { values: ['2020-01-17', '2020-01-17'] },
          },
          {
            stringVal: { values: ['1 00:00:00.000000000', '1 00:00:00.000000000'] },
          },
          {
            stringVal: { values: ['0-1', '0-1'] },
          },
        ],
      },
    ];

    const result = new JsonResult(schema);

    expect(result.getValue(data)).to.be.deep.eq([
      {
        'table.str': 'a',
        'table.int64': 282578800148737,
        'table.bin': Buffer.from([1]),
        'table.bool': true,
        'table.char': 'c',
        'table.dbl': 1.2,
        'table.flt': 2.2,
        'table.int': 1,
        'table.small_int': 3,
        'table.tiny_int': 5,
        'table.varch': 'e',
        'table.dec': 2.1,
        'table.ts': '2020-01-17 00:17:13.0',
        'table.date': '2020-01-17',
        'table.day_interval': '1 00:00:00.000000000',
        'table.month_interval': '0-1',
      },
      {
        'table.str': 'b',
        'table.int64': 565157600297474,
        'table.bin': Buffer.from([2]),
        'table.bool': false,
        'table.char': 'd',
        'table.dbl': 1.3,
        'table.flt': 2.3,
        'table.int': 2,
        'table.small_int': 4,
        'table.tiny_int': 6,
        'table.varch': 'f',
        'table.dec': 2.2,
        'table.ts': '2020-01-17 00:17:13.0',
        'table.date': '2020-01-17',
        'table.day_interval': '1 00:00:00.000000000',
        'table.month_interval': '0-1',
      },
    ]);
  });

  it('should convert complex types', () => {
    const schema = {
      columns: [
        getColumnSchema('table.array', TCLIService_types.TTypeId.ARRAY_TYPE, 1),
        getColumnSchema('table.map', TCLIService_types.TTypeId.MAP_TYPE, 2),
        getColumnSchema('table.struct', TCLIService_types.TTypeId.STRUCT_TYPE, 3),
        getColumnSchema('table.union', TCLIService_types.TTypeId.UNION_TYPE, 4),
      ],
    };
    const data = [
      {
        columns: [
          {
            stringVal: { values: ['["a", "b"]', '["c", "d"]'] },
          },
          {
            stringVal: { values: ['{ "key": 12 }', '{ "key": 13 }'] },
          },
          {
            stringVal: { values: ['{ "name": "Jon", "surname": "Doe" }', '{ "name": "Jane", "surname": "Doe" }'] },
          },
          {
            stringVal: { values: ['{0:12}', '{1:"foo"}'] },
          },
        ],
      },
    ];

    const result = new JsonResult(schema);

    expect(result.getValue(data)).to.be.deep.eq([
      {
        'table.array': ['a', 'b'],
        'table.map': { key: 12 },
        'table.struct': { name: 'Jon', surname: 'Doe' },
        'table.union': '{0:12}',
      },
      {
        'table.array': ['c', 'd'],
        'table.map': { key: 13 },
        'table.struct': { name: 'Jane', surname: 'Doe' },
        'table.union': '{1:"foo"}',
      },
    ]);
  });

  it('should merge data items', () => {
    const schema = {
      columns: [getColumnSchema('table.id', TCLIService_types.TTypeId.STRING_TYPE, 1)],
    };
    const data = [
      {
        columns: [
          {
            stringVal: { values: ['0', '1'] },
          },
        ],
      },
      {
        columns: [
          {
            stringVal: { values: ['2', '3'] },
          },
        ],
      },
    ];

    const result = new JsonResult(schema);

    expect(result.getValue(data)).to.be.deep.eq([
      { 'table.id': '0' },
      { 'table.id': '1' },
      { 'table.id': '2' },
      { 'table.id': '3' },
    ]);
  });

  it('should detect nulls', () => {
    const result = new JsonResult(null);
    const buf = Buffer.from([0x55, 0xaa, 0xc3]);

    [
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false, // 0x55
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true, // 0xAA,
      true,
      true,
      false,
      false,
      false,
      false,
      true,
      true, // 0xC3
    ].forEach((value, i) => {
      expect(result.isNull(buf, i), i).to.be.eq(value);
    });
  });

  it('should detect nulls for each type', () => {
    const schema = {
      columns: [
        getColumnSchema('table.str', TCLIService_types.TTypeId.STRING_TYPE, 1),
        getColumnSchema('table.int64', TCLIService_types.TTypeId.BIGINT_TYPE, 2),
        getColumnSchema('table.bin', TCLIService_types.TTypeId.BINARY_TYPE, 3),
        getColumnSchema('table.bool', TCLIService_types.TTypeId.BOOLEAN_TYPE, 4),
        getColumnSchema('table.char', TCLIService_types.TTypeId.CHAR_TYPE, 5),
        getColumnSchema('table.dbl', TCLIService_types.TTypeId.DOUBLE_TYPE, 6),
        getColumnSchema('table.flt', TCLIService_types.TTypeId.FLOAT_TYPE, 7),
        getColumnSchema('table.int', TCLIService_types.TTypeId.INT_TYPE, 8),
        getColumnSchema('table.small_int', TCLIService_types.TTypeId.SMALLINT_TYPE, 9),
        getColumnSchema('table.tiny_int', TCLIService_types.TTypeId.TINYINT_TYPE, 10),
        getColumnSchema('table.varch', TCLIService_types.TTypeId.VARCHAR_TYPE, 11),
        getColumnSchema('table.dec', TCLIService_types.TTypeId.DECIMAL_TYPE, 12),
        getColumnSchema('table.ts', TCLIService_types.TTypeId.TIMESTAMP_TYPE, 13),
        getColumnSchema('table.date', TCLIService_types.TTypeId.DATE_TYPE, 14),
        getColumnSchema('table.day_interval', TCLIService_types.TTypeId.INTERVAL_DAY_TIME_TYPE, 15),
        getColumnSchema('table.month_interval', TCLIService_types.TTypeId.INTERVAL_YEAR_MONTH_TYPE, 16),
      ],
    };
    const data = [
      {
        columns: [
          {
            stringVal: { values: ['a'], nulls: Buffer.from([0x01]) },
          },
          {
            i64Val: {
              values: [new Int64(Buffer.from([0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01]))],
              nulls: Buffer.from([0x01]),
            },
          },
          {
            binaryVal: { values: [Buffer.from([1])], nulls: Buffer.from([0x01]) },
          },
          {
            boolVal: { values: [true], nulls: Buffer.from([0x01]) },
          },
          {
            stringVal: { values: ['c'], nulls: Buffer.from([0x01]) },
          },
          {
            doubleVal: { values: [1.2], nulls: Buffer.from([0x01]) },
          },
          {
            doubleVal: { values: [2.2], nulls: Buffer.from([0x01]) },
          },
          {
            i32Val: { values: [1], nulls: Buffer.from([0x01]) },
          },
          {
            i16Val: { values: [3], nulls: Buffer.from([0x01]) },
          },
          {
            byteVal: { values: [5], nulls: Buffer.from([0x01]) },
          },
          {
            stringVal: { values: ['e'], nulls: Buffer.from([0x01]) },
          },
          {
            stringVal: { values: ['2.1'], nulls: Buffer.from([0x01]) },
          },
          {
            stringVal: { values: ['2020-01-17 00:17:13.0'], nulls: Buffer.from([0x01]) },
          },
          {
            stringVal: { values: ['2020-01-17'], nulls: Buffer.from([0x01]) },
          },
          {
            stringVal: { values: ['1 00:00:00.000000000'], nulls: Buffer.from([0x01]) },
          },
          {
            stringVal: { values: ['0-1'], nulls: Buffer.from([0x01]) },
          },
        ],
      },
    ];

    const result = new JsonResult(schema);

    expect(result.getValue(data)).to.be.deep.eq([
      {
        'table.str': null,
        'table.int64': null,
        'table.bin': null,
        'table.bool': null,
        'table.char': null,
        'table.dbl': null,
        'table.flt': null,
        'table.int': null,
        'table.small_int': null,
        'table.tiny_int': null,
        'table.varch': null,
        'table.dec': null,
        'table.ts': null,
        'table.date': null,
        'table.day_interval': null,
        'table.month_interval': null,
      },
    ]);
  });

  it('should return empty array if no data to process', () => {
    const schema = {
      columns: [getColumnSchema('table.id', TCLIService_types.TTypeId.STRING_TYPE, 1)],
    };

    const result = new JsonResult(schema);

    expect(result.getValue()).to.be.deep.eq([]);
    expect(result.getValue([])).to.be.deep.eq([]);
  });

  it('should return empty array if no schema available', () => {
    const data = [
      {
        columns: [
          {
            stringVal: { values: ['0', '1'] },
          },
        ],
      },
    ];

    const result = new JsonResult();

    expect(result.getValue(data)).to.be.deep.eq([]);
  });
});
