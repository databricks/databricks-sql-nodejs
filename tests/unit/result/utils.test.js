const { expect } = require('chai');
const Int64 = require('node-int64');
const { TCLIService_types } = require('../../../').thrift;
const { getSchemaColumns, convertThriftValue } = require('../../../dist/result/utils');

const { TTypeId } = TCLIService_types;

describe('getSchemaColumns', () => {
  it('should handle missing schema', () => {
    const result = getSchemaColumns();
    expect(result).to.deep.equal([]);
  });

  it('should return ordered columns', () => {
    const columnA = { columnName: 'a', position: 2 };
    const columnB = { columnName: 'b', position: 3 };
    const columnC = { columnName: 'c', position: 1 };

    const result = getSchemaColumns({
      columns: [columnA, columnB, columnC],
    });

    expect(result).to.deep.equal([columnC, columnA, columnB]);
  });
});

describe('convertThriftValue', () => {
  it('should return value if type descriptor is missing', () => {
    const value = 'test';
    const result = convertThriftValue(undefined, value);
    expect(result).to.equal(value);
  });

  [TTypeId.DATE_TYPE, TTypeId.TIMESTAMP_TYPE].forEach((typeId) => {
    it(`should handle ${TTypeId[typeId]}`, () => {
      const value = new Date();
      const result = convertThriftValue({ type: typeId }, value);
      expect(result).to.equal(value);
    });
  });

  [TTypeId.UNION_TYPE, TTypeId.USER_DEFINED_TYPE].forEach((typeId) => {
    it(`should handle ${TTypeId[typeId]}`, () => {
      const value = {
        n: 3.14,
        s: 'test',
        toString: () => JSON.stringify(this, null, 2),
      };
      const result = convertThriftValue({ type: typeId }, value);
      expect(result).to.equal(String(value));
    });
  });

  [TTypeId.DECIMAL_TYPE].forEach((typeId) => {
    it(`should handle ${TTypeId[typeId]}`, () => {
      [
        ['3.14', 3.14],
        [123, 123],
      ].forEach(([value, expected]) => {
        const result = convertThriftValue({ type: typeId }, value);
        expect(result).to.equal(expected);
      });
    });
  });

  [TTypeId.BIGINT_TYPE].forEach((typeId) => {
    it(`should handle ${TTypeId[typeId]}`, () => {
      [
        [BigInt(123), 123],
        [new Int64(123), 123],
        [123, 123],
      ].forEach(([value, expected]) => {
        const result = convertThriftValue({ type: typeId }, value);
        expect(result).to.equal(expected);
      });
    });
  });

  [TTypeId.STRUCT_TYPE, TTypeId.MAP_TYPE].forEach((typeId) => {
    it(`should handle ${TTypeId[typeId]}`, () => {
      [
        [undefined, undefined],
        [{ s: 'test' }, { s: 'test' }],
        [JSON.stringify({ s: 'test' }), { s: 'test' }],
        ['{malformed json}', {}],
      ].forEach(([value, expected]) => {
        const result = convertThriftValue({ type: typeId }, value);
        expect(result).to.deep.equal(expected);
      });
    });
  });

  [TTypeId.ARRAY_TYPE].forEach((typeId) => {
    it(`should handle ${TTypeId[typeId]}`, () => {
      [
        [undefined, undefined],
        [['test'], ['test']],
        [JSON.stringify(['test']), ['test']],
        ['{malformed json}', []],
      ].forEach(([value, expected]) => {
        const result = convertThriftValue({ type: typeId }, value);
        expect(result).to.deep.equal(expected);
      });
    });
  });

  [
    TTypeId.NULL_TYPE,
    TTypeId.BINARY_TYPE,
    TTypeId.INTERVAL_YEAR_MONTH_TYPE,
    TTypeId.INTERVAL_DAY_TIME_TYPE,
    TTypeId.FLOAT_TYPE,
    TTypeId.DOUBLE_TYPE,
    TTypeId.INT_TYPE,
    TTypeId.SMALLINT_TYPE,
    TTypeId.TINYINT_TYPE,
    TTypeId.BOOLEAN_TYPE,
    TTypeId.STRING_TYPE,
    TTypeId.CHAR_TYPE,
    TTypeId.VARCHAR_TYPE,
  ].forEach((typeId) => {
    it(`should handle ${TTypeId[typeId]}`, () => {
      const value = 'test';
      const result = convertThriftValue({ type: typeId }, value);
      expect(result).to.equal(value);
    });
  });

  it('should return value if type is not recognized', () => {
    const value = 'test';
    const result = convertThriftValue({ type: null }, value);
    expect(result).to.equal(value);
  });
});
