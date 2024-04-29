const { expect } = require('chai');

const Int64 = require('node-int64');
const { TSparkParameterValue, TSparkParameter } = require('../../thrift/TCLIService_types');
const { DBSQLParameter, DBSQLParameterType } = require('../../lib/DBSQLParameter');

describe('DBSQLParameter', () => {
  it('should infer types correctly', () => {
    const cases = [
      [
        false,
        new TSparkParameter({
          type: DBSQLParameterType.BOOLEAN,
          value: new TSparkParameterValue({ stringValue: 'FALSE' }),
        }),
      ],
      [
        true,
        new TSparkParameter({
          type: DBSQLParameterType.BOOLEAN,
          value: new TSparkParameterValue({ stringValue: 'TRUE' }),
        }),
      ],
      [
        123,
        new TSparkParameter({
          type: DBSQLParameterType.INTEGER,
          value: new TSparkParameterValue({ stringValue: '123' }),
        }),
      ],
      [
        3.14,
        new TSparkParameter({
          type: DBSQLParameterType.DOUBLE,
          value: new TSparkParameterValue({ stringValue: '3.14' }),
        }),
      ],
      [
        BigInt(1234),
        new TSparkParameter({
          type: DBSQLParameterType.BIGINT,
          value: new TSparkParameterValue({ stringValue: '1234' }),
        }),
      ],
      [
        new Int64(1234),
        new TSparkParameter({
          type: DBSQLParameterType.BIGINT,
          value: new TSparkParameterValue({ stringValue: '1234' }),
        }),
      ],
      [
        new Date('2023-09-06T03:14:27.843Z'),
        new TSparkParameter({
          type: DBSQLParameterType.TIMESTAMP,
          value: new TSparkParameterValue({ stringValue: '2023-09-06T03:14:27.843Z' }),
        }),
      ],
      [
        'Hello',
        new TSparkParameter({
          type: DBSQLParameterType.STRING,
          value: new TSparkParameterValue({ stringValue: 'Hello' }),
        }),
      ],
    ];

    for (const [value, expectedParam] of cases) {
      const dbsqlParam = new DBSQLParameter({ value });
      expect(dbsqlParam.toSparkParameter()).to.deep.equal(expectedParam);
    }
  });

  it('should use provided type', () => {
    const expectedType = '_CUSTOM_TYPE_'; // it doesn't have to be valid type name, just any string

    const cases = [
      [false, new TSparkParameter({ type: expectedType, value: new TSparkParameterValue({ stringValue: 'FALSE' }) })],
      [true, new TSparkParameter({ type: expectedType, value: new TSparkParameterValue({ stringValue: 'TRUE' }) })],
      [123, new TSparkParameter({ type: expectedType, value: new TSparkParameterValue({ stringValue: '123' }) })],
      [3.14, new TSparkParameter({ type: expectedType, value: new TSparkParameterValue({ stringValue: '3.14' }) })],
      [
        BigInt(1234),
        new TSparkParameter({ type: expectedType, value: new TSparkParameterValue({ stringValue: '1234' }) }),
      ],
      [
        new Int64(1234),
        new TSparkParameter({ type: expectedType, value: new TSparkParameterValue({ stringValue: '1234' }) }),
      ],
      [
        new Date('2023-09-06T03:14:27.843Z'),
        new TSparkParameter({
          type: expectedType,
          value: new TSparkParameterValue({ stringValue: '2023-09-06T03:14:27.843Z' }),
        }),
      ],
      ['Hello', new TSparkParameter({ type: expectedType, value: new TSparkParameterValue({ stringValue: 'Hello' }) })],
    ];

    for (const [value, expectedParam] of cases) {
      const dbsqlParam = new DBSQLParameter({ type: expectedType, value });
      expect(dbsqlParam.toSparkParameter()).to.deep.equal(expectedParam);
    }
  });
});
