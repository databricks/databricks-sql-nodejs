import { expect } from 'chai';
import Int64 from 'node-int64';
import { TSparkParameterValue, TSparkParameter } from '../../thrift/TCLIService_types';
import { DBSQLParameter, DBSQLParameterType, DBSQLParameterValue } from '../../lib/DBSQLParameter';

describe('DBSQLParameter', () => {
  it('should infer types correctly', () => {
    const cases: Array<[DBSQLParameterValue, TSparkParameter]> = [
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
    const expectedType = '_CUSTOM_TYPE_' as DBSQLParameterType; // it doesn't have to be valid type name, just any string

    const cases: Array<[DBSQLParameterValue, TSparkParameter]> = [
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

  it('maps timezone-explicit timestamp types to valid Spark wire types', () => {
    // TIMESTAMP_NTZ is a real Spark type → bound verbatim.
    expect(
      new DBSQLParameter({ type: DBSQLParameterType.TIMESTAMP_NTZ, value: '2024-01-15 10:30:00' }).toSparkParameter(),
    ).to.deep.equal(
      new TSparkParameter({
        type: DBSQLParameterType.TIMESTAMP_NTZ,
        value: new TSparkParameterValue({ stringValue: '2024-01-15 10:30:00' }),
      }),
    );
    // TIMESTAMP_LTZ has no distinct Spark type → bound as TIMESTAMP (valid on
    // both Thrift and kernel; the old verbatim 'TIMESTAMP_LTZ' was rejected by
    // the Thrift server).
    expect(
      new DBSQLParameter({ type: DBSQLParameterType.TIMESTAMP_LTZ, value: '2024-01-15 10:30:00' }).toSparkParameter(),
    ).to.deep.equal(
      new TSparkParameter({
        type: DBSQLParameterType.TIMESTAMP,
        value: new TSparkParameterValue({ stringValue: '2024-01-15 10:30:00' }),
      }),
    );
  });
});
