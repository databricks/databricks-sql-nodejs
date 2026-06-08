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

  it('infers a fitting integer type by magnitude', () => {
    const cases: Array<[number, DBSQLParameterType, string]> = [
      // Within INT (i32) range → INTEGER.
      [42, DBSQLParameterType.INTEGER, '42'],
      [2147483647, DBSQLParameterType.INTEGER, '2147483647'],
      [-2147483648, DBSQLParameterType.INTEGER, '-2147483648'],
      // Beyond i32 but a safe integer → BIGINT (INTEGER would overflow the
      // server's INT literal parse).
      [3000000000, DBSQLParameterType.BIGINT, '3000000000'],
      // Whole-number double outside the safe-integer range → DOUBLE, not
      // INTEGER. Regression: `Number.isInteger(1e30)` is `true`, so this used
      // to be typed INTEGER and rejected as `invalid INT literal "1e+30"`.
      [1e30, DBSQLParameterType.DOUBLE, '1e+30'],
    ];
    for (const [value, type, stringValue] of cases) {
      expect(new DBSQLParameter({ value }).toSparkParameter()).to.deep.equal(
        new TSparkParameter({ type, value: new TSparkParameterValue({ stringValue }) }),
      );
    }
  });

  it('binds a Date as a calendar date when typed DATE', () => {
    // Explicit DATE type → date-only `yyyy-mm-dd`. The full ISO timestamp is
    // rejected by the SEA wire as a DATE literal ("trailing input").
    expect(
      new DBSQLParameter({
        type: DBSQLParameterType.DATE,
        value: new Date(Date.UTC(2024, 0, 15, 10, 30, 0)),
      }).toSparkParameter(),
    ).to.deep.equal(
      new TSparkParameter({
        type: DBSQLParameterType.DATE,
        value: new TSparkParameterValue({ stringValue: '2024-01-15' }),
      }),
    );
    // Without an explicit type a Date still binds as a full TIMESTAMP.
    expect(new DBSQLParameter({ value: new Date('2023-09-06T03:14:27.843Z') }).toSparkParameter()).to.deep.equal(
      new TSparkParameter({
        type: DBSQLParameterType.TIMESTAMP,
        value: new TSparkParameterValue({ stringValue: '2023-09-06T03:14:27.843Z' }),
      }),
    );
  });
});
