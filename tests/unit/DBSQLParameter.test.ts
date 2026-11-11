import { expect } from 'chai';
import Int64 from 'node-int64';
import { TSparkParameterValue, TSparkParameter } from '../../thrift/TCLIService_types';
import { DBSQLParameter, DBSQLParameterType, DBSQLParameterValue } from '../../lib/DBSQLParameter';
import ParameterError from '../../lib/errors/ParameterError';

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

  describe('DECIMAL', () => {
    const decimal = (value: DBSQLParameterValue, type: string = DBSQLParameterType.DECIMAL) =>
      new DBSQLParameter({ type: type as DBSQLParameterType, value }).toSparkParameter();
    const expected = (type: string, stringValue: string) =>
      new TSparkParameter({ type, value: new TSparkParameterValue({ stringValue }) });

    it('derives precision and scale from the value instead of sending a scale-0 DECIMAL', () => {
      // A bare `DECIMAL` makes the Thrift server bind decimal(p,0): "123.45" → 123.
      expect(decimal('123.45')).to.deep.equal(expected('DECIMAL(5,2)', '123.45'));
      expect(decimal('-123.45')).to.deep.equal(expected('DECIMAL(5,2)', '-123.45'));
      expect(decimal('-123')).to.deep.equal(expected('DECIMAL(3,0)', '-123'));
      expect(decimal('1.500')).to.deep.equal(expected('DECIMAL(4,3)', '1.500'));
      expect(decimal('99.99', 'decimal')).to.deep.equal(expected('DECIMAL(4,2)', '99.99'));
    });

    it('excludes insignificant leading zeros from precision', () => {
      expect(decimal('0.00001')).to.deep.equal(expected('DECIMAL(5,5)', '0.00001'));
      expect(decimal('007.50')).to.deep.equal(expected('DECIMAL(3,2)', '007.50'));
      expect(decimal('.5')).to.deep.equal(expected('DECIMAL(1,1)', '.5'));
      expect(decimal('0')).to.deep.equal(expected('DECIMAL(1,0)', '0'));
      expect(decimal('000.000')).to.deep.equal(expected('DECIMAL(3,3)', '000.000'));
    });

    it('derives precision and scale for number, bigint and Int64 values', () => {
      expect(decimal(123.45)).to.deep.equal(expected('DECIMAL(5,2)', '123.45'));
      expect(decimal(-7)).to.deep.equal(expected('DECIMAL(1,0)', '-7'));
      expect(decimal(BigInt('12345678901234567890'))).to.deep.equal(expected('DECIMAL(20,0)', '12345678901234567890'));
      expect(decimal(new Int64(1234))).to.deep.equal(expected('DECIMAL(4,0)', '1234'));
    });

    it('rewrites exponent forms as plain numerals', () => {
      // `Number#toString` uses exponent notation below 1e-6 and from 1e21.
      expect(decimal(1e-7)).to.deep.equal(expected('DECIMAL(7,7)', '0.0000001'));
      expect(decimal(-1.5e-7)).to.deep.equal(expected('DECIMAL(8,8)', '-0.00000015'));
      expect(decimal(1e21)).to.deep.equal(expected('DECIMAL(22,0)', '1000000000000000000000'));
      expect(decimal('1.5e3')).to.deep.equal(expected('DECIMAL(4,0)', '1500'));
      expect(decimal('1.50E1')).to.deep.equal(expected('DECIMAL(3,1)', '15.0'));
      expect(decimal('+12.5e-1')).to.deep.equal(expected('DECIMAL(3,2)', '1.25'));
      expect(decimal('0.0e5')).to.deep.equal(expected('DECIMAL(1,0)', '0'));
      expect(decimal('0e999999999999')).to.deep.equal(expected('DECIMAL(1,0)', '0'));
      expect(decimal('123.')).to.deep.equal(expected('DECIMAL(3,0)', '123'));
    });

    it('accepts the full 38-digit precision', () => {
      const value = '12345678901234567890.123456789012345678';
      expect(decimal(value)).to.deep.equal(expected('DECIMAL(38,18)', value));
      expect(decimal('-0.12345678901234567890123456789012345678')).to.deep.equal(
        expected('DECIMAL(38,38)', '-0.12345678901234567890123456789012345678'),
      );
    });

    it('rejects a value that needs precision > 38 instead of truncating it', () => {
      const tooBig = () => decimal('123456789012345678901234567890123456789');
      expect(tooBig).to.throw(ParameterError, /needs precision 39, exceeding the Databricks maximum of 38/);
      expect(() => decimal('0.000000000000000000000000000000000000001')).to.throw(ParameterError, /precision 39/);
      expect(() => decimal('1e38')).to.throw(ParameterError, /precision 39/);
      expect(() => decimal('1e-39')).to.throw(ParameterError, /precision 39/);
      expect(() => decimal(`1e${'9'.repeat(400)}`)).to.throw(ParameterError, /exceeding the Databricks maximum/);
    });

    it('rejects non-numeric values', () => {
      for (const value of ['abc', '', ' ', '.', 'e5', '1,000', '1e', '0x10', 'NaN', NaN, Infinity, true]) {
        expect(() => decimal(value), String(value)).to.throw(ParameterError, /is not a decimal number/);
      }
    });

    it('leaves an explicitly qualified DECIMAL(p,s) type unchanged', () => {
      expect(decimal('123.45', 'DECIMAL(10,2)')).to.deep.equal(expected('DECIMAL(10,2)', '123.45'));
    });

    it('still binds a NULL DECIMAL without a type', () => {
      expect(decimal(null)).to.deep.equal(new TSparkParameter({}));
    });
  });

  it('binds a typed DATE from local calendar accessors, never via toISOString (no UTC off-by-one)', () => {
    // A `Date` constructed from local components must bind the wall-calendar
    // date the user intended, regardless of the process timezone.
    // `toISOString()` converts to UTC first, so in a positive-offset zone a
    // local-midnight `new Date(2024, 2, 14)` (internally 2024-03-13T..Z) would
    // bind "2024-03-13" — off by one. Sabotage `toISOString` to PROVE the DATE
    // path doesn't use it: this guards the regression in any timezone, including
    // the UTC CI runner where the two formulations would otherwise agree.
    const localDate = new Date(2024, 2, 14);
    (localDate as unknown as { toISOString: () => string }).toISOString = () => '1999-12-31T00:00:00.000Z';
    expect(new DBSQLParameter({ type: DBSQLParameterType.DATE, value: localDate }).toSparkParameter()).to.deep.equal(
      new TSparkParameter({
        type: DBSQLParameterType.DATE,
        value: new TSparkParameterValue({ stringValue: '2024-03-14' }),
      }),
    );
  });
});
