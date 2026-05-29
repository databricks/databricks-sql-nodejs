import { expect } from 'chai';
import Int64 from 'node-int64';
import { TSparkParameterValue, TSparkParameter } from '../../thrift/TCLIService_types';
import {
  DBSQLParameter,
  DBSQLParameterType,
  DBSQLParameterValue,
  TypedValueInput,
  convertOrdinalParametersToTypedValueInputs,
} from '../../lib/DBSQLParameter';

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

  // ─── toNapiTypedValue (SEA-backend codec input) ─────────────────────
  //
  // These tests pin the wire-level alignment between the Thrift emitter
  // (`toSparkParameter`) and the SEA emitter (`toNapiTypedValue`).
  // Divergence between the two is a parity regression — see C5 cluster
  // in sea-workflow/decisions/2026-05-28-autonomous-parity-decisions.md.

  describe('toNapiTypedValue', () => {
    it('infers types in sync with toSparkParameter (10-type C5 matrix)', () => {
      const cases: Array<[DBSQLParameterValue, TypedValueInput]> = [
        // BOOLEAN — note "TRUE"/"FALSE" casing matches Thrift; the napi
        // codec's parse_bool is case-insensitive so this is wire-safe.
        [false, { sqlType: DBSQLParameterType.BOOLEAN, value: 'FALSE' }],
        [true, { sqlType: DBSQLParameterType.BOOLEAN, value: 'TRUE' }],
        // INTEGER from a plain JS number (`Number.isInteger(123) === true`).
        [123, { sqlType: DBSQLParameterType.INTEGER, value: '123' }],
        // DOUBLE from a non-integer JS number.
        [3.14, { sqlType: DBSQLParameterType.DOUBLE, value: '3.14' }],
        // BIGINT from JS bigint.
        [BigInt(9999999999), { sqlType: DBSQLParameterType.BIGINT, value: '9999999999' }],
        // BIGINT from Int64 (the node-int64 path the Thrift driver uses).
        [new Int64(1234), { sqlType: DBSQLParameterType.BIGINT, value: '1234' }],
        // TIMESTAMP from a JS Date — `.toISOString()` emits the trailing
        // `Z`, the napi codec strips it before NaiveDateTime parsing.
        [
          new Date('2023-09-06T03:14:27.843Z'),
          { sqlType: DBSQLParameterType.TIMESTAMP, value: '2023-09-06T03:14:27.843Z' },
        ],
        // STRING fallback.
        ['Hello', { sqlType: DBSQLParameterType.STRING, value: 'Hello' }],
      ];
      for (const [value, expected] of cases) {
        const param = new DBSQLParameter({ value });
        expect(param.toNapiTypedValue()).to.deep.equal(expected);
      }
    });

    it('honours an explicitly-set type', () => {
      const customType = 'DECIMAL(10,2)' as DBSQLParameterType;
      const param = new DBSQLParameter({ type: customType, value: '-123.45' });
      expect(param.toNapiTypedValue()).to.deep.equal({
        sqlType: 'DECIMAL(10,2)',
        value: '-123.45',
      });
    });

    it('emits VOID/null for an explicit VOID type regardless of value', () => {
      const param = new DBSQLParameter({ type: DBSQLParameterType.VOID, value: 'ignored' });
      expect(param.toNapiTypedValue()).to.deep.equal({ sqlType: 'VOID', value: null });
    });

    it('emits VOID/null for an undefined or null value', () => {
      for (const value of [undefined, null] as Array<DBSQLParameterValue>) {
        const param = new DBSQLParameter({ value });
        expect(param.toNapiTypedValue()).to.deep.equal({ sqlType: 'VOID', value: null });
      }
    });

    it('alignment regression-lock: every type-inference branch matches the Thrift emitter', () => {
      // Every value the Thrift emitter handles must round-trip to the
      // same wire-level (sqlType, stringValue) pair through the napi
      // emitter. This is the one-liner that catches a future
      // refactor that diverges the two stringification paths.
      const values: Array<DBSQLParameterValue> = [
        false,
        true,
        0,
        1,
        -1,
        3.14,
        BigInt(0),
        new Int64(0),
        new Date(0),
        '',
      ];
      for (const value of values) {
        const param = new DBSQLParameter({ value });
        const thrift = param.toSparkParameter();
        const napi = param.toNapiTypedValue();
        expect(napi.sqlType, `sqlType for ${String(value)}`).to.equal(thrift.type);
        expect(napi.value, `value for ${String(value)}`).to.equal(thrift.value?.stringValue);
      }
    });
  });

  describe('convertOrdinalParametersToTypedValueInputs', () => {
    it('returns [] for undefined input', () => {
      expect(convertOrdinalParametersToTypedValueInputs(undefined)).to.deep.equal([]);
    });

    it('returns [] for an empty array', () => {
      expect(convertOrdinalParametersToTypedValueInputs([])).to.deep.equal([]);
    });

    it('wraps bare JS values in a DBSQLParameter for stringification', () => {
      const got = convertOrdinalParametersToTypedValueInputs([42, 'hello', true, null]);
      expect(got).to.deep.equal([
        { sqlType: DBSQLParameterType.INTEGER, value: '42' },
        { sqlType: DBSQLParameterType.STRING, value: 'hello' },
        { sqlType: DBSQLParameterType.BOOLEAN, value: 'TRUE' },
        // null → VOID/null per the toNapiTypedValue contract.
        { sqlType: 'VOID', value: null },
      ]);
    });

    it('passes DBSQLParameter instances through with their explicit type', () => {
      const decimal = new DBSQLParameter({
        type: 'DECIMAL(10,2)' as DBSQLParameterType,
        value: '-123.45',
      });
      const got = convertOrdinalParametersToTypedValueInputs([decimal, BigInt('9999999999')]);
      expect(got).to.deep.equal([
        { sqlType: 'DECIMAL(10,2)', value: '-123.45' },
        { sqlType: DBSQLParameterType.BIGINT, value: '9999999999' },
      ]);
    });

    it('preserves order — index i in the input is index i on the wire', () => {
      // The napi codec's `positional_params` is 1-based (index i ↔
      // ordinal i+1). The JS adapter is responsible for preserving the
      // input ordering — this test pins that contract.
      const got = convertOrdinalParametersToTypedValueInputs([1, 2, 3, 4, 5]);
      expect(got.map((x) => x.value)).to.deep.equal(['1', '2', '3', '4', '5']);
    });

    it('c5 ten-type matrix — one positional value per basic SQL type', () => {
      // Mirrors the kernel-napi `c5_ten_type_matrix_round_trips` test
      // (databricks-sql-kernel/napi/src/params.rs) one-for-one. The
      // wire-level (sqlType, value) pairs emitted here must be the
      // exact input the kernel codec accepts. BINARY is omitted —
      // the napi codec rejects it at bind time, see the cross-driver
      // validation reply from python-sea-oracle for the contract.
      const inputs: Array<DBSQLParameter | DBSQLParameterValue> = [
        new DBSQLParameter({ type: DBSQLParameterType.INTEGER, value: 42 }),
        new DBSQLParameter({ type: DBSQLParameterType.BIGINT, value: BigInt('9999999999') }),
        new DBSQLParameter({ type: DBSQLParameterType.FLOAT, value: 3.14 }),
        new DBSQLParameter({ type: DBSQLParameterType.DOUBLE, value: 2.718281828459045 }),
        new DBSQLParameter({ type: DBSQLParameterType.STRING, value: 'hello world' }),
        new DBSQLParameter({ type: DBSQLParameterType.BOOLEAN, value: true }),
        new DBSQLParameter({ type: DBSQLParameterType.DATE, value: '2026-05-15' }),
        new DBSQLParameter({ type: DBSQLParameterType.TIMESTAMP, value: '2026-05-15 12:30:45' }),
        new DBSQLParameter({ type: 'DECIMAL(10,2)' as DBSQLParameterType, value: '-123.45' }),
      ];
      const got = convertOrdinalParametersToTypedValueInputs(inputs);
      expect(got).to.have.lengthOf(9);
      expect(got[0]).to.deep.equal({ sqlType: 'INTEGER', value: '42' });
      expect(got[1]).to.deep.equal({ sqlType: 'BIGINT', value: '9999999999' });
      expect(got[2]).to.deep.equal({ sqlType: 'FLOAT', value: '3.14' });
      expect(got[3]).to.deep.equal({ sqlType: 'DOUBLE', value: '2.718281828459045' });
      expect(got[4]).to.deep.equal({ sqlType: 'STRING', value: 'hello world' });
      expect(got[5]).to.deep.equal({ sqlType: 'BOOLEAN', value: 'TRUE' });
      expect(got[6]).to.deep.equal({ sqlType: 'DATE', value: '2026-05-15' });
      expect(got[7]).to.deep.equal({ sqlType: 'TIMESTAMP', value: '2026-05-15 12:30:45' });
      expect(got[8]).to.deep.equal({ sqlType: 'DECIMAL(10,2)', value: '-123.45' });
    });
  });
});
