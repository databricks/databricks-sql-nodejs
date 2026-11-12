import Int64 from 'node-int64';
import { TSparkParameter, TSparkParameterValue } from '../thrift/TCLIService_types';
import ParameterError from './errors/ParameterError';

export type DBSQLParameterValue = undefined | null | boolean | number | bigint | Int64 | Date | string;

export enum DBSQLParameterType {
  VOID = 'VOID', // aka NULL
  STRING = 'STRING',
  DATE = 'DATE',
  TIMESTAMP = 'TIMESTAMP',
  // `TIMESTAMP_NTZ` binds a timezone-free (wall-clock) timestamp. It is a real
  // Spark type, bound natively on both the Thrift and kernel backends (requires
  // a server that supports TIMESTAMP_NTZ; Spark 3.4+ / recent DBR).
  TIMESTAMP_NTZ = 'TIMESTAMP_NTZ',
  // `TIMESTAMP_LTZ` is an alias for `TIMESTAMP`: Spark has no distinct
  // TIMESTAMP_LTZ type — `TIMESTAMP` already carries local/instant (LTZ)
  // semantics. `toSparkParameter` therefore binds it as `TIMESTAMP` on the wire
  // (valid on both backends); it exists only as a self-documenting alias.
  TIMESTAMP_LTZ = 'TIMESTAMP_LTZ',
  FLOAT = 'FLOAT',
  DECIMAL = 'DECIMAL',
  DOUBLE = 'DOUBLE',
  INTEGER = 'INTEGER',
  BIGINT = 'BIGINT',
  SMALLINT = 'SMALLINT',
  TINYINT = 'TINYINT',
  BOOLEAN = 'BOOLEAN',
  INTERVALMONTH = 'INTERVAL MONTH',
  INTERVALDAY = 'INTERVAL DAY',
}

// 32-bit signed integer bounds — the range of the Spark `INT` type.
const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

/**
 * Infer the Spark parameter type for a JS `number` when the caller didn't set
 * one explicitly.
 *
 * A JS `number` is an IEEE-754 double, so a whole-number value can still be far
 * outside the `INT` range (e.g. `1e30`). Typing such a value as `INTEGER`
 * makes the server reject it (`invalid INT literal "1e+30"`). Pick the
 * narrowest type that actually fits:
 *   - non-integer / non-finite → `DOUBLE`
 *   - integer within INT (i32) range → `INTEGER`
 *   - integer within the safe-integer range → `BIGINT`
 *   - anything larger → `DOUBLE` (can't be represented exactly as an integer
 *     anyway; callers needing exact 64-bit integers should pass a `bigint`).
 */
function inferNumberType(value: number): DBSQLParameterType {
  if (!Number.isInteger(value)) {
    return DBSQLParameterType.DOUBLE;
  }
  if (value >= INT32_MIN && value <= INT32_MAX) {
    return DBSQLParameterType.INTEGER;
  }
  if (Number.isSafeInteger(value)) {
    return DBSQLParameterType.BIGINT;
  }
  return DBSQLParameterType.DOUBLE;
}

const DECIMAL_MAX_PRECISION = 38;

/**
 * Qualify a bare `DECIMAL` (or its `DEC` / `NUMERIC` synonyms) with the
 * precision and scale of its value. Both backends otherwise lose the fraction:
 * the Thrift server binds a scale-0 decimal, truncating `"123.45"` to `123`,
 * and the kernel's raw-param path forwards the type verbatim, so SEA binds it
 * as `DECIMAL(10,0)` and rounds.
 *
 * Precision is significant integer digits plus scale (Spark's decimal-literal
 * rule), so `"99.99"` ⇒ `DECIMAL(4,2)` and `"0.00001"` ⇒ `DECIMAL(5,5)`. Plain
 * numerals are sent unchanged; exponent forms (`1e-7`, which `Number#toString`
 * produces) are rewritten as the equivalent plain numeral. A non-numeric value
 * or one needing precision > 38 throws instead of being truncated server-side.
 */
function qualifyDecimal(stringValue: string): { type: string; value: string } {
  const m = /^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/.exec(stringValue.trim());
  const [, sign = '', intPart = '', fracPart, exponent] = m ?? [];
  if (!m || (intPart === '' && !fracPart)) {
    throw new ParameterError(
      `DECIMAL parameter value "${stringValue}" is not a decimal number (expected e.g. "123.45" or "1.5e-7").`,
    );
  }

  const digits = intPart + (fracPart ?? '');
  const exp = exponent === undefined ? 0 : Number(exponent);
  // Position of the decimal point within `digits` once the exponent is applied.
  const pointPos = intPart.length + exp;
  const leadingZeros = digits.length - digits.replace(/^0+/, '').length;
  const isZero = leadingZeros === digits.length;
  const scale = Math.max(digits.length - pointPos, 0);
  const significantIntDigits = isZero ? 0 : Math.max(pointPos - leadingZeros, 0);
  const precision = Math.max(significantIntDigits + scale, 1);
  if (precision > DECIMAL_MAX_PRECISION) {
    throw new ParameterError(
      `DECIMAL parameter value "${stringValue}" needs precision ${precision}, exceeding the Databricks maximum of ` +
        `${DECIMAL_MAX_PRECISION}. Round/scale the value or bind it as a STRING.`,
    );
  }

  const type = `DECIMAL(${precision},${scale})`;
  if (exponent === undefined && fracPart !== '') {
    return { type, value: stringValue };
  }
  // Rebuild as a plain numeral; both padding lengths are bounded by `precision`.
  let plain: string;
  if (isZero) {
    plain = scale > 0 ? `0.${'0'.repeat(scale)}` : '0';
  } else if (pointPos >= digits.length) {
    plain = digits + '0'.repeat(pointPos - digits.length);
  } else if (pointPos <= 0) {
    plain = `0.${'0'.repeat(-pointPos)}${digits}`;
  } else {
    plain = `${digits.slice(0, pointPos)}.${digits.slice(pointPos)}`;
  }
  plain = plain.replace(/^0+(?=\d)/, '');
  return { type, value: sign === '-' ? `-${plain}` : plain };
}

// Unqualified decimal type names; a qualified `DECIMAL(p,s)` is left as given.
const BARE_DECIMAL_TYPE = /^(DECIMAL|DEC|NUMERIC)$/i;

/**
 * Exact decimal string for an `Int64`. `Int64#toString` goes through a JS
 * number and returns `"Infinity"` for magnitudes ≥ 2^53, so read the
 * big-endian two's-complement bytes as a `bigint` instead.
 */
function int64ToString(value: Int64): string {
  return value.toBuffer().readBigInt64BE(0).toString();
}

interface DBSQLParameterOptions {
  type?: DBSQLParameterType;
  value: DBSQLParameterValue;
}

interface ToSparkParameterOptions {
  name?: string;
}

export class DBSQLParameter {
  public readonly type?: string;

  public readonly value: DBSQLParameterValue;

  constructor({ type, value }: DBSQLParameterOptions) {
    this.type = type;
    this.value = value;
  }

  public toSparkParameter(options: ToSparkParameterOptions = {}): TSparkParameter {
    const param = this.toUnqualifiedSparkParameter(options);
    const stringValue = param.value?.stringValue;
    // Only an explicit type can be DECIMAL; inferred types never are.
    if (this.type !== undefined && BARE_DECIMAL_TYPE.test(this.type.trim()) && typeof stringValue === 'string') {
      const decimal = qualifyDecimal(stringValue);
      param.type = decimal.type;
      param.value = new TSparkParameterValue({ stringValue: decimal.value });
    }
    return param;
  }

  private toUnqualifiedSparkParameter({ name }: ToSparkParameterOptions): TSparkParameter {
    // If VOID type was set explicitly - ignore value
    if (this.type === DBSQLParameterType.VOID) {
      return new TSparkParameter({ name }); // for NULL neither `type` nor `value` should be set
    }

    // Infer NULL values
    if (this.value === undefined || this.value === null) {
      return new TSparkParameter({ name }); // for NULL neither `type` nor `value` should be set
    }

    // Map timezone-explicit timestamp aliases to their Spark wire type. Spark
    // has no distinct TIMESTAMP_LTZ type (TIMESTAMP carries LTZ semantics), so
    // bind it as TIMESTAMP — valid on both the Thrift and kernel backends.
    // TIMESTAMP_NTZ is a real Spark type and is bound natively.
    const wireType = this.type === DBSQLParameterType.TIMESTAMP_LTZ ? DBSQLParameterType.TIMESTAMP : this.type;

    if (typeof this.value === 'boolean') {
      return new TSparkParameter({
        name,
        type: wireType ?? DBSQLParameterType.BOOLEAN,
        value: new TSparkParameterValue({
          stringValue: this.value ? 'TRUE' : 'FALSE',
        }),
      });
    }

    if (typeof this.value === 'number') {
      return new TSparkParameter({
        name,
        type: wireType ?? inferNumberType(this.value),
        value: new TSparkParameterValue({
          stringValue: Number(this.value).toString(),
        }),
      });
    }

    if (this.value instanceof Int64 || typeof this.value === 'bigint') {
      return new TSparkParameter({
        name,
        type: wireType ?? DBSQLParameterType.BIGINT,
        value: new TSparkParameterValue({
          stringValue: this.value instanceof Int64 ? int64ToString(this.value) : this.value.toString(),
        }),
      });
    }

    if (this.value instanceof Date) {
      // A `Date` bound as `DATE` must project a calendar date (`yyyy-mm-dd`),
      // not a full ISO-8601 timestamp: the SEA wire rejects
      // `2024-03-14T00:00:00.000Z` as a DATE literal ("trailing input"), and
      // Thrift accepts the date-only form just as well. Without an explicit
      // DATE type the value still binds as a TIMESTAMP from the full ISO string.
      const isDateType = wireType === DBSQLParameterType.DATE;
      return new TSparkParameter({
        name,
        type: wireType ?? DBSQLParameterType.TIMESTAMP,
        value: new TSparkParameterValue({
          // For DATE, project the *calendar* date using local-time accessors
          // rather than `toISOString().slice(0, 10)`. `toISOString()` first
          // converts to UTC, so a `new Date(2024, 2, 14)` constructed in a
          // positive-offset zone (e.g. UTC+10, internal `2024-03-13T14:00Z`)
          // would yield "2024-03-13" — off by one. Users reason about a DATE
          // as the wall-calendar date they constructed, so extract that.
          stringValue: isDateType
            ? `${this.value.getFullYear()}-${String(this.value.getMonth() + 1).padStart(2, '0')}-${String(
                this.value.getDate(),
              ).padStart(2, '0')}`
            : this.value.toISOString(),
        }),
      });
    }

    return new TSparkParameter({
      name,
      type: wireType ?? DBSQLParameterType.STRING,
      value: new TSparkParameterValue({
        stringValue: this.value,
      }),
    });
  }
}
