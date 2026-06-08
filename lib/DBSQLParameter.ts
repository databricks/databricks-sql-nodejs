import Int64 from 'node-int64';
import { TSparkParameter, TSparkParameterValue } from '../thrift/TCLIService_types';

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

  public toSparkParameter({ name }: ToSparkParameterOptions = {}): TSparkParameter {
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
          stringValue: this.value.toString(),
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
          stringValue: isDateType ? this.value.toISOString().slice(0, 10) : this.value.toISOString(),
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
