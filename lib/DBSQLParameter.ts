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
        type: wireType ?? (Number.isInteger(this.value) ? DBSQLParameterType.INTEGER : DBSQLParameterType.DOUBLE),
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
      return new TSparkParameter({
        name,
        type: wireType ?? DBSQLParameterType.TIMESTAMP,
        value: new TSparkParameterValue({
          stringValue: this.value.toISOString(),
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
