import Int64 from 'node-int64';
import { TSparkParameter, TSparkParameterValue } from '../thrift/TCLIService_types';

export type DBSQLParameterValue = undefined | null | boolean | number | bigint | Int64 | Date | string;

export enum DBSQLParameterType {
  VOID = 'VOID', // aka NULL
  STRING = 'STRING',
  DATE = 'DATE',
  TIMESTAMP = 'TIMESTAMP',
  // Timezone-explicit timestamp variants. A bare `Date` value defaults to
  // `TIMESTAMP`; set one of these explicitly to bind a TIMESTAMP_NTZ
  // (no timezone, wall-clock) or TIMESTAMP_LTZ (local timezone) parameter.
  // The Thrift wire only has `TIMESTAMP`; these are SEA-path types the kernel
  // param codec accepts — without them a migrating caller silently coerces
  // NTZ/LTZ columns to TIMESTAMP.
  TIMESTAMP_NTZ = 'TIMESTAMP_NTZ',
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

    if (typeof this.value === 'boolean') {
      return new TSparkParameter({
        name,
        type: this.type ?? DBSQLParameterType.BOOLEAN,
        value: new TSparkParameterValue({
          stringValue: this.value ? 'TRUE' : 'FALSE',
        }),
      });
    }

    if (typeof this.value === 'number') {
      return new TSparkParameter({
        name,
        type: this.type ?? (Number.isInteger(this.value) ? DBSQLParameterType.INTEGER : DBSQLParameterType.DOUBLE),
        value: new TSparkParameterValue({
          stringValue: Number(this.value).toString(),
        }),
      });
    }

    if (this.value instanceof Int64 || typeof this.value === 'bigint') {
      return new TSparkParameter({
        name,
        type: this.type ?? DBSQLParameterType.BIGINT,
        value: new TSparkParameterValue({
          stringValue: this.value.toString(),
        }),
      });
    }

    if (this.value instanceof Date) {
      return new TSparkParameter({
        name,
        type: this.type ?? DBSQLParameterType.TIMESTAMP,
        value: new TSparkParameterValue({
          stringValue: this.value.toISOString(),
        }),
      });
    }

    return new TSparkParameter({
      name,
      type: this.type ?? DBSQLParameterType.STRING,
      value: new TSparkParameterValue({
        stringValue: this.value,
      }),
    });
  }
}
