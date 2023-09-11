import Int64 from 'node-int64';
import { TSparkParameter, TSparkParameterValue } from '../thrift/TCLIService_types';

export type DBSQLParameterValue = boolean | number | bigint | Int64 | Date | string;

enum DBSQLParameterType {
  STRING = "STRING",
  DATE = "DATE",
  TIMESTAMP = "TIMESTAMP",
  FLOAT = "FLOAT",
  DECIMAL = "DECIMAL",
  DOUBLE = "DOUBLE",
  INTEGER = "INTEGER",
  BIGINT = "BIGINT",
  SMALLINT = "SMALLINT",
  TINYINT = "TINYINT",
  BOOLEAN = "BOOLEAN",
  INTERVALMONTH = "INTERVAL MONTH",
  INTERVALDAY = "INTERVAL DAY"
}

interface DBSQLParameterOptions {
  type?: DBSQLParameterType;
  value: DBSQLParameterValue;
}

export default class DBSQLParameter {
  public readonly type?: string;

  public readonly value: DBSQLParameterValue;

  constructor({ type, value }: DBSQLParameterOptions) {
    this.type = type;
    this.value = value;
  }

  public toSparkParameter(): TSparkParameter {
    if (typeof this.value === 'boolean') {
      return new TSparkParameter({
        type: this.type ?? DBSQLParameterType.BOOLEAN,
        value: new TSparkParameterValue({
          stringValue: this.value ? 'TRUE' : 'FALSE',
        }),
      });
    }

    if (typeof this.value === 'number') {
      return new TSparkParameter({
        type: this.type ?? (Number.isInteger(this.value) ? DBSQLParameterType.INTEGER : DBSQLParameterType.DOUBLE),
        value: new TSparkParameterValue({
          stringValue: Number(this.value).toString(),
        }),
      });
    }

    if (this.value instanceof Int64 || typeof this.value === 'bigint') {
      return new TSparkParameter({
        type: this.type ?? DBSQLParameterType.BIGINT,
        value: new TSparkParameterValue({
          stringValue: this.value.toString(),
        }),
      });
    }

    if (this.value instanceof Date) {
      return new TSparkParameter({
        type: this.type ?? DBSQLParameterType.TIMESTAMP,
        value: new TSparkParameterValue({
          stringValue: this.value.toISOString(),
        }),
      });
    }

    return new TSparkParameter({
      type: this.type ?? DBSQLParameterType.STRING,
      value: new TSparkParameterValue({
        stringValue: this.value,
      }),
    });
  }
}
