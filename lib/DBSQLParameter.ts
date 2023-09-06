import Int64 from 'node-int64';
import { TSparkParameter, TSparkParameterValue } from '../thrift/TCLIService_types';

export type DBSQLParameterValue = boolean | number | bigint | Int64 | string;

interface DBSQLParameterOptions {
  type?: string;
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
        type: this.type ?? 'BOOLEAN',
        value: new TSparkParameterValue({
          stringValue: this.value ? 'TRUE' : 'FALSE',
        }),
      });
    }

    if (typeof this.value === 'number') {
      return new TSparkParameter({
        type: this.type ?? (Number.isInteger(this.value) ? 'INTEGER' : 'DOUBLE'),
        value: new TSparkParameterValue({
          stringValue: Number(this.value).toString(),
        }),
      });
    }

    if (this.value instanceof Int64 || typeof this.value === 'bigint') {
      return new TSparkParameter({
        type: this.type ?? 'BIGINT',
        value: new TSparkParameterValue({
          stringValue: this.value.toString(),
        }),
      });
    }

    return new TSparkParameter({
      type: this.type ?? 'STRING',
      value: new TSparkParameterValue({
        stringValue: this.value,
      }),
    });
  }
}
