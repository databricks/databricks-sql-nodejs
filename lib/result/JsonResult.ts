import { ColumnCode, ColumnType } from '../hive/Types';
import {
  TTypeId,
  TRowSet,
  TTableSchema,
  TColumn,
  TColumnDesc,
  TPrimitiveTypeEntry,
} from '../../thrift/TCLIService_types';
import IOperationResult from './IOperationResult';
import IOperation from '../contracts/IOperation';

export default class JsonResult implements IOperationResult {
  private schema: TTableSchema | null;
  private data: Array<TRowSet> | null;

  constructor() {
    this.schema = null;
    this.data = null;
  }

  setOperation(operation: IOperation): void {
    this.schema = operation.getSchema();
    this.data = operation.getData();
  }

  getValue(): Array<object> {
    if (!this.data) {
      return [];
    }

    const descriptors = this.getSchemaColumns();

    return this.data.reduce((result: Array<any>, rowSet: TRowSet) => {
      const columns = rowSet.columns || [];
      const rows = this.getRows(columns, descriptors);

      return result.concat(rows);
    }, []);
  }

  private getSchemaColumns(): Array<TColumnDesc> {
    if (!this.schema) {
      return [];
    }

    return [...this.schema.columns].sort((c1, c2) => c1.position - c2.position);
  }

  private getRows(columns: Array<TColumn>, descriptors: Array<TColumnDesc>): Array<any> {
    return descriptors.reduce((rows, descriptor) => {
      return this.getSchemaValues(descriptor, columns[descriptor.position - 1]).reduce((result, value, i) => {
        if (!result[i]) {
          result[i] = {};
        }

        const name = this.getColumnName(descriptor);

        result[i][name] = value;

        return result;
      }, rows);
    }, []);
  }

  private getSchemaValues(descriptor: TColumnDesc, column: TColumn): Array<any> {
    const typeDescriptor = descriptor.typeDesc.types[0]?.primitiveEntry;
    const columnValue = this.getColumnValue(column);

    if (!columnValue) {
      return [];
    }

    return columnValue.values.map((value: any, i: number) => {
      if (columnValue.nulls && this.isNull(columnValue.nulls, i)) {
        return null;
      } else {
        return this.convertData(typeDescriptor, value);
      }
    });
  }

  private getColumnName(column: TColumnDesc): string {
    const name = column.columnName || '';

    return name.split('.').pop() || '';
  }

  private convertData(typeDescriptor: TPrimitiveTypeEntry | undefined, value: ColumnType): any {
    if (!typeDescriptor) {
      return value;
    }

    switch (typeDescriptor.type) {
      case TTypeId.TIMESTAMP_TYPE:
      case TTypeId.DATE_TYPE:
      case TTypeId.UNION_TYPE:
      case TTypeId.USER_DEFINED_TYPE:
        return String(value);
      case TTypeId.DECIMAL_TYPE:
        return Number(value);
      case TTypeId.STRUCT_TYPE:
      case TTypeId.MAP_TYPE:
        return this.toJSON(value, {});
      case TTypeId.ARRAY_TYPE:
        return this.toJSON(value, []);
      case TTypeId.BIGINT_TYPE:
        return this.convertBigInt(value);
      case TTypeId.NULL_TYPE:
      case TTypeId.BINARY_TYPE:
      case TTypeId.INTERVAL_YEAR_MONTH_TYPE:
      case TTypeId.INTERVAL_DAY_TIME_TYPE:
      case TTypeId.FLOAT_TYPE:
      case TTypeId.DOUBLE_TYPE:
      case TTypeId.INT_TYPE:
      case TTypeId.SMALLINT_TYPE:
      case TTypeId.TINYINT_TYPE:
      case TTypeId.BOOLEAN_TYPE:
      case TTypeId.STRING_TYPE:
      case TTypeId.CHAR_TYPE:
      case TTypeId.VARCHAR_TYPE:
      default:
        return value;
    }
  }

  private isNull(nulls: Buffer, i: number): boolean {
    const byte = nulls[Math.floor(i / 8)];
    const ofs = Math.pow(2, i % 8);

    return (byte & ofs) !== 0;
  }

  private toJSON(value: any, defaultValue: any): any {
    try {
      return JSON.parse(value);
    } catch (e) {
      return defaultValue;
    }
  }

  private convertBigInt(value: any): any {
    return value.toNumber();
  }

  private getColumnValue(column: TColumn) {
    return (
      column[ColumnCode.binaryVal] ||
      column[ColumnCode.boolVal] ||
      column[ColumnCode.byteVal] ||
      column[ColumnCode.doubleVal] ||
      column[ColumnCode.i16Val] ||
      column[ColumnCode.i32Val] ||
      column[ColumnCode.i64Val] ||
      column[ColumnCode.stringVal]
    );
  }
}
