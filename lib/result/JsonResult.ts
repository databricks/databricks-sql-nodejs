import { ColumnCode } from '../hive/Types';
import { TRowSet, TTableSchema, TColumn, TColumnDesc } from '../../thrift/TCLIService_types';
import IOperationResult from './IOperationResult';
import { getSchemaColumns, convertThriftValue } from './utils';

export default class JsonResult implements IOperationResult {
  private readonly schema: Array<TColumnDesc>;

  constructor(schema?: TTableSchema) {
    this.schema = getSchemaColumns(schema);
  }

  getValue(data?: Array<TRowSet>): Array<object> {
    if (this.schema.length === 0 || !data) {
      return [];
    }

    return data.reduce((result: Array<any>, rowSet: TRowSet) => {
      const columns = rowSet.columns || [];
      const rows = this.getRows(columns, this.schema);

      return result.concat(rows);
    }, []);
  }

  private getRows(columns: Array<TColumn>, descriptors: Array<TColumnDesc>): Array<any> {
    return descriptors.reduce(
      (rows, descriptor) =>
        this.getSchemaValues(descriptor, columns[descriptor.position - 1]).reduce((result, value, i) => {
          if (!result[i]) {
            result[i] = {};
          }

          const { columnName } = descriptor;

          result[i][columnName] = value;

          return result;
        }, rows),
      [],
    );
  }

  private getSchemaValues(descriptor: TColumnDesc, column?: TColumn): Array<any> {
    const typeDescriptor = descriptor.typeDesc.types[0]?.primitiveEntry;
    const columnValue = this.getColumnValue(column);

    if (!columnValue) {
      return [];
    }

    return columnValue.values.map((value: any, i: number) => {
      if (columnValue.nulls && this.isNull(columnValue.nulls, i)) {
        return null;
      }
      return convertThriftValue(typeDescriptor, value);
    });
  }

  private isNull(nulls: Buffer, i: number): boolean {
    const byte = nulls[Math.floor(i / 8)];
    const ofs = 2 ** (i % 8);

    return (byte & ofs) !== 0;
  }

  private getColumnValue(column?: TColumn) {
    if (!column) {
      return undefined;
    }
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
