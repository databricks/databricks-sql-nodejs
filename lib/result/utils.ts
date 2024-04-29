import Int64 from 'node-int64';
import {
  Schema,
  Field,
  DataType,
  Bool as ArrowBool,
  Int8 as ArrowInt8,
  Int16 as ArrowInt16,
  Int32 as ArrowInt32,
  Int64 as ArrowInt64,
  Float32 as ArrowFloat32,
  Float64 as ArrowFloat64,
  Utf8 as ArrowString,
  Date_ as ArrowDate,
  Binary as ArrowBinary,
  DateUnit,
  RecordBatchWriter,
} from 'apache-arrow';
import { TTableSchema, TColumnDesc, TPrimitiveTypeEntry, TTypeId, TColumn } from '../../thrift/TCLIService_types';
import HiveDriverError from '../errors/HiveDriverError';

export interface ArrowBatch {
  batches: Array<Buffer>;
  rowCount: number;
}

export function getSchemaColumns(schema?: TTableSchema): Array<TColumnDesc> {
  if (!schema) {
    return [];
  }

  return [...schema.columns].sort((c1, c2) => c1.position - c2.position);
}

function isString(value: unknown): value is string {
  return typeof value === 'string' || value instanceof String;
}

function convertJSON(value: any, defaultValue: any): any {
  if (!isString(value)) {
    return value;
  }

  try {
    return JSON.parse(value);
  } catch (e) {
    return defaultValue;
  }
}

function convertBigInt(value: any): any {
  if (typeof value === 'bigint') {
    return Number(value);
  }
  if (value instanceof Int64) {
    return value.toNumber();
  }
  return value;
}

export function convertThriftValue(typeDescriptor: TPrimitiveTypeEntry | undefined, value: any): any {
  if (!typeDescriptor) {
    return value;
  }

  switch (typeDescriptor.type) {
    case TTypeId.DATE_TYPE:
    case TTypeId.TIMESTAMP_TYPE:
      return value;
    case TTypeId.UNION_TYPE:
    case TTypeId.USER_DEFINED_TYPE:
      return String(value);
    case TTypeId.DECIMAL_TYPE:
      return Number(value);
    case TTypeId.STRUCT_TYPE:
    case TTypeId.MAP_TYPE:
      return convertJSON(value, {});
    case TTypeId.ARRAY_TYPE:
      return convertJSON(value, []);
    case TTypeId.BIGINT_TYPE:
      return convertBigInt(value);
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

// This type map corresponds to Arrow without native types support (most complex types are serialized as strings)
const hiveTypeToArrowType: Record<TTypeId, DataType | null> = {
  [TTypeId.BOOLEAN_TYPE]: new ArrowBool(),
  [TTypeId.TINYINT_TYPE]: new ArrowInt8(),
  [TTypeId.SMALLINT_TYPE]: new ArrowInt16(),
  [TTypeId.INT_TYPE]: new ArrowInt32(),
  [TTypeId.BIGINT_TYPE]: new ArrowInt64(),
  [TTypeId.FLOAT_TYPE]: new ArrowFloat32(),
  [TTypeId.DOUBLE_TYPE]: new ArrowFloat64(),
  [TTypeId.STRING_TYPE]: new ArrowString(),
  [TTypeId.TIMESTAMP_TYPE]: new ArrowString(),
  [TTypeId.BINARY_TYPE]: new ArrowBinary(),
  [TTypeId.ARRAY_TYPE]: new ArrowString(),
  [TTypeId.MAP_TYPE]: new ArrowString(),
  [TTypeId.STRUCT_TYPE]: new ArrowString(),
  [TTypeId.UNION_TYPE]: new ArrowString(),
  [TTypeId.USER_DEFINED_TYPE]: new ArrowString(),
  [TTypeId.DECIMAL_TYPE]: new ArrowString(),
  [TTypeId.NULL_TYPE]: null,
  [TTypeId.DATE_TYPE]: new ArrowDate(DateUnit.DAY),
  [TTypeId.VARCHAR_TYPE]: new ArrowString(),
  [TTypeId.CHAR_TYPE]: new ArrowString(),
  [TTypeId.INTERVAL_YEAR_MONTH_TYPE]: new ArrowString(),
  [TTypeId.INTERVAL_DAY_TIME_TYPE]: new ArrowString(),
};

export function hiveSchemaToArrowSchema(schema?: TTableSchema): Buffer | undefined {
  if (!schema) {
    return undefined;
  }

  const columns = getSchemaColumns(schema);

  const arrowFields = columns.map((column) => {
    const hiveType = column.typeDesc.types[0].primitiveEntry?.type ?? undefined;
    const arrowType = hiveType !== undefined ? hiveTypeToArrowType[hiveType] : undefined;
    if (!arrowType) {
      throw new HiveDriverError(`Unsupported column type: ${hiveType ? TTypeId[hiveType] : 'undefined'}`);
    }
    return new Field(column.columnName, arrowType, true);
  });

  const arrowSchema = new Schema(arrowFields);
  const writer = new RecordBatchWriter();
  writer.reset(undefined, arrowSchema);
  writer.finish();
  return Buffer.from(writer.toUint8Array(true));
}

export function getColumnValue(column?: TColumn) {
  if (!column) {
    return undefined;
  }
  return (
    column.binaryVal ??
    column.boolVal ??
    column.byteVal ??
    column.doubleVal ??
    column.i16Val ??
    column.i32Val ??
    column.i64Val ??
    column.stringVal
  );
}
