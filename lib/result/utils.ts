import { TTableSchema, TColumnDesc, TPrimitiveTypeEntry, TTypeId } from '../../thrift/TCLIService_types';

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
    return value;
  }
  // TODO: `Int64.toNumber()` returns a regular JS number value; should we return BigInt instead?
  return value.toNumber();
}

export function convertThriftValue(typeDescriptor: TPrimitiveTypeEntry | undefined, value: any): any {
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
