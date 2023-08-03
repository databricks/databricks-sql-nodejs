import { Buffer } from 'buffer';
import {
  tableFromIPC,
  Schema,
  Field,
  TypeMap,
  DataType,
  Type,
  StructRow,
  MapRow,
  Vector,
  util as arrowUtils,
} from 'apache-arrow';
import { TRowSet, TTableSchema, TColumnDesc } from '../../thrift/TCLIService_types';
import IOperationResult from './IOperationResult';
import { getSchemaColumns, convertThriftValue } from './utils';

const { isArrowBigNumSymbol, bignumToBigInt } = arrowUtils;

type ArrowSchema = Schema<TypeMap>;
type ArrowSchemaField = Field<DataType<Type, TypeMap>>;

export default class ArrowResult implements IOperationResult {
  private readonly schema: Array<TColumnDesc>;

  private readonly arrowSchema?: Buffer;

  constructor(schema?: TTableSchema, arrowSchema?: Buffer) {
    this.schema = getSchemaColumns(schema);
    this.arrowSchema = arrowSchema;
  }

  async hasPendingData() {
    return false;
  }

  async getValue(data?: Array<TRowSet>) {
    if (this.schema.length === 0 || !this.arrowSchema || !data) {
      return [];
    }

    const batches = await this.getBatches(data);
    if (batches.length === 0) {
      return [];
    }

    const table = tableFromIPC<TypeMap>([this.arrowSchema, ...batches]);
    return this.getRows(table.schema, table.toArray());
  }

  protected async getBatches(data: Array<TRowSet>): Promise<Array<Buffer>> {
    const result: Array<Buffer> = [];

    data.forEach((rowSet) => {
      rowSet.arrowBatches?.forEach((arrowBatch) => {
        if (arrowBatch.batch) {
          result.push(arrowBatch.batch);
        }
      });
    });

    return result;
  }

  private getRows(schema: ArrowSchema, rows: Array<StructRow | MapRow>): Array<any> {
    return rows.map((row) => {
      // First, convert native Arrow values to corresponding plain JS objects
      const record = this.convertArrowTypes(row, undefined, schema.fields);
      // Second, cast all the values to original Thrift types
      return this.convertThriftTypes(record);
    });
  }

  private convertArrowTypes(value: any, valueType: DataType | undefined, fields: Array<ArrowSchemaField> = []): any {
    if (value === null) {
      return value;
    }

    const fieldsMap: Record<string, ArrowSchemaField> = {};
    for (const field of fields) {
      fieldsMap[field.name] = field;
    }

    // Convert structures to plain JS object and process all its fields recursively
    if (value instanceof StructRow) {
      const result = value.toJSON();
      for (const key of Object.keys(result)) {
        const field: ArrowSchemaField | undefined = fieldsMap[key];
        result[key] = this.convertArrowTypes(result[key], field?.type, field?.type.children || []);
      }
      return result;
    }
    if (value instanceof MapRow) {
      const result = value.toJSON();
      // Map type consists of its key and value types. We need only value type here, key will be cast to string anyway
      const field = fieldsMap.entries?.type.children.find((item) => item.name === 'value');
      for (const key of Object.keys(result)) {
        result[key] = this.convertArrowTypes(result[key], field?.type, field?.type.children || []);
      }
      return result;
    }

    // Convert lists to JS array and process items recursively
    if (value instanceof Vector) {
      const result = value.toJSON();
      // Array type contains the only child which defines a type of each array's element
      const field = fieldsMap.element;
      return result.map((item) => this.convertArrowTypes(item, field?.type, field?.type.children || []));
    }

    if (DataType.isTimestamp(valueType)) {
      return new Date(value);
    }

    // Convert big number values to BigInt
    // Decimals are also represented as big numbers in Arrow, so additionally process them (convert to float)
    if (value instanceof Object && value[isArrowBigNumSymbol]) {
      const result = bignumToBigInt(value);
      if (DataType.isDecimal(valueType)) {
        return Number(result) / 10 ** valueType.scale;
      }
      return result;
    }

    // Convert binary data to Buffer
    if (value instanceof Uint8Array) {
      return Buffer.from(value);
    }

    // Return other values as is
    return typeof value === 'bigint' ? Number(value) : value;
  }

  private convertThriftTypes(record: Record<string, any>): any {
    const result: Record<string, any> = {};

    this.schema.forEach((column) => {
      const typeDescriptor = column.typeDesc.types[0]?.primitiveEntry;
      const field = column.columnName;
      result[field] = convertThriftValue(typeDescriptor, record[field]);
    });

    return result;
  }
}
