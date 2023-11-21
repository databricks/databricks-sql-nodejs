import { Buffer } from 'buffer';
import {
  Table,
  Schema,
  Field,
  TypeMap,
  DataType,
  Type,
  StructRow,
  MapRow,
  Vector,
  RecordBatch,
  RecordBatchReader,
  util as arrowUtils,
} from 'apache-arrow';
import { TTableSchema, TColumnDesc } from '../../thrift/TCLIService_types';
import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';
import { getSchemaColumns, convertThriftValue } from './utils';

const { isArrowBigNumSymbol, bigNumToBigInt } = arrowUtils;

type ArrowSchema = Schema<TypeMap>;
type ArrowSchemaField = Field<DataType<Type, TypeMap>>;

export default class ArrowResultConverter implements IResultsProvider<Array<any>> {
  protected readonly context: IClientContext;

  private readonly source: IResultsProvider<Array<Buffer>>;

  private readonly schema: Array<TColumnDesc>;

  private reader?: IterableIterator<RecordBatch<TypeMap>>;

  private pendingRecordBatch?: RecordBatch<TypeMap>;

  constructor(context: IClientContext, source: IResultsProvider<Array<Buffer>>, schema?: TTableSchema) {
    this.context = context;
    this.source = source;
    this.schema = getSchemaColumns(schema);
  }

  public async hasMore() {
    if (this.pendingRecordBatch) {
      return true;
    }
    return this.source.hasMore();
  }

  public async fetchNext(options: ResultsProviderFetchNextOptions) {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      // It's not possible to know if iterator has more items until trying
      // to get the next item. But we need to know if iterator is empty right
      // after getting the next item. Therefore, after creating the iterator,
      // we get one item more and store it in `pendingRecordBatch`. Next time,
      // we use that stored item, and prefetch the next one. Prefetched item
      // is therefore the next item we are going to return, so it can be used
      // to know if we actually can return anything next time
      const recordBatch = this.pendingRecordBatch;
      this.pendingRecordBatch = this.prefetch();

      if (recordBatch) {
        const table = new Table(recordBatch);
        return this.getRows(table.schema, table.toArray());
      }

      // eslint-disable-next-line no-await-in-loop
      const batches = await this.source.fetchNext(options);
      if (batches.length === 0) {
        this.reader = undefined;
        break;
      }

      const reader = RecordBatchReader.from<TypeMap>(batches);
      this.reader = reader[Symbol.iterator]();
      this.pendingRecordBatch = this.prefetch();
    }

    return [];
  }

  private prefetch(): RecordBatch<TypeMap> | undefined {
    const item = this.reader?.next() ?? { done: true, value: undefined };

    if (item.done || item.value === undefined) {
      this.reader = undefined;
      return undefined;
    }

    return item.value;
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
      const result = bigNumToBigInt(value);
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
      const value = record[field];
      result[field] = value === null ? null : convertThriftValue(typeDescriptor, value);
    });

    return result;
  }
}
