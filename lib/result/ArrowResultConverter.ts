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
import { TGetResultSetMetadataResp, TColumnDesc } from '../../thrift/TCLIService_types';
import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';
import { ArrowBatch, getSchemaColumns, convertThriftValue } from './utils';

const { isArrowBigNumSymbol, bigNumToBigInt } = arrowUtils;

type ArrowSchema = Schema<TypeMap>;
type ArrowSchemaField = Field<DataType<Type, TypeMap>>;

export default class ArrowResultConverter implements IResultsProvider<Array<any>> {
  protected readonly context: IClientContext;

  public readonly source: IResultsProvider<ArrowBatch>;

  private readonly schema: Array<TColumnDesc>;

  private recordBatchReader?: IterableIterator<RecordBatch<TypeMap>>;

  // Remaining rows in current Arrow batch (not the record batch!)
  private remainingRows: number = 0;

  // This is the next (!!) record batch to be read. It is unset only in two cases:
  // - prior to the first call to `fetchNext`
  // - when no more data available
  // This field is primarily used by a `hasMore`, so it can tell if next `fetchNext` will
  // actually return a non-empty result
  private prefetchedRecordBatch?: RecordBatch<TypeMap>;

  constructor(context: IClientContext, source: IResultsProvider<ArrowBatch>, { schema }: TGetResultSetMetadataResp) {
    this.context = context;
    this.source = source;
    this.schema = getSchemaColumns(schema);
  }

  public async hasMore() {
    if (this.schema.length === 0) {
      return false;
    }
    if (this.prefetchedRecordBatch) {
      return true;
    }
    return this.source.hasMore();
  }

  public async fetchNext(options: ResultsProviderFetchNextOptions) {
    if (this.schema.length === 0) {
      return [];
    }

    // It's not possible to know if iterator has more items until trying to get the next item.
    // So each time we read one batch ahead and store it, but process the batch prefetched on
    // a previous `fetchNext` call. Because we actually already have the next item - it's easy
    // to tell if the subsequent `fetchNext` will be able to read anything, and `hasMore` logic
    // becomes trivial

    // This prefetch handles a first call to `fetchNext`, when all the internal fields are not initialized yet.
    // On subsequent calls to `fetchNext` it will do nothing
    await this.prefetch(options);

    if (this.prefetchedRecordBatch) {
      // Consume a record batch fetched during previous call to `fetchNext`
      const table = new Table(this.prefetchedRecordBatch);
      this.prefetchedRecordBatch = undefined;
      // Get table rows, but not more than remaining count
      const arrowRows = table.toArray().slice(0, this.remainingRows);
      const result = this.getRows(table.schema, arrowRows);

      // Reduce remaining rows count by a count of rows we just processed.
      // If the remaining count reached zero - we're done with current arrow
      // batch, so discard the batch reader
      this.remainingRows -= result.length;
      if (this.remainingRows === 0) {
        this.recordBatchReader = undefined;
      }

      // Prefetch the next record batch
      await this.prefetch(options);

      return result;
    }

    return [];
  }

  // This method tries to read one more record batch and store it in `prefetchedRecordBatch` field.
  // If `prefetchedRecordBatch` is already non-empty - the method does nothing.
  // This method pulls the next item from source if needed, initializes a record batch reader and
  // gets the next item from it - until either reaches end of data or finds a non-empty record batch
  private async prefetch(options: ResultsProviderFetchNextOptions) {
    // This loop will be executed until a next non-empty record batch is retrieved
    // Another implicit loop condition (end of data) is checked in the loop body
    while (!this.prefetchedRecordBatch) {
      // First, try to fetch next item from source and initialize record batch reader.
      // If source has no more data - exit prematurely
      if (!this.recordBatchReader) {
        const sourceHasMore = await this.source.hasMore(); // eslint-disable-line no-await-in-loop
        if (!sourceHasMore) {
          return;
        }

        const arrowBatch = await this.source.fetchNext(options); // eslint-disable-line no-await-in-loop
        if (arrowBatch.batches.length > 0 && arrowBatch.rowCount > 0) {
          const reader = RecordBatchReader.from<TypeMap>(arrowBatch.batches);
          this.recordBatchReader = reader[Symbol.iterator]();
          this.remainingRows = arrowBatch.rowCount;
        }
      }

      // Try to get a next item from current record batch reader. The reader may be unavailable at this point -
      // in this case we fall back to a "done" state, and the `while` loop will do one more iteration attempting
      // to create a new reader. Eventually it will either succeed or reach end of source. This scenario also
      // handles readers which are already empty
      const item = this.recordBatchReader?.next() ?? { done: true, value: undefined };
      if (item.done || item.value === undefined) {
        this.recordBatchReader = undefined;
      } else {
        // Skip empty batches
        // eslint-disable-next-line no-lonely-if
        if (item.value.numRows > 0) {
          this.prefetchedRecordBatch = item.value;
        }
      }
    }
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
