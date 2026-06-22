import { Buffer } from 'buffer';
import {
  Table,
  Schema,
  Field,
  TypeMap,
  DataType,
  Type,
  Interval,
  IntervalUnit,
  StructRow,
  MapRow,
  Vector,
  RecordBatch,
  RecordBatchReader,
  util as arrowUtils,
} from 'apache-arrow';
import { TTableSchema, TColumnDesc, TTypeId } from '../../thrift/TCLIService_types';
import IClientContext from '../contracts/IClientContext';
import HiveDriverError from '../errors/HiveDriverError';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';
import { ArrowBatch, getSchemaColumns, convertThriftValue } from './utils';

const { isArrowBigNumSymbol, bigNumToBigInt } = arrowUtils;

type ArrowSchema = Schema<TypeMap>;
type ArrowSchemaField = Field<DataType<Type, TypeMap>>;

/**
 * Metadata key carrying the original Arrow `Duration` time unit on fields
 * rewritten to `Int64` by the kernel IPC pre-processor
 * (`lib/kernel/KernelArrowIpcDurationFix.ts`). Re-declared here (rather than
 * imported) to keep this generic `lib/result` converter free of a
 * compile-time dependency on `lib/kernel`.
 *
 * **kernel-gated by construction — NOT shared with Thrift.** This key (and the
 * `DataType.isInterval` / duration branches below) only ever execute on the
 * kernel path. The Thrift backend sets `intervalTypesAsArrow: false` and maps
 * both INTERVAL `TTypeId`s to `ArrowString` (`lib/result/utils.ts`), so the
 * server pre-formats intervals to strings and this logic is never reached.
 * `export`ed so `KernelIntervalParity.test` can pin it equal to the kernel-side
 * declaration and catch a rename/typo that would silently no-op the consumer.
 */
export const DURATION_UNIT_METADATA_KEY = 'databricks.arrow.duration_unit';
const ZERO_BIGINT = BigInt(0);
const NS_PER_MICRO = BigInt(1_000);
const NS_PER_MILLI = BigInt(1_000_000);
const NS_PER_SEC = BigInt(1_000_000_000);
const NS_PER_MIN = NS_PER_SEC * BigInt(60);
const NS_PER_HOUR = NS_PER_MIN * BigInt(60);
const NS_PER_DAY = NS_PER_HOUR * BigInt(24);

/**
 * Format a native Arrow `Interval[YearMonth]` value into the canonical thrift
 * string `"Y-M"` (e.g. 1 year 2 months → `"1-2"`, -1 month → `"-0-1"`).
 *
 * Arrow surfaces YEAR-MONTH as an `Int32Array(2)` `[years, months]` via the
 * `GetVisitor` (years/months derived from a single int32 of total months).
 *
 * **Only YEAR_MONTH reaches here.** The kernel emits INTERVAL DAY-TIME as an
 * Arrow `Duration` (rewritten to `Int64`), handled by
 * `formatDurationToIntervalDayTime` — never as a native `Interval[DayTime]`.
 * Any other unit (DAY_TIME / MONTH_DAY_NANO / undefined) is therefore
 * unexpected; we throw rather than silently misread the value as `[days, ms]`
 * and emit a confidently-wrong string (the old non-exhaustive default).
 */
function formatArrowInterval(value: Int32Array, valueType: Interval): string {
  if (valueType?.unit !== IntervalUnit.YEAR_MONTH) {
    throw new HiveDriverError(
      `kernel result converter: unsupported Arrow Interval unit ${valueType?.unit}. The kernel emits only ` +
        `YEAR_MONTH as a native Arrow Interval (DAY-TIME arrives as Duration); MONTH_DAY_NANO is unsupported.`,
    );
  }
  return formatYearMonth(Number(value[0]), Number(value[1]));
}

/**
 * Format the (years, months) decomposition into `"Y-M"` (or `"-Y-M"`
 * for negative intervals). Arrow's `getIntervalYearMonth` (in
 * `apache-arrow/visitor/get.js:179`) decomposes a signed total-months
 * int32 via integer truncation, so years and months always share the
 * same sign. We render the absolute values with a single leading `-`
 * to match the Spark display format used on the thrift path.
 */
function formatYearMonth(years: number, months: number): string {
  const total = years * 12 + months;
  if (total < 0) {
    const abs = -total;
    const y = Math.trunc(abs / 12);
    const m = abs % 12;
    return `-${y}-${m}`;
  }
  return `${years}-${months}`;
}

/**
 * Format an Arrow `Duration` value (rewritten by the kernel IPC
 * pre-processor to `Int64`) into the thrift INTERVAL DAY-TIME string.
 *
 * @param value     the duration value as `bigint` (signed nanos/micros/
 *                  millis/seconds depending on `unit`)
 * @param unit      one of `SECOND` / `MILLISECOND` / `MICROSECOND` /
 *                  `NANOSECOND` (the original Arrow time unit, captured
 *                  by `KernelArrowIpcDurationFix.ts`)
 */
function formatDurationToIntervalDayTime(value: bigint | number, unit: string): string {
  const bi = typeof value === 'bigint' ? value : BigInt(value);
  const nanos = toNanoseconds(bi, unit);
  return formatDayTimeFromTotal(nanos);
}

/**
 * Scale a duration value to nanoseconds based on its unit.
 *
 *   SECOND      → ×1_000_000_000
 *   MILLISECOND → ×    1_000_000
 *   MICROSECOND → ×        1_000
 *   NANOSECOND  → ×            1
 *
 * Throws on any other unit rather than silently treating it as
 * NANOSECOND. The four units above are exactly what
 * `KernelArrowIpcDurationFix` enumerates when it stamps the
 * `databricks.arrow.duration_unit` metadata, so an unrecognized unit
 * here means the two sides have drifted — fail loud (matching
 * `formatArrowInterval`'s stance) instead of emitting a confidently
 * wrong value.
 */
function toNanoseconds(value: bigint, unit: string): bigint {
  switch (unit) {
    case 'SECOND':
      return value * NS_PER_SEC;
    case 'MILLISECOND':
      return value * NS_PER_MILLI;
    case 'MICROSECOND':
      return value * NS_PER_MICRO;
    case 'NANOSECOND':
      return value;
    default:
      throw new HiveDriverError(
        `kernel INTERVAL DAY-TIME: unrecognized Arrow duration unit ${JSON.stringify(unit)}; ` +
          `expected one of SECOND / MILLISECOND / MICROSECOND / NANOSECOND`,
      );
  }
}

/**
 * Format a signed total-nanoseconds value as `"D HH:mm:ss.fffffffff"`.
 * Always emits 9 fractional digits to match the thrift driver's wire
 * format (`"1 02:03:04.000000000"` — 9 digits regardless of the
 * server-side storage precision). Negative values get a single
 * leading `-`. The caller has already scaled to nanoseconds.
 */
function formatDayTimeFromTotal(totalNanos: bigint): string {
  const sign = totalNanos < ZERO_BIGINT ? '-' : '';
  const abs = totalNanos < ZERO_BIGINT ? -totalNanos : totalNanos;

  const days = abs / NS_PER_DAY;
  let rem = abs % NS_PER_DAY;
  const hours = rem / NS_PER_HOUR;
  rem %= NS_PER_HOUR;
  const minutes = rem / NS_PER_MIN;
  rem %= NS_PER_MIN;
  const seconds = rem / NS_PER_SEC;
  const subSeconds = rem % NS_PER_SEC;

  const pad2 = (n: bigint): string => n.toString().padStart(2, '0');
  const fraction = `.${subSeconds.toString().padStart(9, '0')}`;

  return `${sign}${days.toString()} ${pad2(hours)}:${pad2(minutes)}:${pad2(seconds)}${fraction}`;
}

/**
 * Render an Arrow `Decimal` value — supplied as its unscaled integer (from
 * `bigNumToBigInt`) plus the column `scale` — as an exact decimal string,
 * e.g. unscaled `1234567890` / scale `5` → `"12345.67890"`. Used by the
 * precision-preserving path so high-precision DECIMALs survive the round-trip
 * instead of being flattened to an IEEE-754 double.
 */
export function bigNumDecimalToString(unscaled: bigint, scale: number): string {
  if (scale <= 0) {
    return unscaled.toString();
  }
  const negative = unscaled < ZERO_BIGINT;
  // `padStart(scale + 1)` guarantees at least one digit before the point
  // (e.g. unscaled `5` / scale `2` → `"005"` → `"0.05"`).
  const digits = (negative ? -unscaled : unscaled).toString().padStart(scale + 1, '0');
  const cut = digits.length - scale;
  return `${negative ? '-' : ''}${digits.slice(0, cut)}.${digits.slice(cut)}`;
}

export default class ArrowResultConverter implements IResultsProvider<Array<any>> {
  private readonly context: IClientContext;

  private readonly source: IResultsProvider<ArrowBatch>;

  private readonly schema: Array<TColumnDesc>;

  // When true, DECIMAL and 64-bit integer values keep full precision —
  // DECIMAL as an exact string and BIGINT as a JS `bigint` — instead of being
  // coerced to a lossy `number`. Enabled by the kernel backend, which always
  // receives native Arrow `Decimal128` / `Int64` from the kernel and has no
  // server-side "send as string" escape hatch (the Thrift backend gets the
  // string form via `useArrowNativeTypes=false`). Off by default so the Thrift
  // path keeps its long-standing `number` representation unchanged.
  private readonly preserveBigNumericPrecision: boolean;

  private recordBatchReader?: IterableIterator<RecordBatch<TypeMap>>;

  // Remaining rows in current Arrow batch (not the record batch!)
  private remainingRows: number = 0;

  // This is the next (!!) record batch to be read. It is unset only in two cases:
  // - prior to the first call to `fetchNext`
  // - when no more data available
  // This field is primarily used by a `hasMore`, so it can tell if next `fetchNext` will
  // actually return a non-empty result
  private prefetchedRecordBatch?: RecordBatch<TypeMap>;

  // Only the column `schema` is consumed here. Typed as the minimal shape
  // (not the full Thrift `TGetResultSetMetadataResp`) so both the Thrift
  // operation backend and the kernel backend's neutral `ResultMetadata` —
  // which both carry `schema?: TTableSchema` — can construct the converter
  // without an adapter at the call site.
  constructor(
    context: IClientContext,
    source: IResultsProvider<ArrowBatch>,
    { schema }: { schema?: TTableSchema },
    { preserveBigNumericPrecision = false }: { preserveBigNumericPrecision?: boolean } = {},
  ) {
    this.context = context;
    this.source = source;
    this.schema = getSchemaColumns(schema);
    this.preserveBigNumericPrecision = preserveBigNumericPrecision;
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
        if (arrowBatch.recordBatches !== undefined && arrowBatch.rowCount > 0) {
          // Kernel copycage path: batches are already decoded `RecordBatch`
          // objects (rebuilt from the kernel's in-cage Arrow buffers in
          // `KernelArrowImport`), so there are no IPC bytes to parse —
          // iterate them directly.
          this.recordBatchReader = arrowBatch.recordBatches[Symbol.iterator]();
          this.remainingRows = arrowBatch.rowCount;
        } else if (arrowBatch.batches.length > 0 && arrowBatch.rowCount > 0) {
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
      const record = this.convertArrowTypes(row, undefined, schema.fields, undefined);
      // Second, cast all the values to original Thrift types
      return this.convertThriftTypes(record);
    });
  }

  private convertArrowTypes(
    value: any,
    valueType: DataType | undefined,
    fields: Array<ArrowSchemaField> = [],
    field?: ArrowSchemaField,
  ): any {
    if (value === null) {
      return value;
    }

    const fieldsMap: Record<string, ArrowSchemaField> = {};
    for (const f of fields) {
      fieldsMap[f.name] = f;
    }

    // Convert structures to plain JS object and process all its fields recursively
    if (value instanceof StructRow) {
      const result = value.toJSON();
      for (const key of Object.keys(result)) {
        const childField: ArrowSchemaField | undefined = fieldsMap[key];
        result[key] = this.convertArrowTypes(
          result[key],
          childField?.type,
          childField?.type.children || [],
          childField,
        );
      }
      return result;
    }
    if (value instanceof MapRow) {
      const result = value.toJSON();
      // Map type consists of its key and value types. We need only value type here, key will be cast to string anyway
      const valueField = fieldsMap.entries?.type.children.find((item) => item.name === 'value');
      for (const key of Object.keys(result)) {
        result[key] = this.convertArrowTypes(
          result[key],
          valueField?.type,
          valueField?.type.children || [],
          valueField,
        );
      }
      return result;
    }

    // Convert lists to JS array and process items recursively
    if (value instanceof Vector) {
      const result = value.toJSON();
      // Array type contains the only child which defines a type of each array's element
      const elementField = fieldsMap.element;
      return result.map((item) =>
        this.convertArrowTypes(item, elementField?.type, elementField?.type.children || [], elementField),
      );
    }

    if (DataType.isTimestamp(valueType)) {
      return new Date(value);
    }

    // INTERVAL — Spark/Databricks kernel emits two flavours: native Arrow
    // `Interval[YearMonth]` / `Interval[DayTime]` (handled here) and
    // `Duration` (transparently rewritten to `Int64` upstream by
    // `KernelArrowIpcDurationFix.ts`; handled in the bigint/Int64 branch
    // below). In every case we coerce to the canonical thrift string
    // form so the kernel path is byte-identical with the thrift path:
    //   YEAR-MONTH → `"Y-M"`
    //   DAY-TIME   → `"D HH:mm:ss.fffffffff"`
    if (DataType.isInterval(valueType)) {
      return formatArrowInterval(value, valueType);
    }

    // Convert big number values to BigInt
    // Decimals are also represented as big numbers in Arrow, so additionally process them (convert to float)
    if (value instanceof Object && value[isArrowBigNumSymbol]) {
      const result = bigNumToBigInt(value);
      if (DataType.isDecimal(valueType)) {
        // Preserve full precision as an exact string when requested (kernel);
        // otherwise keep the historical lossy `number` form.
        if (this.preserveBigNumericPrecision) {
          return bigNumDecimalToString(result, valueType.scale);
        }
        return Number(result) / 10 ** valueType.scale;
      }
      // A rewritten Duration Int64 surfaces as a raw `bigint`, not a BigNum
      // wrapper, so it is handled in the bigint branch below — not here.
      return result;
    }

    // Convert binary data to Buffer.
    if (value instanceof Uint8Array) {
      // Note: Arrow `Int32Array` / `BigInt64Array` are NOT `instanceof
      // Uint8Array` (they are sibling TypedArrays), so an interval value never
      // reaches this branch — intervals are handled by the `isInterval` /
      // bigint branches above. This is purely the binary-column → Buffer path.
      return Buffer.from(value);
    }

    // Bigint fallback — for raw bigints (not BigNum wrappers), the
    // duration_unit metadata also gates the INTERVAL DAY-TIME format.
    if (typeof value === 'bigint') {
      const durationUnit = field?.metadata.get(DURATION_UNIT_METADATA_KEY);
      if (durationUnit) {
        return formatDurationToIntervalDayTime(value, durationUnit);
      }
      // Keep the exact `bigint` when precision must be preserved (kernel); the
      // default path narrows to `number` for backward compatibility (the
      // Thrift backend has always returned BIGINT as a JS `number`).
      if (this.preserveBigNumericPrecision) {
        return value;
      }
      return Number(value);
    }

    // Return other values as is
    return value;
  }

  private convertThriftTypes(record: Record<string, any>): any {
    const result: Record<string, any> = {};

    this.schema.forEach((column) => {
      const typeDescriptor = column.typeDesc.types[0]?.primitiveEntry;
      const field = column.columnName;
      const value = record[field];
      if (value === null) {
        result[field] = null;
        return;
      }
      // When preserving precision, DECIMAL and BIGINT values were already
      // produced in their exact form by `convertArrowTypes` (string / bigint).
      // `convertThriftValue` would narrow both back to a lossy `number`
      // (DECIMAL_TYPE → `Number(value)`, BIGINT_TYPE → `convertBigInt`), so
      // pass them through untouched on this path.
      if (
        this.preserveBigNumericPrecision &&
        (typeDescriptor?.type === TTypeId.DECIMAL_TYPE || typeDescriptor?.type === TTypeId.BIGINT_TYPE)
      ) {
        result[field] = value;
        return;
      }
      result[field] = convertThriftValue(typeDescriptor, value);
    });

    return result;
  }
}
