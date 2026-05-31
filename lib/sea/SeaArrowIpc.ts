// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { RecordBatchReader, Schema, Field, DataType, TypeMap } from 'apache-arrow';
import { TTableSchema, TTypeId, TPrimitiveTypeEntry } from '../../thrift/TCLIService_types';
import { rewriteDurationToInt64, DURATION_UNIT_METADATA_KEY } from './SeaArrowIpcDurationFix';

/**
 * Field metadata key used by the kernel to attach the original Databricks
 * SQL type name to each Arrow field. See `databricks-sql-kernel/src/reader/mod.rs`.
 */
const DATABRICKS_TYPE_NAME = 'databricks.type_name';

/**
 * Decode an Arrow IPC stream payload (schema header + zero-or-more
 * record-batch messages) into its row count.
 *
 * Returns `{ schema, rowCount }`. The schema is left intact as the
 * apache-arrow Schema object so callers can reuse it; the rowCount is
 * the sum of `RecordBatch.numRows` across every record-batch message
 * in the stream.
 *
 * Why we parse upfront: `ArrowResultConverter` consumes `ArrowBatch`
 * objects which carry an explicit `rowCount`. The kernel's IPC payload
 * does not carry a separate count — only per-RecordBatch numRows. We
 * walk the messages once to sum them so the converter sees the same
 * shape as the thrift path (`ArrowResultHandler.fetchNext` at
 * `lib/result/ArrowResultHandler.ts:55`).
 *
 * Re-parsing inside the converter is unavoidable because `RecordBatch`
 * instances created here cannot be passed across the converter's
 * `Buffer[]` boundary without rewriting the converter. Callers that already
 * patched the IPC bytes can set `alreadyPatched` to avoid running the
 * FlatBuffer rewrite twice on the hot fetch path.
 */
export function decodeIpcBatch(
  ipcBytes: Buffer,
  options: { alreadyPatched?: boolean } = {},
): { schema: Schema<TypeMap>; rowCount: number } {
  const patched = options.alreadyPatched ? ipcBytes : rewriteDurationToInt64(ipcBytes);
  const reader = RecordBatchReader.from<TypeMap>(patched);
  // Eagerly open so `schema` is populated.
  reader.open();
  const { schema } = reader;

  let rowCount = 0;
  // Iterate all record batches in the stream and sum row counts.
  for (const batch of reader) {
    rowCount += batch.numRows;
  }
  return { schema, rowCount };
}

/**
 * Decode an Arrow IPC schema payload (no record batches) into the
 * apache-arrow Schema object.
 */
export function decodeIpcSchema(ipcBytes: Buffer): Schema<TypeMap> {
  const patched = rewriteDurationToInt64(ipcBytes);
  const reader = RecordBatchReader.from<TypeMap>(patched);
  reader.open();
  return reader.schema;
}

/**
 * Pre-process raw IPC bytes from the kernel so they're consumable by
 * `apache-arrow@13`. The current transformation is `Duration → Int64`
 * with the original duration unit preserved in field metadata (see
 * `SeaArrowIpcDurationFix.ts`). Returned bytes are byte-identical to
 * the input when no transformation is needed.
 *
 * Exposed so callers can pre-patch the buffer **once** and pass the
 * result through both `decodeIpcBatch` (for row-count extraction in
 * `SeaResultsProvider`) and `ArrowResultConverter.fetchNext` (which
 * re-decodes the same bytes via `RecordBatchReader.from`). Without
 * this, the converter would re-throw on `Duration` because it never
 * sees the patched bytes.
 */
export function patchIpcBytes(ipcBytes: Buffer): Buffer {
  return rewriteDurationToInt64(ipcBytes);
}

/**
 * Map an Arrow `DataType` (with optional `databricks.type_name`
 * metadata) onto the closest Thrift `TTypeId`.
 *
 * This is the synthesis step that lets the existing
 * `ArrowResultConverter` Phase-2 dispatch (`convertThriftValue` in
 * `lib/result/utils.ts:61-98`) keep working unchanged for the SEA
 * path. Phase-2 keys exclusively off `TPrimitiveTypeEntry.type` per
 * column, so we synthesize a `TColumnDesc` whose `TTypeId` matches the
 * server-emitted Arrow type as closely as possible.
 *
 * Resolution order:
 *   1. The kernel attaches `databricks.type_name` (e.g. "DECIMAL",
 *      "INTERVAL", "STRUCT") to each field's metadata. Prefer that when
 *      present — it carries the original SQL semantic that the Arrow
 *      type alone can lose (e.g. INTERVAL → Utf8 with metadata).
 *   2. Fall back to the Arrow `DataType.typeId` for primitive types.
 *
 * This matches the JDBC and Python drivers' policy of trusting the
 * server's logical type assignment over the wire-level Arrow encoding.
 */
function arrowTypeToTTypeId(field: Field<DataType>): TTypeId {
  const typeName = field.metadata.get(DATABRICKS_TYPE_NAME)?.toUpperCase();

  // `intervals_as_string` (set by the SEA backend for Thrift parity)
  // renders INTERVAL columns as physical Arrow `Utf8` while the kernel
  // keeps the `INTERVAL …` type_name metadata. The Thrift driver reports
  // such string-rendered intervals as STRING (type 7), so honour the
  // physical type here rather than the semantic metadata — otherwise the
  // SEA path would report INTERVAL (20/21) and diverge from Thrift on a
  // column whose values are already identical strings. Native interval
  // encodings (the kernel default) are Duration / MonthInterval, never
  // Utf8, so this guard is inert unless `intervals_as_string` is on.
  if (typeName?.startsWith('INTERVAL') && DataType.isUtf8(field.type)) {
    return TTypeId.STRING_TYPE;
  }

  switch (typeName) {
    case 'BOOLEAN':
      return TTypeId.BOOLEAN_TYPE;
    case 'TINYINT':
    case 'BYTE':
      return TTypeId.TINYINT_TYPE;
    case 'SMALLINT':
    case 'SHORT':
      return TTypeId.SMALLINT_TYPE;
    case 'INT':
    case 'INTEGER':
      return TTypeId.INT_TYPE;
    case 'BIGINT':
    case 'LONG':
      return TTypeId.BIGINT_TYPE;
    case 'FLOAT':
    case 'REAL':
      return TTypeId.FLOAT_TYPE;
    case 'DOUBLE':
      return TTypeId.DOUBLE_TYPE;
    case 'STRING':
      return TTypeId.STRING_TYPE;
    case 'VARCHAR':
      return TTypeId.VARCHAR_TYPE;
    case 'CHAR':
      return TTypeId.CHAR_TYPE;
    case 'BINARY':
      return TTypeId.BINARY_TYPE;
    case 'DATE':
      return TTypeId.DATE_TYPE;
    case 'TIMESTAMP':
    case 'TIMESTAMP_NTZ':
      return TTypeId.TIMESTAMP_TYPE;
    case 'DECIMAL':
      return TTypeId.DECIMAL_TYPE;
    case 'INTERVAL':
    case 'INTERVAL DAY':
    case 'INTERVAL DAY TO HOUR':
    case 'INTERVAL DAY TO MINUTE':
    case 'INTERVAL DAY TO SECOND':
    case 'INTERVAL HOUR':
    case 'INTERVAL HOUR TO MINUTE':
    case 'INTERVAL HOUR TO SECOND':
    case 'INTERVAL MINUTE':
    case 'INTERVAL MINUTE TO SECOND':
    case 'INTERVAL SECOND':
      return TTypeId.INTERVAL_DAY_TIME_TYPE;
    case 'INTERVAL YEAR':
    case 'INTERVAL YEAR TO MONTH':
    case 'INTERVAL MONTH':
      return TTypeId.INTERVAL_YEAR_MONTH_TYPE;
    case 'ARRAY':
      return TTypeId.ARRAY_TYPE;
    case 'MAP':
      return TTypeId.MAP_TYPE;
    case 'STRUCT':
      return TTypeId.STRUCT_TYPE;
    case 'NULL':
    case 'VOID':
      return TTypeId.NULL_TYPE;
    default:
      break;
  }

  // Fall back to Arrow's own type id when no databricks metadata is set
  // (e.g. unit tests constructing batches without metadata).
  const arrowType = field.type;
  if (DataType.isBool(arrowType)) return TTypeId.BOOLEAN_TYPE;
  if (DataType.isInt(arrowType)) {
    // Duration columns are rewritten to Int64 with a
    // `databricks.arrow.duration_unit` metadata marker (see
    // `SeaArrowIpcDurationFix.ts`). Surface them as INTERVAL_DAY_TIME
    // so the converter formats them back into the thrift string form.
    if (arrowType.bitWidth === 64 && field.metadata.has(DURATION_UNIT_METADATA_KEY)) {
      return TTypeId.INTERVAL_DAY_TIME_TYPE;
    }
    switch (arrowType.bitWidth) {
      case 8:
        return TTypeId.TINYINT_TYPE;
      case 16:
        return TTypeId.SMALLINT_TYPE;
      case 32:
        return TTypeId.INT_TYPE;
      case 64:
        return TTypeId.BIGINT_TYPE;
      default:
        return TTypeId.BIGINT_TYPE;
    }
  }
  if (DataType.isFloat(arrowType)) {
    // arrow Float precision: 16=HALF, 32=SINGLE, 64=DOUBLE
    return arrowType.precision === 2 ? TTypeId.DOUBLE_TYPE : TTypeId.FLOAT_TYPE;
  }
  if (DataType.isDecimal(arrowType)) return TTypeId.DECIMAL_TYPE;
  if (DataType.isUtf8(arrowType)) return TTypeId.STRING_TYPE;
  if (DataType.isBinary(arrowType)) return TTypeId.BINARY_TYPE;
  if (DataType.isDate(arrowType)) return TTypeId.DATE_TYPE;
  if (DataType.isTimestamp(arrowType)) return TTypeId.TIMESTAMP_TYPE;
  // Native Arrow Interval types. The server-side INTERVAL YEAR-MONTH
  // (and the legacy IntervalDayTime variant) come through with type
  // id 11 / -25 / -26 — apache-arrow@13 surfaces them as `Int32Array`
  // pairs which the converter formats to thrift's `"Y-M"` / day-time
  // strings.
  if (DataType.isInterval(arrowType)) {
    // unit 0 = YEAR_MONTH, unit 1 = DAY_TIME, unit 2 = MONTH_DAY_NANO
    return arrowType.unit === 0 ? TTypeId.INTERVAL_YEAR_MONTH_TYPE : TTypeId.INTERVAL_DAY_TIME_TYPE;
  }
  if (DataType.isList(arrowType)) return TTypeId.ARRAY_TYPE;
  if (DataType.isMap(arrowType)) return TTypeId.MAP_TYPE;
  if (DataType.isStruct(arrowType)) return TTypeId.STRUCT_TYPE;
  if (DataType.isNull(arrowType)) return TTypeId.NULL_TYPE;

  return TTypeId.STRING_TYPE;
}

/**
 * Synthesize a Thrift `TTableSchema` from an Arrow schema decoded out
 * of the kernel's IPC stream. Used by `SeaOperationBackend.getResultMetadata`
 * to drive `ArrowResultConverter.convertThriftTypes` (Phase 2) without
 * changing that code.
 */
export function arrowSchemaToThriftSchema(arrowSchema: Schema<TypeMap>): TTableSchema {
  const columns = arrowSchema.fields.map((field, index) => {
    const primitiveEntry: TPrimitiveTypeEntry = {
      type: arrowTypeToTTypeId(field),
    };
    return {
      columnName: field.name,
      typeDesc: {
        types: [
          {
            primitiveEntry,
          },
        ],
      },
      position: index + 1,
    };
  });
  return { columns };
}
