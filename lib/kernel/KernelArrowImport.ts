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

/**
 * Copy-into-cage Arrow import: reconstruct an `apache-arrow` `RecordBatch`
 * from the buffer tree the kernel hands across the napi boundary
 * (`databricks-sql-kernel/napi/src/zerocopy.rs`), WITHOUT re-decoding an
 * Arrow IPC stream.
 *
 * The kernel ships each `RecordBatch` column as a node tree of in-cage
 * (V8-allocated) `ArrayBuffer`s — one `memcpy` per Arrow buffer. We rebuild
 * `arrow.Data` for each node via `makeData`, driven by the result schema's
 * per-field `DataType` (decoded once, elsewhere). Because the reconstructed
 * batch goes through the exact same `apache-arrow` `Data`/`Vector` machinery
 * that `RecordBatchReader` produces on the IPC path, the downstream
 * `ArrowResultConverter` is byte-for-byte unaffected — only the *route* the
 * batch took to become a `RecordBatch` changed.
 *
 * Buffer→slot mapping mirrors Arrow's columnar layout: variable-binary
 * (`Utf8`/`Binary`) carries `[offsets, values]`; list/map carry
 * `[offsets]` + a child; struct/fixed-size-list carry only children;
 * everything else (bool, ints, floats, decimal, date/time/timestamp,
 * duration, fixed-size-binary) is a single value buffer.
 *
 * The historical name `importZeroCopyBatch` is retained because the kernel's
 * copycage descriptor is byte-for-byte identical to the borrowed (zero-copy)
 * descriptor — the importer is agnostic to whether the backing `ArrayBuffer`s
 * were borrowed or cage-allocated.
 */

import { makeData, Data, RecordBatch, Struct, Schema, DataType, TypeMap } from 'apache-arrow';
import HiveDriverError from '../errors/HiveDriverError';

/** One `ArrayData` node as exported by `zerocopy.rs::build_node`. */
export interface KernelArrayNode {
  length: number;
  offset: number;
  nullCount: number;
  /** Bit-packed validity buffer; absent when the node has no null buffer. */
  validity?: ArrayBuffer;
  /** The type's data buffers, in canonical Arrow order. */
  buffers: ArrayBuffer[];
  /** Child nodes (struct fields, list/map values). */
  children: KernelArrayNode[];
}

/** A whole batch: one `KernelArrayNode` per top-level column. */
export interface KernelZeroCopyBatch {
  numRows: number;
  columns: KernelArrayNode[];
}

/**
 * Can this Arrow type be reconstructed by the buffer-handoff importer?
 *
 * This is an *allowlist*: only the layouts `buildData` knows how to wire are
 * accepted, so any type the importer doesn't positively recognise (today
 * Dictionary and Union — whose dictionary side-vector / union type-id +
 * offset side-buffers the single `{ buffers, children }` tree cannot carry —
 * and any future / 64-bit-offset "Large" variant the pinned apache-arrow
 * version may add) defaults to *unsupported* and routes the whole result
 * through IPC. The kernel does not emit Dictionary/Union/Large types for
 * Databricks result sets today; the allowlist guarantees that if one ever
 * slips through it degrades to a correct (IPC) decode rather than a silent
 * mis-decode. Recurses into struct / list / map / fixed-size-list children.
 */
export function isZeroCopySupported(type: DataType): boolean {
  // Composite types: supported iff every child is supported.
  if (
    DataType.isStruct(type) ||
    DataType.isList(type) ||
    DataType.isMap(type) ||
    DataType.isFixedSizeList(type)
  ) {
    return type.children.every((f) => isZeroCopySupported(f.type));
  }
  // Leaf types `buildData` handles via a single value buffer (or
  // offsets+values for variable-binary). Anything not on this list — notably
  // Dictionary, Union/DenseUnion/SparseUnion, and any 64-bit-offset Large
  // variant — is treated as unsupported and forced onto the IPC path.
  return (
    DataType.isNull(type) ||
    DataType.isBool(type) ||
    DataType.isInt(type) ||
    DataType.isFloat(type) ||
    DataType.isDecimal(type) ||
    DataType.isDate(type) ||
    DataType.isTime(type) ||
    DataType.isTimestamp(type) ||
    DataType.isInterval(type) ||
    DataType.isUtf8(type) ||
    DataType.isBinary(type) ||
    DataType.isFixedSizeBinary(type)
  );
}

/**
 * Does every field of `schema` reconstruct under the buffer-handoff path?
 * The provider gates the whole result on this — a single unsupported column
 * forces IPC for the entire fetch.
 */
export function isSchemaZeroCopySupported(schema: Schema<TypeMap>): boolean {
  return schema.fields.every((f) => isZeroCopySupported(f.type));
}

/**
 * Build `arrow.Data` for one node against its `DataType`.
 *
 * `makeData` coerces each raw `ArrayBuffer` to the type's backing
 * TypedArray (e.g. `Float64Array`, `Int32Array`) by reinterpreting the
 * same bytes — no copy — so this stays type-agnostic for the common
 * fixed-width case and only special-cases the structural layouts.
 */
function buildData(type: DataType, node: KernelArrayNode): Data {
  // The kernel compacts every batch to offset 0 before the handoff
  // (`zerocopy::compact_to_offset_zero`), because `makeData` always builds at
  // offset 0 — a non-zero offset here would mean the kernel contract was
  // violated, so assert defensively rather than silently read wrong rows.
  if (node.offset !== 0) {
    throw new HiveDriverError(
      `kernel buffer-handoff import: unexpected non-zero array offset ${node.offset} — ` +
        `the kernel must compact to offset 0 (zerocopy::compact_to_offset_zero) before handoff`,
    );
  }

  const common = {
    length: node.length,
    nullCount: node.nullCount,
    nullBitmap: node.validity ? new Uint8Array(node.validity) : undefined,
  };

  // Defence-in-depth: the schema-level `isSchemaZeroCopySupported` gate should
  // already have routed any dictionary/union result through IPC, so reaching
  // here means a contract violation — reject loudly rather than mis-decode.
  if (DataType.isDictionary(type) || DataType.isUnion(type)) {
    throw new HiveDriverError(
      `kernel buffer-handoff import: unsupported Arrow type ${type} (dictionary/union) — ` +
        `should have fallen back to IPC`,
    );
  }

  if (DataType.isNull(type)) {
    return makeData({ type, length: node.length });
  }

  if (DataType.isStruct(type)) {
    const children = type.children.map((f, i) => buildData(f.type, node.children[i]));
    return makeData({ ...common, type, children });
  }

  if (DataType.isList(type) || DataType.isMap(type)) {
    // List: [offsets] + value child. Map: [offsets] + entries-struct child.
    const child = buildData(type.children[0].type, node.children[0]);
    return makeData({
      ...common,
      type: type as any,
      valueOffsets: new Int32Array(node.buffers[0]),
      child,
    });
  }

  if (DataType.isFixedSizeList(type)) {
    const child = buildData(type.children[0].type, node.children[0]);
    return makeData({ ...common, type, child });
  }

  if (DataType.isUtf8(type) || DataType.isBinary(type)) {
    return makeData({
      ...common,
      type: type as any,
      valueOffsets: new Int32Array(node.buffers[0]),
      data: new Uint8Array(node.buffers[1]),
    });
  }

  // Single value-buffer fixed-width types: bool, int, float, decimal,
  // date, time, timestamp, duration, interval, fixed-size-binary.
  return makeData({ ...common, type: type as any, data: new Uint8Array(node.buffers[0]) });
}

/**
 * Reconstruct a `RecordBatch` from the kernel's buffer tree, using `schema`
 * (decoded once via `decodeIpcSchema`) for the per-column `DataType`s.
 */
export function importZeroCopyBatch(schema: Schema<TypeMap>, batch: KernelZeroCopyBatch): RecordBatch<TypeMap> {
  if (batch.columns.length !== schema.fields.length) {
    throw new HiveDriverError(
      `kernel buffer-handoff import: column count ${batch.columns.length} ` +
        `does not match schema field count ${schema.fields.length}`,
    );
  }
  const columns = schema.fields.map((field, i) => buildData(field.type, batch.columns[i]));
  const structData = makeData({
    type: new Struct(schema.fields),
    length: batch.numRows,
    nullCount: 0,
    children: columns,
  });
  return new RecordBatch(schema, structData);
}
