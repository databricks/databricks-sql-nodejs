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

import { expect } from 'chai';
import {
  tableFromArrays,
  vectorFromArray,
  makeData,
  Data,
  Table,
  RecordBatch,
  RecordBatchReader,
  RecordBatchStreamWriter,
  Schema,
  Field,
  Struct,
  List,
  Int32,
  Int64,
  Float64,
  Utf8,
  Bool,
  Dictionary,
  TypeMap,
  DataType,
} from 'apache-arrow';
import {
  importZeroCopyBatch,
  isZeroCopySupported,
  isSchemaZeroCopySupported,
  KernelArrayNode,
  KernelZeroCopyBatch,
} from '../../../lib/kernel/KernelArrowImport';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

// Hermetic coverage for the kernel copycage import layer. There is no
// warehouse here, so we synthesise the kernel's `zerocopy.rs::build_node`
// descriptor in pure JS by walking an apache-arrow `Data` tree's buffers —
// the exact buffer→slot mapping the native side emits — then feed it through
// `importZeroCopyBatch` and assert the result is byte-identical (via IPC
// round-trip) to the source. This pins the layout contract so any
// apache-arrow / arrow-rs columnar-layout drift fails loudly here rather than
// silently mis-decoding live results.

/**
 * Build a `KernelArrayNode` from an apache-arrow `Data`, mirroring the kernel's
 * `build_node`: copy each Arrow buffer into a fresh `ArrayBuffer` (the in-cage
 * memcpy the native side does), expose validity + the type's data buffers in
 * canonical order, and recurse into children. Offset is always 0 (the kernel
 * compacts before handoff).
 */
function nodeFromData(data: Data): KernelArrayNode {
  const toAB = (ta: { buffer: ArrayBufferLike; byteOffset: number; byteLength: number } | undefined): ArrayBuffer => {
    if (!ta) return new ArrayBuffer(0);
    // Copy out the exact byte window (the kernel ships a fresh in-cage copy).
    return ta.buffer.slice(ta.byteOffset, ta.byteOffset + ta.byteLength) as ArrayBuffer;
  };

  const validity =
    data.nullCount > 0 && data.values && (data as any).nullBitmap && (data as any).nullBitmap.byteLength > 0
      ? toAB((data as any).nullBitmap)
      : undefined;

  // Canonical Arrow buffer order per type. apache-arrow stores these as
  // `valueOffsets` (offsets) + `values` (data) for variable-binary, and
  // `values` for fixed-width. We mirror `ArrayData::buffers()` order.
  const buffers: ArrayBuffer[] = [];
  const type = data.type;
  if (DataType.isUtf8(type) || DataType.isBinary(type)) {
    buffers.push(toAB((data as any).valueOffsets));
    buffers.push(toAB((data as any).values));
  } else if (DataType.isList(type) || DataType.isMap(type)) {
    buffers.push(toAB((data as any).valueOffsets));
  } else if (DataType.isStruct(type) || DataType.isFixedSizeList(type) || DataType.isNull(type)) {
    // No data buffers (children carry everything; null has none).
  } else {
    buffers.push(toAB((data as any).values));
  }

  return {
    length: data.length,
    offset: 0,
    nullCount: data.nullCount,
    validity,
    buffers,
    children: data.children ? data.children.map((c) => nodeFromData(c)) : [],
  };
}

/** Build the kernel batch descriptor from an apache-arrow RecordBatch. */
function descriptorFromRecordBatch(rb: RecordBatch): KernelZeroCopyBatch {
  return {
    numRows: rb.numRows,
    columns: rb.schema.fields.map((_, i) => nodeFromData(rb.data.children[i])),
  };
}

/** Canonicalise a Table's rows to a comparable JSON string (bigint-safe). */
function serializeTable(t: Table): string {
  return JSON.stringify(t.toArray().map((r: any) => (r.toJSON ? r.toJSON() : r)), (_k, v) =>
    typeof v === 'bigint' ? `BI:${v}` : v,
  );
}

/** IPC round-trip: write the batch to an Arrow IPC stream and decode it back. */
function ipcRoundTrip(rb: RecordBatch): Table {
  const writer = new RecordBatchStreamWriter();
  writer.write(rb);
  writer.finish();
  const bytes = Buffer.from(writer.toUint8Array(true));
  const reader = RecordBatchReader.from([bytes]);
  return new Table(reader);
}

describe('KernelArrowImport — isZeroCopySupported predicate', () => {
  it('accepts the fixed-width / variable-binary leaf families', () => {
    expect(isZeroCopySupported(new Int32())).to.equal(true);
    expect(isZeroCopySupported(new Int64())).to.equal(true);
    expect(isZeroCopySupported(new Float64())).to.equal(true);
    expect(isZeroCopySupported(new Utf8())).to.equal(true);
    expect(isZeroCopySupported(new Bool())).to.equal(true);
  });

  it('rejects dictionary types (side-vector not in the buffer tree)', () => {
    const dict = new Dictionary(new Utf8(), new Int32());
    expect(isZeroCopySupported(dict as unknown as DataType)).to.equal(false);
  });

  it('recurses: a struct/list of a supported type is supported', () => {
    const okStruct = new Struct([new Field('a', new Int32()), new Field('b', new Utf8())]);
    expect(isZeroCopySupported(okStruct)).to.equal(true);
    const okList = new List(new Field('item', new Int32()));
    expect(isZeroCopySupported(okList)).to.equal(true);
  });

  it('recurses: a struct/list containing a dictionary is unsupported', () => {
    const dict = new Dictionary(new Utf8(), new Int32());
    const badStruct = new Struct([new Field('a', new Int32()), new Field('d', dict as unknown as DataType)]);
    expect(isZeroCopySupported(badStruct)).to.equal(false);
    const badList = new List(new Field('item', dict as unknown as DataType));
    expect(isZeroCopySupported(badList)).to.equal(false);
  });

  it('gates a whole schema: one unsupported column forces IPC', () => {
    const dict = new Dictionary(new Utf8(), new Int32());
    const goodSchema = new Schema([new Field('a', new Int32()), new Field('s', new Utf8())]);
    const badSchema = new Schema([new Field('a', new Int32()), new Field('d', dict as unknown as DataType)]);
    expect(isSchemaZeroCopySupported(goodSchema)).to.equal(true);
    expect(isSchemaZeroCopySupported(badSchema)).to.equal(false);
  });
});

describe('KernelArrowImport — importZeroCopyBatch layout-compat (pinned)', () => {
  it('rebuilds a fixed-width + string batch byte-identical to IPC', () => {
    // Build the string column as a PLAIN Utf8 vector (not dictionary): the
    // kernel always ships plain Utf8, whereas `tableFromArrays` would
    // auto-dictionary-encode a JS string array.
    const iVec = vectorFromArray([1, 2, 3, 4], new Int32());
    const dVec = vectorFromArray([1.5, 2.5, Number.NaN, -0], new Float64());
    const sVec = vectorFromArray(['a', '', '日本語🚀', 'x'.repeat(100)], new Utf8());
    const schema = new Schema([
      new Field('i', new Int32(), true),
      new Field('d', new Float64(), true),
      new Field('s', new Utf8(), true),
    ]);
    const rb = new RecordBatch(
      schema,
      makeData({ type: new Struct(schema.fields), length: 4, children: [iVec.data[0], dVec.data[0], sVec.data[0]] }),
    );
    const desc = descriptorFromRecordBatch(rb);
    const rebuilt = new Table(importZeroCopyBatch(schema, desc));
    expect(serializeTable(rebuilt)).to.equal(serializeTable(ipcRoundTrip(rb)));
  });

  it('rebuilds a batch with nulls byte-identical to IPC', () => {
    const v = vectorFromArray([10, null, 30, null, 50], new Int32());
    const schema = new Schema([new Field('n', new Int32(), true)]);
    const rb = new RecordBatch(schema, makeData({ type: new Struct(schema.fields), length: 5, children: [v.data[0]] }));
    const desc = descriptorFromRecordBatch(rb);
    const rebuilt = new Table(importZeroCopyBatch(schema, desc));
    expect(serializeTable(rebuilt)).to.equal(serializeTable(ipcRoundTrip(rb)));
  });

  it('rebuilds nested struct<int,list<int>> byte-identical to IPC', () => {
    const structVec = vectorFromArray(
      [
        { k: 1, v: [1, 2] },
        { k: 2, v: [3] },
        { k: 3, v: [] },
      ],
      new Struct([new Field('k', new Int32()), new Field('v', new List(new Field('item', new Int32())))]),
    );
    const schema = new Schema([new Field('st', structVec.type, true)]);
    const rb = new RecordBatch(
      schema,
      makeData({ type: new Struct(schema.fields), length: 3, children: [structVec.data[0]] }),
    );
    const desc = descriptorFromRecordBatch(rb);
    const rebuilt = new Table(importZeroCopyBatch(schema, desc));
    expect(serializeTable(rebuilt)).to.equal(serializeTable(ipcRoundTrip(rb)));
  });

  it('rejects a non-zero offset descriptor (kernel must compact first)', () => {
    const table = tableFromArrays({ i: Int32Array.from([1, 2, 3]) });
    const rb = table.batches[0];
    const desc = descriptorFromRecordBatch(rb);
    desc.columns[0].offset = 1; // simulate a sliced array slipping through
    expect(() => importZeroCopyBatch(rb.schema, desc)).to.throw(HiveDriverError, /non-zero array offset/);
  });

  it('rejects a column-count mismatch', () => {
    const table = tableFromArrays({ i: Int32Array.from([1, 2, 3]) });
    const rb = table.batches[0];
    const desc = descriptorFromRecordBatch(rb);
    desc.columns = []; // schema has 1 field, descriptor has 0
    expect(() => importZeroCopyBatch(rb.schema, desc)).to.throw(HiveDriverError, /column count/);
  });
});
