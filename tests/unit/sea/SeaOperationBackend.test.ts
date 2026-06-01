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
  Schema,
  Field,
  RecordBatch,
  Table,
  tableToIPC,
  Bool,
  Int8,
  Int16,
  Int32,
  Int64,
  Float32,
  Float64,
  Utf8,
  Binary,
  DateDay,
  TimestampMicrosecond,
  Decimal,
  Struct,
  makeData,
  vectorFromArray,
} from 'apache-arrow';

import SeaOperationBackend from '../../../lib/sea/SeaOperationBackend';
import ClientContextStub from '../.stubs/ClientContextStub';

// Minimal stub of the napi `Statement` surface that emits a precomputed
// Arrow IPC payload per `fetchNextBatch()` call. Used to feed
// `SeaOperationBackend` synthetic batches that mirror the kernel's
// per-batch IPC stream contract (`schema header + 1 record-batch
// message`) without loading the native binding.
class StatementStub {
  private readonly batches: Buffer[];

  private readonly schemaIpc: Buffer;

  public cancelled = false;

  public closed = false;

  constructor(schemaIpc: Buffer, batches: Buffer[]) {
    this.schemaIpc = schemaIpc;
    this.batches = [...batches];
  }

  // Mirrors the kernel `Statement.statementId` getter.
  public readonly statementId = '01ef-fake-statement-id';

  public async fetchNextBatch(): Promise<{ ipcBytes: Buffer } | null> {
    if (this.batches.length === 0) return null;
    return { ipcBytes: this.batches.shift() as Buffer };
  }

  // schema() is synchronous on the merged-kernel binding.
  public schema(): { ipcBytes: Buffer } {
    return { ipcBytes: this.schemaIpc };
  }

  public async cancel(): Promise<void> {
    this.cancelled = true;
  }

  public async close(): Promise<void> {
    this.closed = true;
  }

  // Status accessors from the kernel's status-fields surface.
  public async numModifiedRows(): Promise<number | null> {
    return null;
  }

  public async displayMessage(): Promise<string | null> {
    return null;
  }

  public async diagnosticInfo(): Promise<string | null> {
    return null;
  }

  public async errorDetailsJson(): Promise<string | null> {
    return null;
  }
}

// Helper: attach `databricks.type_name` to a field so the SEA Thrift
// schema synthesiser can resolve the TTypeId (matches kernel behaviour
// at `src/reader/mod.rs:476-504`).
function withTypeName<T extends Field>(field: T, typeName: string): T {
  const meta = new Map<string, string>(field.metadata);
  meta.set('databricks.type_name', typeName);
  return new Field(field.name, field.type, field.nullable, meta) as T;
}

// Build a single IPC stream (schema header + 1 record-batch message)
// from a Schema and a column->values mapping. Mirrors the kernel's
// per-batch ResultStream output shape.
function ipcFromColumns(schema: Schema, columns: Record<string, unknown[]>): Buffer {
  const vectors: any[] = [];
  for (const field of schema.fields) {
    const col = columns[field.name];
    vectors.push(vectorFromArray(col, field.type));
  }
  const data = vectors.map((v) => v.data[0]);
  const struct = makeData({
    type: new Struct(schema.fields),
    children: data,
    length: data[0]?.length ?? 0,
    nullCount: 0,
  });
  const batch = new RecordBatch(schema, struct);
  const table = new Table([batch]);
  return Buffer.from(tableToIPC(table, 'stream'));
}

function ipcSchemaOnly(schema: Schema): Buffer {
  // tableToIPC on an empty table produces a schema-only stream.
  const struct = makeData({
    type: new Struct(schema.fields),
    children: schema.fields.map((f) => makeData({ type: f.type as any, length: 0, nullCount: 0 })),
    length: 0,
    nullCount: 0,
  });
  const batch = new RecordBatch(schema, struct);
  const table = new Table([batch]);
  return Buffer.from(tableToIPC(table, 'stream'));
}

describe('SeaOperationBackend — M0 datatype round-trip via napi → ArrowResultConverter', () => {
  it('passes M0 primitive datatypes through the same converter the thrift path uses', async () => {
    // One row per M0 primitive type with a kernel-style metadata tag on
    // each field. Decimal carries a real scale (2) so the converter's
    // Phase-1 scale division produces 1.5 from the unscaled bigint.
    const fields = [
      withTypeName(new Field('b', new Bool(), true), 'BOOLEAN'),
      withTypeName(new Field('i8', new Int8(), true), 'TINYINT'),
      withTypeName(new Field('i16', new Int16(), true), 'SMALLINT'),
      withTypeName(new Field('i32', new Int32(), true), 'INT'),
      withTypeName(new Field('i64', new Int64(), true), 'BIGINT'),
      withTypeName(new Field('f32', new Float32(), true), 'FLOAT'),
      withTypeName(new Field('f64', new Float64(), true), 'DOUBLE'),
      withTypeName(new Field('s', new Utf8(), true), 'STRING'),
      withTypeName(new Field('bin', new Binary(), true), 'BINARY'),
      withTypeName(new Field('dt', new DateDay(), true), 'DATE'),
      withTypeName(new Field('ts', new TimestampMicrosecond(), true), 'TIMESTAMP'),
      // apache-arrow's Decimal signature is `(scale, precision, bitWidth)`.
      withTypeName(new Field('dec', new Decimal(2, 10, 128), true), 'DECIMAL'),
      // INTERVAL on the kernel side: Utf8 + metadata annotation.
      withTypeName(new Field('iv', new Utf8(), true), 'INTERVAL'),
    ];
    const schema = new Schema(fields);
    const schemaIpc = ipcSchemaOnly(schema);

    // DECIMAL: 128-bit little-endian unscaled integer. 150 little-endian
    // → [150, 0, 0, 0, ...0]. Phase-1 reads `valueType.scale` (=2) so the
    // converter divides by 100 to yield 1.5.
    const decimalBytes = new Uint8Array(16);
    decimalBytes[0] = 150;
    const dataIpc = ipcFromColumns(schema, {
      b: [true],
      i8: [Int8Array.from([1])[0]],
      i16: [Int16Array.from([200])[0]],
      i32: [42],
      i64: [BigInt(1234567890123)],
      f32: [Math.fround(1.5)],
      f64: [3.14],
      s: ['hello'],
      bin: [new Uint8Array([0xde, 0xad, 0xbe, 0xef])],
      dt: [new Date('2026-01-01T00:00:00Z')],
      // Builder for TimestampMicrosecond accepts numeric epoch-ms; the
      // internal scaling multiplies by 1000 to land on µs.
      ts: [new Date('2026-05-15T12:00:00Z').valueOf()],
      dec: [decimalBytes],
      iv: ['1-0'],
    });

    const stub = new StatementStub(schemaIpc, [dataIpc]);
    const backend = new SeaOperationBackend({
      statement: stub,
      context: new ClientContextStub(),
    });

    const rows = await backend.fetchChunk({ limit: 100 });
    expect(rows.length).to.equal(1);
    const row = rows[0] as Record<string, unknown>;

    expect(row.b).to.equal(true);
    expect(row.i8).to.equal(1);
    expect(row.i16).to.equal(200);
    expect(row.i32).to.equal(42);
    // BIGINT goes through Phase-2 convertBigInt → Number (matches thrift)
    expect(row.i64).to.equal(1234567890123);
    expect(row.f32).to.equal(Math.fround(1.5));
    expect(row.f64).to.equal(3.14);
    expect(row.s).to.equal('hello');
    expect(Buffer.isBuffer(row.bin)).to.equal(true);
    expect((row.bin as Buffer).equals(Buffer.from([0xde, 0xad, 0xbe, 0xef]))).to.equal(true);
    // DECIMAL: Phase-1 scale-aware coercion via Arrow's Decimal type → 1.5
    expect(row.dec).to.equal(1.5);
    // TIMESTAMP: Phase-1 produces JS Date for arrow timestamps
    expect(row.ts).to.be.instanceOf(Date);
    expect((row.ts as Date).toISOString()).to.equal('2026-05-15T12:00:00.000Z');
    // INTERVAL: kernel emits Utf8 + metadata; converter passes through as string
    expect(row.iv).to.equal('1-0');

    // After consuming the single batch, the backend should report no more rows.
    expect(await backend.hasMore()).to.equal(false);
  });

  it('round-trips ARRAY / MAP / STRUCT via the converter Phase-2 JSON fallback', async () => {
    // ARRAY / MAP / STRUCT have two possible wire encodings in M0:
    //   (a) native Arrow `List` / `Map` / `Struct` — Phase 1 produces plain
    //       JS objects; Phase 2 `convertJSON` sees a non-string and is a
    //       no-op (`utils.ts:39-49`).
    //   (b) Utf8 JSON strings — Phase 1 passthrough; Phase 2 `convertJSON`
    //       runs `JSON.parse` (`utils.ts:75-79`).
    // Both produce identical row shapes. We validate (b) here because
    // it's the deterministic case we can construct with the current
    // apache-arrow JS API; the kernel emits either depending on server
    // config (see `findings/rust-kernel/datatype-emission...:140-142`).
    const strSchema = new Schema([
      withTypeName(new Field('arr', new Utf8(), true), 'ARRAY'),
      withTypeName(new Field('m', new Utf8(), true), 'MAP'),
      withTypeName(new Field('s', new Utf8(), true), 'STRUCT'),
    ]);
    const strSchemaIpc = ipcSchemaOnly(strSchema);
    const strDataIpc = ipcFromColumns(strSchema, {
      arr: ['[1,2,3]'],
      m: ['{"k":1}'],
      s: ['{"a":1,"b":"hi"}'],
    });

    const stub = new StatementStub(strSchemaIpc, [strDataIpc]);
    const backend = new SeaOperationBackend({
      statement: stub,
      context: new ClientContextStub(),
    });
    const rows = await backend.fetchChunk({ limit: 100 });
    expect(rows.length).to.equal(1);
    const row = rows[0] as Record<string, unknown>;
    expect(row.arr).to.deep.equal([1, 2, 3]);
    expect(row.m).to.deep.equal({ k: 1 });
    expect(row.s).to.deep.equal({ a: 1, b: 'hi' });
  });

  it('streams multiple batches and reports hasMore correctly', async () => {
    const schema = new Schema([withTypeName(new Field('x', new Int32(), true), 'INT')]);
    const schemaIpc = ipcSchemaOnly(schema);
    const batch1 = ipcFromColumns(schema, { x: [1, 2] });
    const batch2 = ipcFromColumns(schema, { x: [3] });

    const stub = new StatementStub(schemaIpc, [batch1, batch2]);
    const backend = new SeaOperationBackend({
      statement: stub,
      context: new ClientContextStub(),
    });

    const all = await backend.fetchChunk({ limit: 10 });
    expect(all).to.deep.equal([{ x: 1 }, { x: 2 }, { x: 3 }]);
    expect(await backend.hasMore()).to.equal(false);
  });

  it('cancel / close delegate to the native statement', async () => {
    const schema = new Schema([withTypeName(new Field('x', new Int32(), true), 'INT')]);
    const schemaIpc = ipcSchemaOnly(schema);
    const stub = new StatementStub(schemaIpc, []);
    const backend = new SeaOperationBackend({ statement: stub, context: new ClientContextStub() });
    await backend.cancel();
    expect(stub.cancelled).to.equal(true);
    await backend.close();
    expect(stub.closed).to.equal(true);
  });
});
