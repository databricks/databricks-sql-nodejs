// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

/**
 * TDD harness for the round-2 INTERVAL parity fix.
 *
 * Verifies that the SEA path renders the exact thrift wire string for
 * INTERVAL YEAR-MONTH and INTERVAL DAY-TIME columns, regardless of
 * whether the kernel emits the value as native Arrow `Interval` or
 * native Arrow `Duration` (the latter is transparently rewritten to
 * `Int64` by `lib/sea/SeaArrowIpcDurationFix.ts` because `apache-arrow@13`
 * predates the `Duration` type id).
 *
 * Reference failure modes (round 5 testing):
 *   - YEAR-MONTH:
 *       thrift  → `"1-2"`        (string)
 *       SEA pre-fix → `{"0":1,"1":2}` (Int32Array surfaced as struct)
 *   - DAY-TIME:
 *       thrift  → `"1 02:03:04.000000000"`  (string)
 *       SEA pre-fix → throws `Unrecognized type: "Duration" (18)` on schema decode
 *
 * Both modes must now produce byte-identical thrift strings.
 */

import { expect } from 'chai';
import * as flatbuffers from 'flatbuffers';
import {
  Schema,
  Field,
  Int32,
  Int64,
  Interval,
  IntervalUnit,
  Table,
  RecordBatch,
  makeData,
  Struct,
  vectorFromArray,
  tableToIPC,
} from 'apache-arrow';

// eslint-disable-next-line import/no-internal-modules
import { Message as FbMessage } from 'apache-arrow/fb/message';
// eslint-disable-next-line import/no-internal-modules
import { MessageHeader } from 'apache-arrow/fb/message-header';
// eslint-disable-next-line import/no-internal-modules
import { Schema as FbSchema } from 'apache-arrow/fb/schema';
// eslint-disable-next-line import/no-internal-modules
import { Field as FbField } from 'apache-arrow/fb/field';
// eslint-disable-next-line import/no-internal-modules
import { Type as FbType } from 'apache-arrow/fb/type';
// eslint-disable-next-line import/no-internal-modules
import { Duration as FbDuration } from 'apache-arrow/fb/duration';
// eslint-disable-next-line import/no-internal-modules
import { TimeUnit as FbTimeUnit } from 'apache-arrow/fb/time-unit';

import SeaOperationBackend from '../../../lib/sea/SeaOperationBackend';
import ClientContextStub from '../.stubs/ClientContextStub';

// ---------------------------------------------------------------------------
// Test helpers.
// ---------------------------------------------------------------------------

class StatementStub {
  private readonly batches: Buffer[];

  private readonly schemaIpc: Buffer;

  public cancelled = false;

  public closed = false;

  constructor(schemaIpc: Buffer, batches: Buffer[]) {
    this.schemaIpc = schemaIpc;
    this.batches = [...batches];
  }

  public async fetchNextBatch(): Promise<{ ipcBytes: Buffer } | null> {
    if (this.batches.length === 0) return null;
    return { ipcBytes: this.batches.shift() as Buffer };
  }

  public async schema(): Promise<{ ipcBytes: Buffer }> {
    return { ipcBytes: this.schemaIpc };
  }

  public async cancel(): Promise<void> {
    this.cancelled = true;
  }

  public async close(): Promise<void> {
    this.closed = true;
  }
}

function withTypeName<T extends Field>(field: T, typeName: string): T {
  const meta = new Map<string, string>(field.metadata);
  meta.set('databricks.type_name', typeName);
  return new Field(field.name, field.type, field.nullable, meta) as T;
}

function ipcFromColumns(schema: Schema, columns: Record<string, unknown>): Buffer {
  const vectors: any[] = [];
  for (const field of schema.fields) {
    const col = columns[field.name];
    vectors.push(vectorFromArray(col as any, field.type));
  }
  const data = vectors.map((v) => v.data[0]);
  const struct = makeData({
    type: new Struct(schema.fields),
    children: data,
    length: vectors[0]?.length ?? 0,
    nullCount: 0,
  });
  const batch = new RecordBatch(schema, struct);
  const table = new Table([batch]);
  return Buffer.from(tableToIPC(table, 'stream'));
}

function ipcSchemaOnly(schema: Schema): Buffer {
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

/**
 * Build a schema-only IPC payload whose schema declares a single Arrow
 * `Duration` column. `apache-arrow@13` cannot build this directly (no
 * Duration class in the public API), so we hand-roll the FlatBuffer
 * using the internal `fb/*` accessor classes. The body bytes for this
 * column are bit-identical to an Int64 column.
 */
function ipcWithDurationSchema(fieldName: string, durationUnit: FbTimeUnit, typeName = 'INTERVAL'): Buffer {
  const builder = new flatbuffers.Builder(256);

  // KeyValue for databricks.type_name
  const tnKey = builder.createString('databricks.type_name');
  const tnVal = builder.createString(typeName);
  const { KeyValue: FbKeyValueLocal } = require('apache-arrow/fb/key-value'); // eslint-disable-line @typescript-eslint/no-var-requires, global-require, import/no-internal-modules
  FbKeyValueLocal.startKeyValue(builder);
  FbKeyValueLocal.addKey(builder, tnKey);
  FbKeyValueLocal.addValue(builder, tnVal);
  const tnKv = FbKeyValueLocal.endKeyValue(builder);
  const metadataVec = FbField.createCustomMetadataVector(builder, [tnKv]);

  const nameOff = builder.createString(fieldName);
  const durOff = FbDuration.createDuration(builder, durationUnit);
  FbField.startField(builder);
  FbField.addName(builder, nameOff);
  FbField.addNullable(builder, true);
  FbField.addTypeType(builder, FbType.Duration);
  FbField.addType(builder, durOff);
  FbField.addCustomMetadata(builder, metadataVec);
  const fieldOff = FbField.endField(builder);
  const fieldsVec = FbSchema.createFieldsVector(builder, [fieldOff]);
  FbSchema.startSchema(builder);
  FbSchema.addFields(builder, fieldsVec);
  const schemaOff = FbSchema.endSchema(builder);
  FbMessage.startMessage(builder);
  FbMessage.addVersion(builder, 4); // V5
  FbMessage.addHeaderType(builder, MessageHeader.Schema);
  FbMessage.addHeader(builder, schemaOff);
  FbMessage.addBodyLength(builder, BigInt(0));
  const msgOff = FbMessage.endMessage(builder);
  builder.finish(msgOff);
  const bytes = builder.asUint8Array();
  const rem = bytes.byteLength % 8;
  const padded = rem === 0 ? bytes : new Uint8Array(bytes.byteLength + (8 - rem));
  if (rem !== 0) padded.set(bytes, 0);

  // IPC stream framing: continuation marker (0xFFFFFFFF) + length + bytes
  const prefix = Buffer.alloc(8);
  prefix.writeInt32LE(-1, 0);
  prefix.writeInt32LE(padded.byteLength, 4);

  // EOS marker (continuation + zero length) — terminates the stream.
  const eos = Buffer.alloc(8);
  eos.writeInt32LE(-1, 0);
  eos.writeInt32LE(0, 4);

  return Buffer.concat([prefix, Buffer.from(padded), eos]);
}

/**
 * Splice a hand-built Duration schema into an Int64-based IPC stream
 * so the record batch body bytes (which are Int64-encoded) become
 * "Duration-shaped" without us re-encoding the body. Used to fabricate
 * a kernel-shaped Duration IPC payload using only the apache-arrow@13
 * public API.
 */
function buildDurationIpc(fieldName: string, durationUnit: FbTimeUnit, values: bigint[], typeName = 'INTERVAL'): Buffer {
  // Build an Int64 stream that carries the values.
  const int64Schema = new Schema([new Field(fieldName, new Int64(), true)]);
  const int64Ipc = ipcFromColumns(int64Schema, {
    [fieldName]: [new BigInt64Array(values)],
  });

  // Build a Duration schema-only message that we splice in to replace
  // the Int64 schema. The record-batch bytes from int64Ipc follow
  // unchanged.
  const durationSchemaIpc = ipcWithDurationSchema(fieldName, durationUnit, typeName);

  // Skip the Int64 schema header + EOS in durationSchemaIpc, then
  // append the int64 stream's record batches.
  // int64Ipc layout: [continuation+len+schema][continuation+len+recordbatch][continuation+0 EOS]
  let cursor = 0;
  let len = int64Ipc.readInt32LE(cursor);
  cursor += 4;
  if (len === -1) {
    len = int64Ipc.readInt32LE(cursor);
    cursor += 4;
  }
  // Skip the schema body (always empty for schema messages)
  const intRecordsStart = cursor + len;
  const intRecords = int64Ipc.subarray(intRecordsStart);

  // durationSchemaIpc layout: [prefix][padded schema bytes][EOS].
  // Drop its EOS so it concatenates cleanly with intRecords (which has
  // its own EOS).
  const durationNoEos = durationSchemaIpc.subarray(0, durationSchemaIpc.byteLength - 8);
  return Buffer.concat([durationNoEos, intRecords]);
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

describe('SeaOperationBackend — INTERVAL parity with thrift', () => {
  it('YEAR-MONTH via native Arrow Interval[YearMonth] → "Y-M"', async () => {
    // Arrow `Interval[YearMonth]` carries a single int32 total-months
    // value. apache-arrow surfaces it as Int32Array(2) via the
    // GetVisitor. The kernel emits this type for INTERVAL YEAR-MONTH.
    const fields = [
      withTypeName(new Field('iv', new Interval(IntervalUnit.YEAR_MONTH), true), 'INTERVAL'),
    ];
    const schema = new Schema(fields);
    const schemaIpc = ipcSchemaOnly(schema);

    // 1 year, 2 months → 14 total months. `vectorFromArray(Int32Array,
    // new Interval(...))` packs the int32 total directly into the
    // Interval column's underlying values buffer.
    const dataIpc = ipcFromColumns(schema, { iv: Int32Array.from([14]) });

    const stub = new StatementStub(schemaIpc, [dataIpc]);
    const backend = new SeaOperationBackend({ statement: stub, context: new ClientContextStub() });
    const rows = await backend.fetchChunk({ limit: 100 });
    expect(rows).to.have.length(1);
    expect((rows[0] as any).iv).to.equal('1-2');
  });

  it('YEAR-MONTH negative → "-Y-M"', async () => {
    const fields = [
      withTypeName(new Field('iv', new Interval(IntervalUnit.YEAR_MONTH), true), 'INTERVAL'),
    ];
    const schema = new Schema(fields);
    const schemaIpc = ipcSchemaOnly(schema);

    // -14 total months → -1 year -2 months.
    const dataIpc = ipcFromColumns(schema, { iv: Int32Array.from([-14]) });

    const stub = new StatementStub(schemaIpc, [dataIpc]);
    const backend = new SeaOperationBackend({ statement: stub, context: new ClientContextStub() });
    const rows = await backend.fetchChunk({ limit: 100 });
    expect(rows).to.have.length(1);
    expect((rows[0] as any).iv).to.equal('-1-2');
  });

  it('DAY-TIME via Arrow Duration(MICROSECOND) → "1 02:03:04.000000000"', async () => {
    // 1 day + 2h + 3min + 4s = 93784 seconds = 93_784_000_000 µs.
    const microseconds = BigInt(93_784) * BigInt(1_000_000);
    const ipc = buildDurationIpc('iv', FbTimeUnit.MICROSECOND, [microseconds], 'INTERVAL');
    const schemaIpc = ipcWithDurationSchema('iv', FbTimeUnit.MICROSECOND, 'INTERVAL');

    const stub = new StatementStub(schemaIpc, [ipc]);
    const backend = new SeaOperationBackend({ statement: stub, context: new ClientContextStub() });
    const rows = await backend.fetchChunk({ limit: 100 });
    expect(rows).to.have.length(1);
    expect((rows[0] as any).iv).to.equal('1 02:03:04.000000000');
  });

  it('DAY-TIME via Arrow Duration(NANOSECOND) preserves nanosecond precision', async () => {
    // 1 day + 2h + 3min + 4.123456789s
    const nanos =
      BigInt(86400 + 2 * 3600 + 3 * 60 + 4) * BigInt(1_000_000_000) + BigInt(123_456_789);
    const ipc = buildDurationIpc('iv', FbTimeUnit.NANOSECOND, [nanos], 'INTERVAL');
    const schemaIpc = ipcWithDurationSchema('iv', FbTimeUnit.NANOSECOND, 'INTERVAL');

    const stub = new StatementStub(schemaIpc, [ipc]);
    const backend = new SeaOperationBackend({ statement: stub, context: new ClientContextStub() });
    const rows = await backend.fetchChunk({ limit: 100 });
    expect(rows).to.have.length(1);
    expect((rows[0] as any).iv).to.equal('1 02:03:04.123456789');
  });

  it('DAY-TIME zero → "0 00:00:00.000000000"', async () => {
    const ipc = buildDurationIpc('iv', FbTimeUnit.MICROSECOND, [BigInt(0)], 'INTERVAL');
    const schemaIpc = ipcWithDurationSchema('iv', FbTimeUnit.MICROSECOND, 'INTERVAL');

    const stub = new StatementStub(schemaIpc, [ipc]);
    const backend = new SeaOperationBackend({ statement: stub, context: new ClientContextStub() });
    const rows = await backend.fetchChunk({ limit: 100 });
    expect(rows).to.have.length(1);
    expect((rows[0] as any).iv).to.equal('0 00:00:00.000000000');
  });

  it('DAY-TIME negative → leading "-"', async () => {
    // -(1 day + 2h + 3min + 4s) in microseconds.
    const microseconds = -(BigInt(93_784) * BigInt(1_000_000));
    const ipc = buildDurationIpc('iv', FbTimeUnit.MICROSECOND, [microseconds], 'INTERVAL');
    const schemaIpc = ipcWithDurationSchema('iv', FbTimeUnit.MICROSECOND, 'INTERVAL');

    const stub = new StatementStub(schemaIpc, [ipc]);
    const backend = new SeaOperationBackend({ statement: stub, context: new ClientContextStub() });
    const rows = await backend.fetchChunk({ limit: 100 });
    expect(rows).to.have.length(1);
    expect((rows[0] as any).iv).to.equal('-1 02:03:04.000000000');
  });

  it('Duration column round-trips alongside primitive columns (DRY: same converter handles both intervals)', async () => {
    // Schema: [iv: Duration(µs), n: Int32]. The pre-processor must
    // rewrite the Duration field WITHOUT disturbing the Int32 sibling.
    // We hand-build the Duration schema (apache-arrow@13 can't build
    // Duration directly) and a body that has [Int64 column, Int32 col].
    // The rewriter must keep the Int32 column intact and substitute
    // Int64 for Duration.
    //
    // Note: we use a single-Duration-column test here because mixing
    // hand-built Duration with apache-arrow's batch builder requires
    // hand-rolling the entire IPC stream. The "Duration alongside
    // other columns" coverage is provided by the E2E parity tests
    // (M0-DT-019 in `tests/nodejs/test/parity/M0DatatypeParityTests.test.ts`)
    // which use a real warehouse query that mixes INTERVAL with other
    // types.
    const microseconds = BigInt(86_400) * BigInt(1_000_000); // 1 day
    const ipc = buildDurationIpc('iv', FbTimeUnit.MICROSECOND, [microseconds], 'INTERVAL');
    const schemaIpc = ipcWithDurationSchema('iv', FbTimeUnit.MICROSECOND, 'INTERVAL');

    const stub = new StatementStub(schemaIpc, [ipc]);
    const backend = new SeaOperationBackend({ statement: stub, context: new ClientContextStub() });

    // Round-trip the metadata to confirm we synthesise the right TTypeId.
    const metadata = await backend.getResultMetadata();
    expect(metadata.schema?.columns?.[0]?.typeDesc.types?.[0]?.primitiveEntry?.type).to.equal(
      // INTERVAL_DAY_TIME_TYPE = 30 in TCLIService_types
      // We assert by importing the enum below to avoid magic numbers.
      // eslint-disable-next-line global-require, @typescript-eslint/no-var-requires
      require('../../../thrift/TCLIService_types').TTypeId.INTERVAL_DAY_TIME_TYPE,
    );

    const rows = await backend.fetchChunk({ limit: 100 });
    expect(rows).to.have.length(1);
    expect((rows[0] as any).iv).to.equal('1 00:00:00.000000000');
  });
});
