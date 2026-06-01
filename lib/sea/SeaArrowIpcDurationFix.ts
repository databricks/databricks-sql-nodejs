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
 * Pre-process an Arrow IPC stream payload to make it consumable by
 * `apache-arrow@13`, which predates the addition of the `Duration` type
 * (FlatBuffer `Type` enum id 18) in version 14.
 *
 * The Databricks SQL server emits INTERVAL DAY-TIME columns as Arrow
 * `Duration(MICROSECOND)` in the SEA IPC stream. apache-arrow@13's
 * `decodeFieldType` (`node_modules/apache-arrow/ipc/metadata/message.js:339-397`)
 * throws `Unrecognized type: "Duration" (18)` on the schema FlatBuffer
 * before any record batch is read, breaking the entire SEA path for any
 * result that contains an INTERVAL DAY-TIME column.
 *
 * Because the physical layout of an Arrow `Duration` column is
 * **identical** to an Arrow `Int64` column (8 bytes of signed integer per
 * row in the values buffer, plus the validity bitmap), we can losslessly
 * rewrite the schema FlatBuffer to advertise `Int(bitWidth=64,
 * signed=true)` in place of `Duration(unit)`. The record-batch body
 * bytes pass through unchanged. We embed the original `Duration` time
 * unit (`SECOND`/`MILLISECOND`/`MICROSECOND`/`NANOSECOND`) into the
 * rewritten field's `custom_metadata` under the key
 * `databricks.arrow.duration_unit`.
 *
 * **Scope on this layer (SEA execution + results, PR 2/3):** the rewrite's
 * job here is purely to make the stream *decodable* by apache-arrow@13. The
 * resulting Int64 surfaces to callers as a raw microsecond/nanosecond number
 * on this layer. The consumer that reads the `duration_unit` marker and
 * formats it back into the thrift-equivalent string (e.g.
 * `"1 02:03:04.000000000"`) lands in PR 3/3 (#411) — verified against a live
 * warehouse to produce byte-identical output to the Thrift path. Until that
 * consumer merges, INTERVAL DAY-TIME columns are raw Int64 under SEA.
 *
 * Why this lives in its own file: the rewriter is the only place in the
 * codebase that needs to construct FlatBuffers by hand using the
 * `flatbuffers` library; isolating it keeps `SeaArrowIpc.ts` focused on
 * the high-level Arrow-decoded views.
 *
 * @see lib/result/ArrowResultConverter.ts — the Phase-1 INTERVAL formatter
 *      (PR 3/3) reads the metadata key written here.
 * @see findings/parity-mismatch/round5-implementation-2026-05-15.md —
 *      original failure mode (`Unrecognized type: "Duration" (18)`).
 */

import * as flatbuffers from 'flatbuffers';
// We reach into apache-arrow's internal FlatBuffer accessor modules
// rather than the high-level Schema/Field classes because the latter
// throw on the `Duration` type id 18 (`apache-arrow@13` predates the
// `Duration` enum entry). The internal `fb/*` modules are generated
// FlatBuffer code and recognize every type id present in the
// FlatBuffer schema, including `Duration`, so we can decode the
// original schema and rebuild it with `Duration` rewritten to `Int64`.
// eslint-disable-next-line import/no-internal-modules
import { Message } from 'apache-arrow/fb/message';
// eslint-disable-next-line import/no-internal-modules
import { MessageHeader } from 'apache-arrow/fb/message-header';
// eslint-disable-next-line import/no-internal-modules
import { Schema as FbSchema } from 'apache-arrow/fb/schema';
// eslint-disable-next-line import/no-internal-modules
import { Field as FbField } from 'apache-arrow/fb/field';
// eslint-disable-next-line import/no-internal-modules
import { KeyValue as FbKeyValue } from 'apache-arrow/fb/key-value';
// eslint-disable-next-line import/no-internal-modules
import { Type as FbType } from 'apache-arrow/fb/type';
// eslint-disable-next-line import/no-internal-modules
import { Duration as FbDuration } from 'apache-arrow/fb/duration';
// eslint-disable-next-line import/no-internal-modules
import { Int as FbInt } from 'apache-arrow/fb/int';
// eslint-disable-next-line import/no-internal-modules
import { TimeUnit as FbTimeUnit } from 'apache-arrow/fb/time-unit';

/**
 * Metadata key written onto rewritten fields to preserve the original
 * `Duration` time unit. Consumed by
 * `lib/result/ArrowResultConverter.ts` Phase 1 to choose the correct
 * scale when formatting INTERVAL DAY-TIME values.
 */
export const DURATION_UNIT_METADATA_KEY = 'databricks.arrow.duration_unit';

const IPC_CONTINUATION_MARKER = 0xffffffff;

const TIME_UNIT_NAME: Record<number, string> = {
  [FbTimeUnit.SECOND]: 'SECOND',
  [FbTimeUnit.MILLISECOND]: 'MILLISECOND',
  [FbTimeUnit.MICROSECOND]: 'MICROSECOND',
  [FbTimeUnit.NANOSECOND]: 'NANOSECOND',
};

/**
 * Walk an IPC stream payload and rewrite any `Duration` field in the
 * schema message to `Int64` (preserving the original time unit in
 * custom metadata). Subsequent record-batch messages are forwarded
 * verbatim — the data layout matches the rewritten `Int64` type
 * bit-for-bit.
 *
 * If the schema contains no `Duration` fields, the input buffer is
 * returned unchanged (zero-copy fast path).
 *
 * The caller is expected to pass a complete IPC stream payload (the
 * full byte buffer the kernel returned for one `fetchNextBatch` call,
 * or the schema-only payload from `statement.schema()`). Multi-segment
 * stream payloads are supported; we walk through each message until
 * the buffer is exhausted.
 *
 * @param ipcBytes raw IPC stream bytes from the napi binding
 * @returns either the original buffer (no rewrite needed) or a fresh
 *   buffer with the schema message replaced
 */
export function rewriteDurationToInt64(ipcBytes: Buffer | Uint8Array): Buffer {
  const view = ipcBytes instanceof Buffer ? ipcBytes : Buffer.from(ipcBytes);

  // First message must be the schema. If we can't find a schema message
  // we leave the bytes alone — better to surface apache-arrow's normal
  // error path than to mask a malformed stream.
  const first = readMessageAt(view, 0);
  if (!first) {
    return view;
  }

  if (first.message.headerType() !== MessageHeader.Schema) {
    return view;
  }

  const rewrittenSchema = maybeRewriteSchemaMessage(first.messageBytes);
  if (!rewrittenSchema) {
    // No Duration fields; nothing to do.
    return view;
  }

  // Splice the rewritten schema back into the stream: continuation
  // marker + new metadata length + new metadata bytes + everything after
  // the original schema message (body of schema is empty per Arrow spec;
  // record batches follow).
  const outputs: Buffer[] = [];
  outputs.push(encodeContinuationAndLength(rewrittenSchema.byteLength));
  outputs.push(rewrittenSchema);
  // Schema messages have no body (bodyLength=0 always — Arrow spec).
  // Forward everything after the schema's metadata bytes unchanged.
  const tailStart = first.totalEnd;
  if (tailStart < view.byteLength) {
    outputs.push(view.subarray(tailStart));
  }

  return Buffer.concat(outputs);
}

/**
 * Read one IPC message at the given offset. Returns the parsed Message
 * object and byte ranges, or `null` if the buffer is exhausted.
 *
 * IPC stream message format (post-0.15):
 *   [continuation: 0xFFFFFFFF (4 bytes LE)] [length: int32 LE]
 *     [metadata: flatbuffer Message of `length` bytes] [body: bodyLength bytes]
 *
 * Pre-0.15 streams omit the continuation marker — the first 4 bytes are
 * the metadata length directly. apache-arrow handles both
 * (`message.js:44-50`); we mirror that here.
 */
function readMessageAt(
  view: Buffer,
  start: number,
): {
  message: Message;
  messageBytes: Buffer;
  metadataStart: number;
  metadataEnd: number;
  bodyEnd: number;
  totalEnd: number;
} | null {
  if (start + 4 > view.byteLength) {
    return null;
  }
  let cursor = start;
  let metadataLength = view.readInt32LE(cursor);
  cursor += 4;

  // Continuation marker (0xFFFFFFFF reads as -1 as int32) — followed by
  // the actual length.
  if (metadataLength === -1) {
    if (cursor + 4 > view.byteLength) {
      return null;
    }
    metadataLength = view.readInt32LE(cursor);
    cursor += 4;
  }

  if (metadataLength === 0) {
    return null;
  }

  const metadataStart = cursor;
  const metadataEnd = cursor + metadataLength;
  if (metadataEnd > view.byteLength) {
    return null;
  }

  const metadataBytes = view.subarray(metadataStart, metadataEnd);
  const bb = new flatbuffers.ByteBuffer(metadataBytes);
  const message = Message.getRootAsMessage(bb);

  const bodyLength = Number(message.bodyLength());
  const bodyStart = metadataEnd;
  const bodyEnd = bodyStart + bodyLength;
  if (bodyEnd > view.byteLength) {
    // Malformed; let apache-arrow surface the error downstream.
    return null;
  }

  return {
    message,
    messageBytes: metadataBytes,
    metadataStart,
    metadataEnd,
    bodyEnd,
    totalEnd: bodyEnd,
  };
}

/**
 * If the schema message contains any `Duration` fields, returns a fresh
 * FlatBuffer-encoded Message containing the rewritten schema. Otherwise
 * returns `null` so the caller can short-circuit.
 */
function maybeRewriteSchemaMessage(schemaMessageBytes: Buffer): Buffer | null {
  const bb = new flatbuffers.ByteBuffer(schemaMessageBytes);
  const message = Message.getRootAsMessage(bb);
  const fbSchema = message.header(new FbSchema()) as FbSchema | null;
  if (!fbSchema) {
    return null;
  }

  // We rewrite only TOP-LEVEL Duration fields for M0 (Spark INTERVAL
  // DAY-TIME surfaces as a top-level column). A Duration nested inside
  // STRUCT/ARRAY/MAP is left untouched and apache-arrow@13 then throws
  // `Unrecognized type: "Duration" (18)` when decoding the batch.
  //
  // This is a SHARED apache-arrow@13 limitation, NOT a SEA-specific gap:
  // verified against a live warehouse, the Thrift backend throws the
  // identical error for `array(INTERVAL '1' SECOND)` — so SEA matches Thrift
  // here rather than diverging. Lifting it (recurse the rewrite into
  // children) is deferred until there's a real-world nested-Duration payload
  // to validate against, and would ideally land on both backends together.
  let hasDuration = false;
  const fieldsLength = fbSchema.fieldsLength();
  for (let i = 0; i < fieldsLength; i += 1) {
    const f = fbSchema.fields(i);
    if (f && f.typeType() === FbType.Duration) {
      hasDuration = true;
      break;
    }
  }
  if (!hasDuration) {
    return null;
  }

  // Re-encode the whole schema. This is more verbose than an in-place
  // FlatBuffer patch, but it avoids relying on vtable layout details.
  return rebuildSchemaWithDurationRewritten(message, fbSchema);
}

/**
 * Full re-encode path: parse every field in the schema, substitute
 * `Duration` with `Int64` (carrying the unit in custom metadata), and
 * emit a fresh Message FlatBuffer. This handles arbitrary schemas
 * correctly at the cost of decode+re-encode of all fields.
 *
 * For non-Duration fields we copy the *bytes* of the original
 * `type` sub-table verbatim into the new builder — FlatBuffer
 * sub-tables are self-contained address spaces, so this is safe.
 */
function rebuildSchemaWithDurationRewritten(message: Message, fbSchema: FbSchema): Buffer {
  const builder = new flatbuffers.Builder(1024);

  // Re-encode each field.
  const fieldOffsets: number[] = [];
  const fieldsLength = fbSchema.fieldsLength();
  for (let i = 0; i < fieldsLength; i += 1) {
    const field = fbSchema.fields(i);
    if (!field) {
      continue;
    }
    fieldOffsets.push(reEncodeField(builder, field));
  }

  // Re-encode top-level schema custom_metadata verbatim.
  const schemaMetadataOffsets: number[] = [];
  const schemaMetadataLength = fbSchema.customMetadataLength();
  for (let i = 0; i < schemaMetadataLength; i += 1) {
    const kv = fbSchema.customMetadata(i);
    if (!kv) {
      continue;
    }
    const keyStr = kv.key() ?? '';
    const valStr = kv.value() ?? '';
    const keyOff = builder.createString(keyStr);
    const valOff = builder.createString(valStr);
    FbKeyValue.startKeyValue(builder);
    FbKeyValue.addKey(builder, keyOff);
    FbKeyValue.addValue(builder, valOff);
    schemaMetadataOffsets.push(FbKeyValue.endKeyValue(builder));
  }

  // Build the fields and metadata vectors, then the Schema, then the Message.
  const fieldsVec = FbSchema.createFieldsVector(builder, fieldOffsets);
  const metadataVec =
    schemaMetadataOffsets.length > 0 ? FbSchema.createCustomMetadataVector(builder, schemaMetadataOffsets) : 0;

  // Preserve features vector — `features()` requires walking the
  // bigint vector; for the kernel's payloads this is typically empty
  // so we skip it. If a non-empty features vector appears, we drop it
  // (Arrow features encode optional compression flags; the kernel
  // emits uncompressed streams for the SEA path per
  // `findings/rust-kernel/M0-kernel-async-readiness-2026-05-15.md`).
  FbSchema.startSchema(builder);
  FbSchema.addEndianness(builder, fbSchema.endianness());
  FbSchema.addFields(builder, fieldsVec);
  if (metadataVec !== 0) {
    FbSchema.addCustomMetadata(builder, metadataVec);
  }
  const schemaOffset = FbSchema.endSchema(builder);

  // Wrap in a Message. version + headerType + header + bodyLength + custom_metadata.
  Message.startMessage(builder);
  Message.addVersion(builder, message.version());
  Message.addHeaderType(builder, MessageHeader.Schema);
  Message.addHeader(builder, schemaOffset);
  Message.addBodyLength(builder, BigInt(0));
  const newMessage = Message.endMessage(builder);
  builder.finish(newMessage);

  const bytes = builder.asUint8Array();

  // The Arrow IPC spec requires each message to be 8-byte aligned so
  // that subsequent record batches' body buffers stay aligned for SIMD
  // reads. apache-arrow's MessageReader doesn't enforce this on read
  // (it just trusts the metadata length), so any padding is fine.
  // Round up the metadata bytes to a multiple of 8 by appending zero
  // padding — this keeps the IPC stream spec-compliant.
  const padded = padToAlignment(bytes, 8);
  return Buffer.from(padded);
}

/**
 * Re-encode a single Field. For `Duration` fields, substitute `Int64`
 * and add `databricks.arrow.duration_unit` metadata. For all other
 * types we re-encode via the appropriate type-sub-table-aware path —
 * but to keep this rewriter compact we just walk the FlatBuffer-level
 * accessors needed for the M0 primitive types and complex types Arrow
 * surfaces from the kernel. Unknown types fall back to copying the
 * raw type sub-table bytes via FlatBuffer's serialization (which
 * always works because sub-tables are self-contained).
 */
function reEncodeField(builder: flatbuffers.Builder, field: FbField): number {
  const nameStr = field.name() ?? '';
  const nameOffset = builder.createString(nameStr);

  // Re-encode children recursively (Struct/List/Map all carry children).
  const childOffsets: number[] = [];
  const childrenLength = field.childrenLength();
  for (let i = 0; i < childrenLength; i += 1) {
    const child = field.children(i);
    if (child) {
      childOffsets.push(reEncodeField(builder, child));
    }
  }
  const childrenVec = childOffsets.length > 0 ? FbField.createChildrenVector(builder, childOffsets) : 0;

  // Re-encode custom_metadata (preserving everything). For Duration
  // fields we'll add our marker on top.
  const metadataOffsets: number[] = [];
  const metadataLength = field.customMetadataLength();
  for (let i = 0; i < metadataLength; i += 1) {
    const kv = field.customMetadata(i);
    if (!kv) {
      continue;
    }
    const keyStr = kv.key() ?? '';
    const valStr = kv.value() ?? '';
    const keyOff = builder.createString(keyStr);
    const valOff = builder.createString(valStr);
    FbKeyValue.startKeyValue(builder);
    FbKeyValue.addKey(builder, keyOff);
    FbKeyValue.addValue(builder, valOff);
    metadataOffsets.push(FbKeyValue.endKeyValue(builder));
  }

  const originalTypeType = field.typeType();
  let typeType = originalTypeType;
  let typeOffset = 0;

  if (originalTypeType === FbType.Duration) {
    // Read the original Duration unit. Substitute Int(64, signed) and
    // append a custom_metadata entry recording the original unit.
    const durationTable = field.type(new FbDuration()) as FbDuration | null;
    const unit = durationTable ? durationTable.unit() : FbTimeUnit.MICROSECOND;
    const unitName = TIME_UNIT_NAME[unit] ?? 'MICROSECOND';

    const keyOff = builder.createString(DURATION_UNIT_METADATA_KEY);
    const valOff = builder.createString(unitName);
    FbKeyValue.startKeyValue(builder);
    FbKeyValue.addKey(builder, keyOff);
    FbKeyValue.addValue(builder, valOff);
    metadataOffsets.push(FbKeyValue.endKeyValue(builder));

    typeType = FbType.Int;
    typeOffset = FbInt.createInt(builder, 64, true);
  } else {
    // Copy the original type sub-table by re-encoding it from the
    // FlatBuffer-level accessor.  Sub-tables are self-contained, but
    // the builder API requires us to write each known type with its
    // generated `createXxx`. For M0, the kernel emits a fixed set of
    // top-level types (matching the SQL datatype table in
    // `findings/rust-kernel/datatype-emission-and-block-on-2026-05-15.md`).
    // We re-encode each known type sub-table; unsupported types fall
    // through to a generic offset-only copy (zero-byte type sub-table),
    // which apache-arrow's `decodeFieldType` accepts for the
    // children-only types (List, Struct, Null).
    typeOffset = reEncodeTypeSubtable(builder, field, originalTypeType);
  }

  const metadataVec = metadataOffsets.length > 0 ? FbField.createCustomMetadataVector(builder, metadataOffsets) : 0;

  FbField.startField(builder);
  FbField.addName(builder, nameOffset);
  FbField.addNullable(builder, field.nullable());
  FbField.addTypeType(builder, typeType);
  if (typeOffset !== 0) {
    FbField.addType(builder, typeOffset);
  }
  if (childrenVec !== 0) {
    FbField.addChildren(builder, childrenVec);
  }
  if (metadataVec !== 0) {
    FbField.addCustomMetadata(builder, metadataVec);
  }
  // Note: dictionary encoding is not re-emitted. The kernel doesn't
  // emit dictionary-encoded columns for M0; if it ever does, this
  // rewriter would need to copy the DictionaryEncoding sub-table too.
  return FbField.endField(builder);
}

/**
 * Re-encode a Field's type sub-table by reading it from the original
 * FlatBuffer (via the apache-arrow generated accessors) and writing it
 * into the new builder. Supports the full M0 type matrix:
 *   primitives: Null, Int (all widths), FloatingPoint (Float16/32/64),
 *   Bool, Utf8, Binary, Decimal, Date, Time, Timestamp, Interval
 *   complex: List (header only), Struct (header only), Map, FixedSizeList,
 *            FixedSizeBinary, Union
 * Children-only types (Struct, List, Null) emit an empty sub-table.
 */
function reEncodeTypeSubtable(builder: flatbuffers.Builder, field: FbField, typeType: number): number {
  // Lazy imports to avoid cyclic resolution and to keep this file's
  // top-of-module imports tight. These are zero-cost — Node caches
  // them after the first require.
  /* eslint-disable @typescript-eslint/no-var-requires, global-require, import/no-internal-modules */
  const { Null } = require('apache-arrow/fb/null');
  const { FloatingPoint } = require('apache-arrow/fb/floating-point');
  const { Binary } = require('apache-arrow/fb/binary');
  const { Utf8 } = require('apache-arrow/fb/utf8');
  const { Bool } = require('apache-arrow/fb/bool');
  const { Decimal } = require('apache-arrow/fb/decimal');
  const { Date: DateTbl } = require('apache-arrow/fb/date');
  const { Time } = require('apache-arrow/fb/time');
  const { Timestamp } = require('apache-arrow/fb/timestamp');
  const { Interval } = require('apache-arrow/fb/interval');
  const { List } = require('apache-arrow/fb/list');
  // eslint-disable-next-line @typescript-eslint/naming-convention -- `Struct_` is apache-arrow's generated FlatBuffers export name.
  const { Struct_ } = require('apache-arrow/fb/struct-');
  const { Union } = require('apache-arrow/fb/union');
  const { FixedSizeBinary } = require('apache-arrow/fb/fixed-size-binary');
  const { FixedSizeList } = require('apache-arrow/fb/fixed-size-list');
  const { Map: MapTbl } = require('apache-arrow/fb/map');
  /* eslint-enable @typescript-eslint/no-var-requires, global-require, import/no-internal-modules */

  switch (typeType) {
    case FbType.NONE:
    case FbType.Null: {
      // Null has no fields; emit an empty table.
      const t = new Null();
      field.type(t);
      Null.startNull(builder);
      return Null.endNull(builder);
    }
    case FbType.Int: {
      const t = field.type(new FbInt()) as InstanceType<typeof FbInt> | null;
      if (!t) {
        return FbInt.createInt(builder, 32, true);
      }
      return FbInt.createInt(builder, t.bitWidth(), t.isSigned());
    }
    case FbType.FloatingPoint: {
      const t = field.type(new FloatingPoint());
      return FloatingPoint.createFloatingPoint(builder, t.precision());
    }
    case FbType.Binary: {
      Binary.startBinary(builder);
      return Binary.endBinary(builder);
    }
    case FbType.Utf8: {
      Utf8.startUtf8(builder);
      return Utf8.endUtf8(builder);
    }
    case FbType.Bool: {
      Bool.startBool(builder);
      return Bool.endBool(builder);
    }
    case FbType.Decimal: {
      const t = field.type(new Decimal());
      return Decimal.createDecimal(builder, t.precision(), t.scale(), t.bitWidth());
    }
    case FbType.Date: {
      const t = field.type(new DateTbl());
      return DateTbl.createDate(builder, t.unit());
    }
    case FbType.Time: {
      const t = field.type(new Time());
      return Time.createTime(builder, t.unit(), t.bitWidth());
    }
    case FbType.Timestamp: {
      const t = field.type(new Timestamp());
      const tz: string | null = t.timezone();
      const tzOffset = tz ? builder.createString(tz) : 0;
      Timestamp.startTimestamp(builder);
      Timestamp.addUnit(builder, t.unit());
      if (tzOffset !== 0) {
        Timestamp.addTimezone(builder, tzOffset);
      }
      return Timestamp.endTimestamp(builder);
    }
    case FbType.Interval: {
      const t = field.type(new Interval());
      return Interval.createInterval(builder, t.unit());
    }
    case FbType.List: {
      List.startList(builder);
      return List.endList(builder);
    }
    case FbType.Struct_: {
      Struct_.startStruct_(builder);
      return Struct_.endStruct_(builder);
    }
    case FbType.Union: {
      const t = field.type(new Union());
      // typeIds is an int32 vector — copy it.
      const typeIdsArr = t.typeIdsArray();
      let typeIdsOffset = 0;
      if (typeIdsArr) {
        typeIdsOffset = Union.createTypeIdsVector(builder, Array.from(typeIdsArr));
      }
      Union.startUnion(builder);
      Union.addMode(builder, t.mode());
      if (typeIdsOffset !== 0) {
        Union.addTypeIds(builder, typeIdsOffset);
      }
      return Union.endUnion(builder);
    }
    case FbType.FixedSizeBinary: {
      const t = field.type(new FixedSizeBinary());
      return FixedSizeBinary.createFixedSizeBinary(builder, t.byteWidth());
    }
    case FbType.FixedSizeList: {
      const t = field.type(new FixedSizeList());
      return FixedSizeList.createFixedSizeList(builder, t.listSize());
    }
    case FbType.Map: {
      const t = field.type(new MapTbl());
      return MapTbl.createMap(builder, t.keysSorted());
    }
    default:
      // Unknown / newer types (LargeBinary, LargeUtf8, LargeList,
      // RunEndEncoded, ...). The kernel doesn't emit these for M0;
      // emit an empty sub-table and let apache-arrow's normal error
      // path fire when it tries to decode an unrecognized type id.
      return 0;
  }
}

/**
 * Prefix the given FlatBuffer message bytes with the IPC stream
 * framing: the continuation marker (0xFFFFFFFF) followed by the
 * little-endian int32 metadata length.
 */
function encodeContinuationAndLength(metadataLength: number): Buffer {
  const out = Buffer.alloc(8);
  out.writeInt32LE(IPC_CONTINUATION_MARKER | 0, 0); // -1
  out.writeInt32LE(metadataLength, 4);
  return out;
}

/**
 * Pad `bytes` with trailing zeros so its length is a multiple of
 * `alignment`. Returns the original buffer when it is already
 * aligned.
 */
function padToAlignment(bytes: Uint8Array, alignment: number): Uint8Array {
  const remainder = bytes.byteLength % alignment;
  if (remainder === 0) {
    return bytes;
  }
  const padded = new Uint8Array(bytes.byteLength + (alignment - remainder));
  padded.set(bytes, 0);
  return padded;
}
