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
import { tableFromArrays, RecordBatchStreamWriter } from 'apache-arrow';
import { countRowsInIpc, decodeIpcSchema, patchIpcBytes } from '../../../lib/kernel/KernelArrowIpc';
import { rewriteDurationToInt64 } from '../../../lib/kernel/KernelArrowIpcDurationFix';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

// Hermetic coverage for the SEA Arrow-IPC layer. apache-arrow@13 cannot
// construct a `Duration` column, so the Duration-positive rewrite path is
// covered by the live e2e (tests/e2e/kernel/interval-duration-e2e.test.ts).
// These tests pin everything reachable without a warehouse: the IPC
// framing walk, the no-op / malformed-input handling, the cheap
// row-count path, and the schema-decode guard.

/** Build a multi-message Arrow IPC stream from arrays of per-batch row data. */
function makeIpcStream(batchRows: number[][]): Buffer {
  const writer = new RecordBatchStreamWriter();
  for (const rows of batchRows) {
    const table = tableFromArrays({ a: Int32Array.from(rows) });
    writer.write(table.batches[0]);
  }
  writer.finish();
  return Buffer.from(writer.toUint8Array(true));
}

/** Schema-only IPC stream (no record batches). */
function makeSchemaOnlyStream(): Buffer {
  const writer = new RecordBatchStreamWriter();
  const table = tableFromArrays({ a: Int32Array.from([1]) });
  // Start the stream (emits the schema) then finish without writing a batch.
  writer.reset(undefined, table.schema);
  writer.finish();
  return Buffer.from(writer.toUint8Array(true));
}

describe('KernelArrowIpc.countRowsInIpc', () => {
  it('sums RecordBatch row counts across messages', () => {
    const ipc = makeIpcStream([
      [1, 2, 3],
      [4, 5],
    ]);
    expect(countRowsInIpc(ipc)).to.equal(5);
  });

  it('returns 0 for a schema-only stream (no record batches)', () => {
    expect(countRowsInIpc(makeSchemaOnlyStream())).to.equal(0);
  });

  it('counts a single-batch stream', () => {
    expect(countRowsInIpc(makeIpcStream([[10, 20, 30, 40]]))).to.equal(4);
  });
});

describe('KernelArrowIpc.rewriteDurationToInt64 (no-Duration / malformed paths)', () => {
  it('is a no-op for a stream with no Duration field (returns input unchanged)', () => {
    const ipc = makeIpcStream([[1, 2, 3]]);
    const out = rewriteDurationToInt64(ipc);
    expect(out.equals(ipc)).to.equal(true);
  });

  it('returns the input unchanged for an empty buffer (no throw)', () => {
    const empty = Buffer.alloc(0);
    const out = rewriteDurationToInt64(empty);
    expect(out.byteLength).to.equal(0);
  });

  it('does not throw on garbage / truncated bytes (fail-closed framing)', () => {
    // Random bytes are not a valid IPC stream; readMessageAt must reject
    // them (the negative-length and bounds guards) rather than crash.
    const garbage = Buffer.from([0x01, 0x02, 0x03, 0x04, 0xff, 0xff, 0xff, 0xff]);
    expect(() => rewriteDurationToInt64(garbage)).to.not.throw();
  });

  it('patchIpcBytes is byte-identical to the input when no Duration is present', () => {
    const ipc = makeIpcStream([[7, 8]]);
    expect(patchIpcBytes(ipc).equals(ipc)).to.equal(true);
  });
});

describe('KernelArrowIpc.decodeIpcSchema', () => {
  it('decodes the schema of a normal stream', () => {
    const schema = decodeIpcSchema(makeIpcStream([[1]]));
    expect(schema.fields.map((f) => f.name)).to.deep.equal(['a']);
  });

  it('throws a typed HiveDriverError (not a raw TypeError) on an empty payload', () => {
    expect(() => decodeIpcSchema(Buffer.alloc(0))).to.throw(HiveDriverError);
  });
});
