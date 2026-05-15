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

import IResultsProvider, { ResultsProviderFetchNextOptions } from '../result/IResultsProvider';
import { ArrowBatch } from '../result/utils';
import { decodeIpcBatch } from './SeaArrowIpc';

/**
 * The minimal slice of the napi-binding `Statement` class that we
 * consume from JS. Defined locally (not imported from the binding's
 * d.ts) so the loader layer's loose `unknown` typing doesn't force
 * unsafe casts at every call site, and so unit tests can pass a stub.
 */
export interface SeaStatementHandle {
  fetchNextBatch(): Promise<{ ipcBytes: Buffer } | null>;
}

/**
 * `IResultsProvider<ArrowBatch>` that pulls Arrow IPC batches from the
 * kernel via the napi `Statement` handle and adapts them onto the
 * shape `ArrowResultConverter` already speaks
 * (`lib/result/utils.ts:22-25`).
 *
 * Each kernel `fetchNextBatch()` call returns a complete Arrow IPC
 * stream (schema header + 1 record-batch message) per the design
 * documented at `sea-workflow/findings/arch/napi-binding/round2-methods-2026-05-15.md:46-60`.
 * We pass that buffer through as a single-element `batches: [ipcBytes]`
 * array — `RecordBatchReader.from(arrowBatch.batches)` inside the
 * converter (`lib/result/ArrowResultConverter.ts:119`) reads the
 * schema from the prefix and then the record-batch messages from the
 * remainder of the same buffer.
 *
 * We pre-parse the IPC bytes once here to extract `rowCount` (the
 * sum of `RecordBatch.numRows` across messages in the stream) because
 * the converter consumes that as an explicit field rather than
 * deriving it from the batch contents. See the comment in
 * `SeaArrowIpc.ts:decodeIpcBatch` for the cost rationale.
 */
export default class SeaResultsProvider implements IResultsProvider<ArrowBatch> {
  private readonly statement: SeaStatementHandle;

  // Prefetched next batch so `hasMore()` can be answered without an
  // extra round-trip. Set by `prime()` (lazy) and by `fetchNext`.
  private prefetched?: ArrowBatch;

  // Set once the kernel returns `null` from `fetchNextBatch()`.
  private exhausted = false;

  constructor(statement: SeaStatementHandle) {
    this.statement = statement;
  }

  public async hasMore(): Promise<boolean> {
    if (this.exhausted) {
      return false;
    }
    if (this.prefetched !== undefined) {
      return true;
    }
    await this.prime();
    return this.prefetched !== undefined;
  }

  public async fetchNext(_options: ResultsProviderFetchNextOptions): Promise<ArrowBatch> {
    if (this.prefetched === undefined && !this.exhausted) {
      await this.prime();
    }
    if (this.prefetched === undefined) {
      return { batches: [], rowCount: 0 };
    }
    const out = this.prefetched;
    this.prefetched = undefined;
    return out;
  }

  // Pull the next batch from the kernel and stash it in `prefetched`,
  // or mark the stream exhausted. Used by both `hasMore` and `fetchNext`
  // to keep one batch buffered ahead so `hasMore` is accurate without
  // re-asking the kernel.
  private async prime(): Promise<void> {
    if (this.exhausted || this.prefetched !== undefined) {
      return;
    }
    const next = await this.statement.fetchNextBatch();
    if (next === null) {
      this.exhausted = true;
      return;
    }
    const { ipcBytes } = next;
    const { rowCount } = decodeIpcBatch(ipcBytes);
    if (rowCount === 0) {
      // Skip empty batches — the converter handles them but pre-filtering
      // here avoids one round-trip through the converter's prefetch loop.
      // Re-prime to either find a non-empty batch or hit exhaustion.
      await this.prime();
      return;
    }
    this.prefetched = { batches: [ipcBytes], rowCount };
  }
}
