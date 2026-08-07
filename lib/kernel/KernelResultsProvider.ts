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

import { Schema, TypeMap } from 'apache-arrow';
import IResultsProvider, { ResultsProviderFetchNextOptions } from '../result/IResultsProvider';
import { ArrowBatch } from '../result/utils';
import { countRowsInIpc, patchIpcBytes } from './KernelArrowIpc';
import { importZeroCopyBatch, KernelZeroCopyBatch } from './KernelArrowImport';

/**
 * The minimal slice of the napi-binding `Statement` class that we
 * consume from JS. Defined locally (not imported from the binding's
 * d.ts) so the loader layer's loose `unknown` typing doesn't force
 * unsafe casts at every call site, and so unit tests can pass a stub.
 */
export interface KernelFetchHandle {
  fetchNextBatch(): Promise<{ ipcBytes: Buffer } | null>;
  // Copy-into-cage fetch (Flavour A): returns the batch's Arrow buffers as
  // V8-allocated (in-cage) copies of the Arrow bytes — cage-safe and
  // finalizer-free, one memcpy per buffer. Optional so stubs / older bindings
  // without the method still satisfy the interface (the provider then falls
  // back to the IPC path).
  fetchNextBatchCopycage?(): Promise<KernelZeroCopyBatch | null>;
}

/**
 * Which native fetch path the provider drives. `ipc` re-encodes each batch
 * to Arrow IPC bytes (default, oldest path). `copycage` hands V8-allocated
 * in-cage copies of the kernel's Arrow buffers (Flavour A) — cage-safe, one
 * memcpy/buffer, no IPC serialize+decode.
 */
export type KernelFetchMode = 'ipc' | 'copycage';

/**
 * Enables the copycage buffer-handoff fetch path. Carries the decoded Arrow
 * `schema` needed to drive `makeData` reconstruction. Only takes effect when
 * the binding also exposes `fetchNextBatchCopycage`; otherwise the provider
 * falls back to the IPC path.
 */
export interface KernelBufferHandoffOptions {
  mode: Exclude<KernelFetchMode, 'ipc'>;
  schema: Schema<TypeMap>;
}

/**
 * `IResultsProvider<ArrowBatch>` that pulls Arrow IPC batches from the
 * kernel via the napi `Statement` handle and adapts them onto the
 * shape `ArrowResultConverter` already speaks
 * (`lib/result/utils.ts:22-25`).
 *
 * Each kernel `fetchNextBatch()` call returns a complete Arrow IPC
 * stream (schema header + 1 record-batch message) per the design
 * documented at `kernel-workflow/findings/arch/napi-binding/round2-methods-2026-05-15.md:46-60`.
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
 * `KernelArrowIpc.ts:decodeIpcBatch` for the cost rationale.
 */
export default class KernelResultsProvider implements IResultsProvider<ArrowBatch> {
  private readonly statement: KernelFetchHandle;

  // Prefetched next batch so `hasMore()` can be answered without an
  // extra round-trip. Set by `prime()` (lazy) and by `fetchNext`.
  private prefetched?: ArrowBatch;

  // Set once the kernel returns `null` from `fetchNextBatch()`.
  private exhausted = false;

  // When set, fetch via a buffer-handoff path (`zerocopy` ⇒
  // `fetchNextBatchZerocopy()`, `copycage` ⇒ `fetchNextBatchCopycage()`)
  // feeding `importZeroCopyBatch(schema, …)`. Undefined ⇒ the IPC path.
  private readonly bufferHandoff?: KernelBufferHandoffOptions;

  constructor(statement: KernelFetchHandle, bufferHandoff?: KernelBufferHandoffOptions) {
    this.statement = statement;
    // Only engage the copycage path if the binding actually exposes
    // `fetchNextBatchCopycage` — otherwise silently fall back to IPC (older
    // binding / stub).
    this.bufferHandoff =
      bufferHandoff !== undefined && typeof statement.fetchNextBatchCopycage === 'function'
        ? bufferHandoff
        : undefined;
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
    // Loop rather than self-recurse: a long run of empty batches drains
    // iteratively toward the null sentinel without building a deep promise
    // chain.
    while (!this.exhausted && this.prefetched === undefined) {
      if (this.bufferHandoff !== undefined) {
        // Copycage: fetch the batch as in-cage `ArrayBuffer` copies and feed
        // `importZeroCopyBatch` to rebuild the `RecordBatch` directly (no IPC
        // serialize+decode). The constructor already verified the binding
        // exposes `fetchNextBatchCopycage`.
        // eslint-disable-next-line no-await-in-loop
        const desc = await this.statement.fetchNextBatchCopycage!.call(this.statement);
        if (desc === null) {
          this.exhausted = true;
          return;
        }
        if (desc.numRows > 0) {
          const recordBatch = importZeroCopyBatch(this.bufferHandoff.schema, desc);
          this.prefetched = { batches: [], recordBatches: [recordBatch], rowCount: desc.numRows };
        }
        // eslint-disable-next-line no-continue
        continue;
      }
      // eslint-disable-next-line no-await-in-loop
      const next = await this.statement.fetchNextBatch();
      if (next === null) {
        this.exhausted = true;
        return;
      }
      // Patch the raw bytes once: rewrite any Arrow `Duration` field to
      // `Int64` with a `databricks.arrow.duration_unit` marker, so that
      // apache-arrow@13 (which predates Duration support) can decode the
      // stream. The downstream `RecordBatchReader.from` inside
      // `ArrowResultConverter` sees the same patched buffer. See
      // `KernelArrowIpcDurationFix.ts`.
      const ipcBytes = patchIpcBytes(next.ipcBytes);
      // Row count only — `countRowsInIpc` reads the RecordBatch metadata
      // headers without materializing vectors (the converter re-decodes
      // the bytes for the actual values). Avoids a full second Arrow
      // decode on the fetch hot path.
      const rowCount = countRowsInIpc(ipcBytes);
      // Skip empty batches — the converter handles them but pre-filtering here
      // avoids a round-trip through the converter's prefetch loop. Continue to
      // find a non-empty batch or hit exhaustion.
      if (rowCount > 0) {
        this.prefetched = { batches: [ipcBytes], rowCount };
      }
    }
  }
}
