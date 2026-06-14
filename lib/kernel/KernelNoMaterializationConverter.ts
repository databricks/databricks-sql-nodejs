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

/**
 * [bench] Kernel no-materialization drain.
 *
 * Stands in for `ArrowResultConverter` on the kernel result path when
 * `disableArrowMaterialization` is set. It still drives the full transport
 * — each `fetchNext` pulls one IPC batch from `KernelResultsProvider`, which
 * calls the napi `Statement.fetchNextBatch()` (kernel download + LZ4 +
 * `encode_ipc_stream`) — but it does NOT hand the bytes to
 * `apache-arrow`'s `RecordBatchReader`. `ArrowResultConverter`'s no-mat path
 * still constructs a `RecordBatchReader` and calls `.next()`, which runs
 * `_loadVectors` and materializes the entire Arrow vector tree (see
 * `KernelArrowIpc.ts:30-43`); only the per-row JS-object construction is
 * skipped. This converter skips the vector materialization too: the row count
 * is already known from `KernelResultsProvider` (`countRowsInIpc`, a header-only
 * read), so we emit a length-only array of `null` placeholders.
 *
 * The result is a measurement of kernel execute + transport (+ napi IPC
 * re-encode) with neither the Arrow vector decode nor the JS row build — the
 * tightest "did the bytes arrive" lower bound the Node kernel path can report.
 *
 * `null` placeholders (not array holes): `ResultSlicer` flattens chunks via
 * `Array.prototype.flat()`, which silently drops sparse-array holes and would
 * collapse the row count to 0. Each `null` costs a single pointer.
 */
export default class KernelNoMaterializationConverter implements IResultsProvider<Array<any>> {
  private readonly source: IResultsProvider<ArrowBatch>;

  constructor(source: IResultsProvider<ArrowBatch>) {
    this.source = source;
  }

  public async hasMore(): Promise<boolean> {
    return this.source.hasMore();
  }

  public async fetchNext(options: ResultsProviderFetchNextOptions): Promise<Array<any>> {
    const batch = await this.source.fetchNext(options);
    // `rowCount` was computed header-only by KernelResultsProvider; the IPC
    // `batches` bytes are intentionally never decoded here.
    return new Array(batch.rowCount).fill(null);
  }
}
