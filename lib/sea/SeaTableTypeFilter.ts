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

import {
  TGetOperationStatusResp,
  TGetResultSetMetadataResp,
} from '../../thrift/TCLIService_types';
import IOperationBackend from '../contracts/IOperationBackend';
import Status from '../dto/Status';
import { WaitUntilReadyOptions } from '../contracts/IOperation';

/**
 * Wraps an `IOperationBackend` and filters rows by the `TABLE_TYPE` column.
 *
 * The Databricks server does not honour the `table_types` filter in the
 * `listTables` kernel call — the field is advisory only (see pyo3 metadata.rs
 * and matrix-audit-python.md).  This adapter applies the filter client-side on
 * every `fetchChunk` call.
 *
 * Semantics:
 * - `allowedTypes` is a `Set` built from `request.tableTypes` before this
 *   wrapper is constructed.  The caller decides what to pass in.
 * - An empty `Set` means "keep nothing" — the caller passed `tableTypes: []`,
 *   which explicitly requests zero table types.
 * - Matching is case-sensitive exact equality on the `TABLE_TYPE` column value.
 *   Databricks returns upper-case values (`TABLE`, `VIEW`, `EXTERNAL TABLE`, …);
 *   callers should pass upper-case strings.
 * - Rows whose `TABLE_TYPE` column is absent or non-string are dropped.
 *
 * All lifecycle methods (`cancel`, `close`, `status`, `waitUntilReady`,
 * `getResultMetadata`) delegate unchanged to the inner backend.
 */
export default class SeaTableTypeFilter implements IOperationBackend {
  private readonly inner: IOperationBackend;

  private readonly allowedTypes: Set<string>;

  constructor(inner: IOperationBackend, allowedTypes: Set<string>) {
    this.inner = inner;
    this.allowedTypes = allowedTypes;
  }

  public get id(): string {
    return this.inner.id;
  }

  public get hasResultSet(): boolean {
    return this.inner.hasResultSet;
  }

  public async fetchChunk(options: { limit: number; disableBuffering?: boolean }): Promise<Array<object>> {
    const rows = await this.inner.fetchChunk(options);
    return rows.filter((row) => {
      const tableType = (row as Record<string, unknown>).TABLE_TYPE;
      return typeof tableType === 'string' && this.allowedTypes.has(tableType);
    });
  }

  public async hasMore(): Promise<boolean> {
    return this.inner.hasMore();
  }

  public async waitUntilReady(options?: WaitUntilReadyOptions): Promise<void> {
    return this.inner.waitUntilReady(options);
  }

  public async status(progress: boolean): Promise<TGetOperationStatusResp> {
    return this.inner.status(progress);
  }

  public async getResultMetadata(): Promise<TGetResultSetMetadataResp> {
    return this.inner.getResultMetadata();
  }

  public async cancel(): Promise<Status> {
    return this.inner.cancel();
  }

  public async close(): Promise<Status> {
    return this.inner.close();
  }
}
