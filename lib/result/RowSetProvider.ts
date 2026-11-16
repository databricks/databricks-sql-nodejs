import Int64 from 'node-int64';
import {
  TColumn,
  TFetchOrientation,
  TFetchResultsResp,
  TOperationHandle,
  TRowSet,
  TSparkArrowResultLink,
} from '../../thrift/TCLIService_types';
import Status from '../dto/Status';
import IClientContext from '../contracts/IClientContext';
import { safeEmit } from '../telemetry/telemetryUtils';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';
import { getColumnValue } from './utils';

export enum FetchType {
  Data = 0,
  Logs = 1,
}

/**
 * Rough byte count for a single `TColumn`. Used only for telemetry — exact
 * wire size isn't worth a deserialization pass. Fixed-width values are
 * estimated by their TypeScript-side footprint; strings count their UTF-16
 * length (close enough proxy for UTF-8 payload in most cases). The `nulls`
 * bitmap buffer is included verbatim.
 *
 * Wire-side payload will be slightly different (Thrift's varint encodings,
 * UTF-8 vs the JS engine's UTF-16) but dashboards aggregating
 * `bytesDownloaded` for non-arrow result sets care about scale, not byte-
 * perfect accuracy.
 */
function estimateColumnBytes(column: TColumn): number {
  let bytes = 0;
  if (column.boolVal) {
    bytes += column.boolVal.values?.length ?? 0;
    bytes += column.boolVal.nulls?.length ?? 0;
  }
  if (column.byteVal) {
    bytes += column.byteVal.values?.length ?? 0;
    bytes += column.byteVal.nulls?.length ?? 0;
  }
  if (column.i16Val) {
    bytes += (column.i16Val.values?.length ?? 0) * 2;
    bytes += column.i16Val.nulls?.length ?? 0;
  }
  if (column.i32Val) {
    bytes += (column.i32Val.values?.length ?? 0) * 4;
    bytes += column.i32Val.nulls?.length ?? 0;
  }
  if (column.i64Val) {
    bytes += (column.i64Val.values?.length ?? 0) * 8;
    bytes += column.i64Val.nulls?.length ?? 0;
  }
  if (column.doubleVal) {
    bytes += (column.doubleVal.values?.length ?? 0) * 8;
    bytes += column.doubleVal.nulls?.length ?? 0;
  }
  if (column.stringVal) {
    for (const value of column.stringVal.values ?? []) {
      bytes += value?.length ?? 0;
    }
    bytes += column.stringVal.nulls?.length ?? 0;
  }
  if (column.binaryVal) {
    for (const value of column.binaryVal.values ?? []) {
      bytes += value?.length ?? 0;
    }
    bytes += column.binaryVal.nulls?.length ?? 0;
  }
  return bytes;
}

function checkIfOperationHasMoreRows(response: TFetchResultsResp): boolean {
  if (response.hasMoreRows) {
    return true;
  }

  const columns = response.results?.columns || [];

  const columnValue = getColumnValue(columns[0]);
  return (columnValue?.values?.length ?? 0) > 0;
}

export default class RowSetProvider implements IResultsProvider<TRowSet | undefined> {
  private readonly context: IClientContext;

  private readonly operationHandle: TOperationHandle;

  private readonly statementId?: string;

  private chunkIndex: number = 0;

  private fetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_FIRST;

  private prefetchedResults: TFetchResultsResp[] = [];

  private readonly returnOnlyPrefetchedResults: boolean;

  private hasMoreRowsFlag?: boolean = undefined;

  // Row offset after the last result link returned so far
  private nextLinkRowOffset: number = 0;

  // A link refresh may move the server's cursor, so after one the next page is requested by offset
  private fetchByOffset: boolean = false;

  // Serializes FetchResults calls, so a link refresh never races a page fetch on the cursor
  private lastFetch: Promise<unknown> = Promise.resolve();

  private serializeFetch<T>(fetchResults: () => Promise<T>): Promise<T> {
    const result = this.lastFetch.then(fetchResults, fetchResults);
    this.lastFetch = result.catch(() => undefined);
    return result;
  }

  private get hasMoreRows(): boolean {
    // `hasMoreRowsFlag` is populated only after fetching the first row set.
    // Prior to that, we use a `operationHandle.hasResultSet` flag which
    // is set if there are any data at all. Also, we have to choose appropriate
    // flag in a getter because both `hasMoreRowsFlag` and `operationHandle.hasResultSet`
    // may change between this getter calls
    return this.hasMoreRowsFlag ?? this.operationHandle.hasResultSet;
  }

  constructor(
    context: IClientContext,
    operationHandle: TOperationHandle,
    prefetchedResults: Array<TFetchResultsResp | undefined>,
    returnOnlyPrefetchedResults: boolean,
    statementId?: string,
  ) {
    this.context = context;
    this.operationHandle = operationHandle;
    this.statementId = statementId;
    prefetchedResults.forEach((item) => {
      if (item) {
        this.prefetchedResults.push(item);
      }
    });
    this.returnOnlyPrefetchedResults = returnOnlyPrefetchedResults;
  }

  private processFetchResponse(response: TFetchResultsResp): TRowSet | undefined {
    Status.assert(response.status);
    this.fetchOrientation = TFetchOrientation.FETCH_NEXT;
    this.hasMoreRowsFlag = checkIfOperationHasMoreRows(response);
    response.results?.resultLinks?.forEach((link) => {
      this.nextLinkRowOffset = Math.max(
        this.nextLinkRowOffset,
        link.startRowOffset.toNumber() + link.rowCount.toNumber(),
      );
    });
    return response.results;
  }

  /**
   * Fetches fresh result links starting at `startRowOffset`, e.g. to replace a presigned
   * URL that cloud storage rejected. Returns no links when the operation is already closed.
   */
  public async refreshLinks(startRowOffset: Int64, maxRows: Int64): Promise<Array<TSparkArrowResultLink>> {
    if (this.returnOnlyPrefetchedResults) {
      return [];
    }
    const driver = await this.context.getDriver();
    const response = await this.serializeFetch(async () => {
      this.fetchByOffset = true;
      return driver.fetchResults({
        operationHandle: this.operationHandle,
        orientation: TFetchOrientation.FETCH_NEXT,
        maxRows,
        fetchType: FetchType.Data,
        startRowOffset,
      });
    });
    Status.assert(response.status);
    return response.results?.resultLinks ?? [];
  }

  public async fetchNext({ limit }: ResultsProviderFetchNextOptions) {
    const prefetchedResponse = this.prefetchedResults.shift();
    if (prefetchedResponse) {
      return this.processFetchResponse(prefetchedResponse);
    }

    // We end up here if no more prefetched results available (checked above)
    if (this.returnOnlyPrefetchedResults) {
      return undefined;
    }

    // Don't fetch next chunk if there are no more data available
    if (!this.hasMoreRows) {
      return undefined;
    }

    const driver = await this.context.getDriver();
    const startTime = Date.now();
    const response = await this.serializeFetch(() =>
      driver.fetchResults({
        operationHandle: this.operationHandle,
        orientation: this.fetchOrientation,
        maxRows: new Int64(limit),
        fetchType: FetchType.Data,
        ...(this.fetchByOffset ? { startRowOffset: new Int64(this.nextLinkRowOffset) } : {}),
      }),
    );
    const latencyMs = Date.now() - startTime;

    this.emitChunkEvent(latencyMs, response);

    return this.processFetchResponse(response);
  }

  /**
   * Emit a chunk telemetry event for one FetchResults page.
   * CRITICAL: All exceptions swallowed and logged at LogLevel.debug ONLY.
   */
  private emitChunkEvent(latencyMs: number, response: TFetchResultsResp): void {
    const { statementId } = this;
    if (!statementId) {
      return;
    }
    safeEmit(this.context, (emitter) => {
      // Aggregate byte counts across all wire shapes a `TFetchResultsResp`
      // can carry. URL-based result sets (`resultLinks`) bypass this site
      // entirely — they emit from `CloudFetchResultHandler.emitCloudFetchChunk`
      // with the post-download byte count from the cloud-storage GET.
      //
      // Shapes counted here:
      //   - `arrowBatches[i].batch`     — Arrow inline payload
      //   - `binaryColumns`             — packed columnar binary blob
      //   - `columns[i].*Val.values`    — COLUMN_BASED_SET, sum across columns
      //   - `rows[i]`                   — legacy row-based set (rare; estimate)
      let bytes = 0;
      const { results } = response;
      if (results) {
        const { arrowBatches } = results;
        if (arrowBatches) {
          for (const batch of arrowBatches) {
            bytes += batch.batch?.length ?? 0;
          }
        }
        const { binaryColumns } = results;
        if (binaryColumns) {
          bytes += binaryColumns.length;
        }
        const { columns } = results;
        if (columns) {
          for (const column of columns) {
            bytes += estimateColumnBytes(column);
          }
        }
      }

      emitter.emitCloudFetchChunk({
        statementId,
        chunkIndex: this.chunkIndex,
        latencyMs,
        bytes,
      });
      this.chunkIndex += 1;
    });
  }

  public async hasMore() {
    // If there are prefetched results available - return `true` regardless of
    // the actual state of `hasMoreRows` flag (because we actually have some data)
    if (this.prefetchedResults.length > 0) {
      return true;
    }
    // We end up here if no more prefetched results available (checked above)
    if (this.returnOnlyPrefetchedResults) {
      return false;
    }

    return this.hasMoreRows;
  }
}
