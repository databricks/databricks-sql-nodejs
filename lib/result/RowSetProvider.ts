import Int64 from 'node-int64';
import { TFetchOrientation, TFetchResultsResp, TOperationHandle, TRowSet } from '../../thrift/TCLIService_types';
import Status from '../dto/Status';
import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';
import { getColumnValue } from './utils';

export enum FetchType {
  Data = 0,
  Logs = 1,
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

  private fetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_FIRST;

  private prefetchedResults: TFetchResultsResp[] = [];

  private readonly returnOnlyPrefetchedResults: boolean;

  private hasMoreRowsFlag?: boolean = undefined;

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
  ) {
    this.context = context;
    this.operationHandle = operationHandle;
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
    return response.results;
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
    const response = await driver.fetchResults({
      operationHandle: this.operationHandle,
      orientation: this.fetchOrientation,
      maxRows: new Int64(limit),
      fetchType: FetchType.Data,
    });

    return this.processFetchResponse(response);
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
