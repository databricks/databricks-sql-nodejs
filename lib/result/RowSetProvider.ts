import {
  TColumn,
  TFetchOrientation,
  TFetchResultsResp,
  TOperationHandle,
  TRowSet,
} from '../../thrift/TCLIService_types';
import { ColumnCode, FetchType, Int64 } from '../hive/Types';
import Status from '../dto/Status';
import IClientContext from '../contracts/IClientContext';
import IResultsProvider, { ResultsProviderFetchNextOptions } from './IResultsProvider';

function checkIfOperationHasMoreRows(response: TFetchResultsResp): boolean {
  if (response.hasMoreRows) {
    return true;
  }

  const columns = response.results?.columns || [];

  if (columns.length === 0) {
    return false;
  }

  const column: TColumn = columns[0];

  const columnValue =
    column[ColumnCode.binaryVal] ||
    column[ColumnCode.boolVal] ||
    column[ColumnCode.byteVal] ||
    column[ColumnCode.doubleVal] ||
    column[ColumnCode.i16Val] ||
    column[ColumnCode.i32Val] ||
    column[ColumnCode.i64Val] ||
    column[ColumnCode.stringVal];

  return (columnValue?.values?.length || 0) > 0;
}

export default class RowSetProvider implements IResultsProvider<TRowSet | undefined> {
  private readonly context: IClientContext;

  private readonly operationHandle: TOperationHandle;

  private fetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_FIRST;

  private prefetchedResults: TFetchResultsResp[] = [];

  private readonly returnOnlyPrefetchedResults: boolean;

  public hasMoreRows: boolean = false;

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

    if (this.prefetchedResults.length > 0) {
      this.hasMoreRows = true;
    } else if (this.returnOnlyPrefetchedResults) {
      this.hasMoreRows = false;
    } else {
      this.hasMoreRows = checkIfOperationHasMoreRows(response);
    }

    return response.results;
  }

  public async fetchNext({ limit }: ResultsProviderFetchNextOptions) {
    const prefetchedResponse = this.prefetchedResults.shift();
    if (prefetchedResponse) {
      return this.processFetchResponse(prefetchedResponse);
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
    return this.hasMoreRows;
  }
}
