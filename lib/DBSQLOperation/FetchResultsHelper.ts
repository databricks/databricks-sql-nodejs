import {
  TColumn,
  TFetchOrientation,
  TFetchResultsResp,
  TOperationHandle,
  TRowSet,
} from '../../thrift/TCLIService_types';
import { ColumnCode, FetchType, Int64 } from '../hive/Types';
import HiveDriver from '../hive/HiveDriver';
import Status from '../dto/Status';

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

export default class FetchResultsHelper {
  private readonly driver: HiveDriver;

  private readonly operationHandle: TOperationHandle;

  private fetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_FIRST;

  private pendingResults: TFetchResultsResp[] = [];

  private readonly returnOnlyPrefetchedResults: boolean;

  public hasMoreRows: boolean = false;

  constructor(
    driver: HiveDriver,
    operationHandle: TOperationHandle,
    prefetchedResults: Array<TFetchResultsResp | undefined>,
    returnOnlyPrefetchedResults: boolean,
  ) {
    this.driver = driver;
    this.operationHandle = operationHandle;
    prefetchedResults.forEach((item) => {
      if (item) {
        this.prepareCloudFetchChunks(item);
      }
    });
    this.returnOnlyPrefetchedResults = returnOnlyPrefetchedResults;
  }

  private processFetchResponse(response: TFetchResultsResp): TRowSet | undefined {
    Status.assert(response.status);
    this.fetchOrientation = TFetchOrientation.FETCH_NEXT;

    if (this.pendingResults.length > 0) {
      this.hasMoreRows = true;
    } else if (this.returnOnlyPrefetchedResults) {
      this.hasMoreRows = false;
    } else {
      this.hasMoreRows = checkIfOperationHasMoreRows(response);
    }

    return response.results;
  }

  public async fetch(maxRows: number) {
    if (this.pendingResults.length === 0) {
      const results = await this.driver.fetchResults({
        operationHandle: this.operationHandle,
        orientation: this.fetchOrientation,
        maxRows: new Int64(maxRows),
        fetchType: FetchType.Data,
      });

      this.prepareCloudFetchChunks(results);
    }

    const response = this.pendingResults.shift();
    // This check is rather for safety and to make TS happy. In practice, such a case should not happen
    if (!response) {
      throw new Error('Unexpected error: no more data');
    }

    return this.processFetchResponse(response);
  }

  private prepareCloudFetchChunks(response: TFetchResultsResp) {
    // TODO: Make it configurable. Effectively, this is a concurrent downloads limit for an operation
    const maxLinkCount = 1;

    if (response.results && response.results.resultLinks && response.results.resultLinks.length > 0) {
      const allLinks = [...response.results.resultLinks];
      while (allLinks.length > 0) {
        // Shallow clone the original response object, but rewrite cloud fetch links array
        // to contain the only entry
        const responseFragment = {
          ...response,
          results: {
            ...response.results,
            resultLinks: allLinks.splice(0, maxLinkCount),
          },
        };

        this.pendingResults.push(responseFragment);
      }
    }
  }
}
