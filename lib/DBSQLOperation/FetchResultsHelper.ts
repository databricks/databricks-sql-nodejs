import {
  TColumn,
  TFetchOrientation,
  TFetchResultsResp,
  TOperationHandle,
  TRowSet,
  TStatus,
} from '../../thrift/TCLIService_types';
import { ColumnCode, FetchType, Int64 } from '../hive/Types';
import HiveDriver from '../hive/HiveDriver';
import StatusFactory from '../factory/StatusFactory';

import RestDriver from '../rest/RestDriver';

function checkIfOperationHasMoreRows(response: TFetchResultsResp): boolean {
  return response.hasMoreRows || false;
}

export default class FetchResultsHelper {
  private driver: RestDriver;

  private operationHandle: TOperationHandle;

  private fetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_FIRST;

  private statusFactory = new StatusFactory();

  private prefetchedResults: TFetchResultsResp[] = [];

  hasMoreRows: boolean = false;

  constructor(
    driver: RestDriver,
    operationHandle: TOperationHandle,
    prefetchedResults: Array<TFetchResultsResp | undefined>,
  ) {
    this.driver = driver;
    this.operationHandle = operationHandle;
    prefetchedResults.forEach((item) => {
      if (item) {
        this.prefetchedResults.push(item);
      }
    });
  }

  private assertStatus(responseStatus: TStatus): void {
    this.statusFactory.create(responseStatus);
  }

  private processFetchResponse(response: TFetchResultsResp): TRowSet | undefined {
    this.assertStatus(response.status);
    this.fetchOrientation = TFetchOrientation.FETCH_NEXT;
    this.hasMoreRows = checkIfOperationHasMoreRows(response);
    return response.results;
  }

  async fetch(maxRows: number) {
    const prefetchedResponse = this.prefetchedResults.shift();
    if (prefetchedResponse) {
      return this.processFetchResponse(prefetchedResponse);
    }
    return this.driver
      .fetchResults({
        operationHandle: this.operationHandle,
        orientation: this.fetchOrientation,
        maxRows: new Int64(maxRows),
        fetchType: FetchType.Data,
      })
      .then((response) => this.processFetchResponse(response));
  }
}
