import {
  TFetchOrientation,
  TFetchResultsResp,
  TOperationHandle,
  TRowSet,
  TStatus,
} from '../../thrift/TCLIService_types';
import { FetchType, Int64 } from '../hive/Types';
import HiveDriver from '../hive/HiveDriver';
import StatusFactory from '../factory/StatusFactory';
import checkIfOperationHasMoreRows from './checkIfOperationHasMoreRows';

export default class DataFetchingHelper {
  private driver: HiveDriver;
  private operationHandle: TOperationHandle;
  private fetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_FIRST;
  private statusFactory = new StatusFactory();

  hasMoreRows: boolean = false;

  constructor(driver: HiveDriver, operationHandle: TOperationHandle) {
    this.driver = driver;
    this.operationHandle = operationHandle;
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
