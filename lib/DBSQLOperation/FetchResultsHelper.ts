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

function checkIfOperationHasMoreRows(response: TFetchResultsResp): boolean {
  if (response.hasMoreRows) {
    return true;
  }

  const columns = response.results?.columns || [];

  if (!columns.length) {
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
