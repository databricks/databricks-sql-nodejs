import BaseCommand from './BaseCommand';
import { OperationHandle, Status, RowSet, Int64 } from '../Types';
import TCLIService_types from '../../../thrift/TCLIService_types';

/**
 * @param orientation - TCLIService_types.TFetchOrientation
 * @param fetchType - 0 represents Query output. 1 represents Log
 */
export type FetchResultsRequest = {
  operationHandle: OperationHandle;
  orientation: number;
  maxRows: Int64;
  fetchType?: number;
};

export type FetchResultsResponse = {
  status: Status;
  hasMoreRows?: boolean;
  results?: RowSet;
};

export default class FetchResultsCommand extends BaseCommand {
  execute(data: FetchResultsRequest): Promise<FetchResultsResponse> {
    const request = new TCLIService_types.TFetchResultsReq(data);

    return this.executeCommand<FetchResultsResponse>(request, this.client.FetchResults);
  }
}
