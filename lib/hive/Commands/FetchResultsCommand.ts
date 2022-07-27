import BaseCommand from './BaseCommand';
import { TFetchResultsReq, TFetchResultsResp } from '../../../thrift/TCLIService_types';

/**
 * TFetchResultsReq.fetchType - 0 represents Query output. 1 represents Log
 */
export default class FetchResultsCommand extends BaseCommand {
  execute(data: TFetchResultsReq): Promise<TFetchResultsResp> {
    const request = new TFetchResultsReq(data);

    return this.executeCommand<TFetchResultsResp>(request, this.client.FetchResults);
  }
}
