import BaseCommand from './BaseCommand';
import { TFetchResultsReq, TFetchResultsResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'FetchResults'>;

/**
 * TFetchResultsReq.fetchType - 0 represents Query output. 1 represents Log
 */
export default class FetchResultsCommand extends BaseCommand<Client> {
  execute(data: TFetchResultsReq): Promise<TFetchResultsResp> {
    const request = new TFetchResultsReq(data);

    return this.executeCommand<TFetchResultsResp>(request, this.client.FetchResults);
  }
}
