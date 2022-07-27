import BaseCommand from './BaseCommand';
import { TFetchResultsReq, TFetchResultsResp } from '../../../thrift/TCLIService_types';

export default class FetchResultsCommand extends BaseCommand {
  execute(data: TFetchResultsReq): Promise<TFetchResultsResp> {
    const request = new TFetchResultsReq(data);

    return this.executeCommand<TFetchResultsResp>(request, this.client.FetchResults);
  }
}
