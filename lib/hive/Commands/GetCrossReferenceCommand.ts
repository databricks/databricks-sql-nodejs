import BaseCommand from './BaseCommand';
import { TGetCrossReferenceReq, TGetCrossReferenceResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'GetCrossReference'>;

export default class GetCrossReferenceCommand extends BaseCommand<Client> {
  execute(data: TGetCrossReferenceReq): Promise<TGetCrossReferenceResp> {
    const request = new TGetCrossReferenceReq(data);

    return this.executeCommand<TGetCrossReferenceResp>(request, this.client.GetCrossReference);
  }
}
