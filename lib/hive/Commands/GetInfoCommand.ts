import BaseCommand from './BaseCommand';
import { TGetInfoReq, TGetInfoResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'GetInfo'>;

export default class GetInfoCommand extends BaseCommand<Client> {
  execute(data: TGetInfoReq): Promise<TGetInfoResp> {
    const request = new TGetInfoReq(data);

    return this.executeCommand<TGetInfoResp>(request, this.client.GetInfo);
  }
}
