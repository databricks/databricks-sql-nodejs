import BaseCommand from './BaseCommand';
import { TGetTypeInfoReq, TGetTypeInfoResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'GetTypeInfo'>;

export default class GetTypeInfoCommand extends BaseCommand<Client> {
  execute(data: TGetTypeInfoReq): Promise<TGetTypeInfoResp> {
    const request = new TGetTypeInfoReq(data);

    return this.executeCommand<TGetTypeInfoResp>(request, this.client.GetTypeInfo);
  }
}
