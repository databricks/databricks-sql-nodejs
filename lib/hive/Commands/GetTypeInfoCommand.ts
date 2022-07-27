import BaseCommand from './BaseCommand';
import { TGetTypeInfoReq, TGetTypeInfoResp } from '../../../thrift/TCLIService_types';

export default class GetTypeInfoCommand extends BaseCommand {
  execute(data: TGetTypeInfoReq): Promise<TGetTypeInfoResp> {
    const request = new TGetTypeInfoReq(data);

    return this.executeCommand<TGetTypeInfoResp>(request, this.client.GetTypeInfo);
  }
}
