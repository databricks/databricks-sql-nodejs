import BaseCommand from './BaseCommand';
import { TGetInfoReq, TGetInfoResp } from '../../../thrift/TCLIService_types';

export default class GetInfoCommand extends BaseCommand {
  execute(data: TGetInfoReq): Promise<TGetInfoResp> {
    const request = new TGetInfoReq(data);

    return this.executeCommand<TGetInfoResp>(request, this.client.GetInfo);
  }
}
