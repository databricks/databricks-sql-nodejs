import BaseCommand from './BaseCommand';
import { TGetColumnsReq, TGetColumnsResp } from '../../../thrift/TCLIService_types';

export default class GetColumnsCommand extends BaseCommand {
  execute(data: TGetColumnsReq): Promise<TGetColumnsResp> {
    const request = new TGetColumnsReq(data);

    return this.executeCommand<TGetColumnsResp>(request, this.client.GetColumns);
  }
}
