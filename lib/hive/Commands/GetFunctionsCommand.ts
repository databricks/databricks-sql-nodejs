import BaseCommand from './BaseCommand';
import { TGetFunctionsReq, TGetFunctionsResp } from '../../../thrift/TCLIService_types';

export default class GetFunctionsCommand extends BaseCommand {
  execute(data: TGetFunctionsReq): Promise<TGetFunctionsResp> {
    const request = new TGetFunctionsReq(data);

    return this.executeCommand<TGetFunctionsResp>(request, this.client.GetFunctions);
  }
}
