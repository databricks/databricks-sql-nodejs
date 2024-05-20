import BaseCommand from './BaseCommand';
import { TGetFunctionsReq, TGetFunctionsResp } from '../../../thrift/TCLIService_types';
import TCLIService from '../../../thrift/TCLIService';

type Client = Pick<TCLIService.Client, 'GetFunctions'>;

export default class GetFunctionsCommand extends BaseCommand<Client> {
  execute(data: TGetFunctionsReq): Promise<TGetFunctionsResp> {
    const request = new TGetFunctionsReq(data);

    return this.executeCommand<TGetFunctionsResp>(request, this.client.GetFunctions);
  }
}
