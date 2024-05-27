import BaseCommand from './BaseCommand';
import { TGetFunctionsReq, TGetFunctionsResp } from '../../../thrift/TCLIService_types';
import IThriftClient from '../../contracts/IThriftClient';

type Client = Pick<IThriftClient, 'GetFunctions'>;

export default class GetFunctionsCommand extends BaseCommand<Client> {
  execute(data: TGetFunctionsReq): Promise<TGetFunctionsResp> {
    const request = new TGetFunctionsReq(data);

    return this.executeCommand<TGetFunctionsResp>(request, this.client.GetFunctions);
  }
}
